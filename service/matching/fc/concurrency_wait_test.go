package fc

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
)

type concurrencyWaitCall struct {
	ctx   context.Context
	req   *fcpb.ConcurrencyServiceWaitRequest
	reply chan concurrencyWaitResult
}

type concurrencyWaitResult struct {
	resp *fcpb.ConcurrencyServiceWaitResponse
	err  error
}

func newWaitingConcurrencyLimiter() (*Readiness, *concurrencyState, <-chan concurrencyWaitCall) {
	calls := make(chan concurrencyWaitCall)
	client := &testClient{wait: func(ctx context.Context, req *fcpb.ConcurrencyServiceWaitRequest) (*fcpb.ConcurrencyServiceWaitResponse, error) {
		reply := make(chan concurrencyWaitResult)
		select {
		case calls <- concurrencyWaitCall{ctx: ctx, req: req, reply: reply}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case result := <-reply:
			return result.resp, result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	r := NewReadiness(client, NewReadinessConfig(dynamicconfig.NewNoopCollection()))
	setTokens(r, 7, 0)
	return r, r.getConcurrencyLimiter(testNsID, testConKey), calls
}

func TestConcurrencyWaitWakesHighestPriority(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r, lim, calls := newWaitingConcurrencyLimiter()
		defer r.Stop()
		highReady := make(chan struct{}, 1)
		lowReady := make(chan struct{}, 1)
		high := &testCallback{onReady: func() { highReady <- struct{}{} }}
		low := &testCallback{onReady: func() { lowReady <- struct{}{} }}
		age := time.Unix(100, 0)
		highPriority := makeWakePriority(1, age)
		lowPriority := makeWakePriority(2, age)

		_, err := lim.check(high, highPriority)
		require.ErrorIs(t, err, ErrConcurrencyBlocked)
		first := await.Rcv(t, calls)
		_, err = lim.check(low, lowPriority)
		require.ErrorIs(t, err, ErrConcurrencyBlocked)
		// A long poll can time out with zero tokens. The next request should include both waiters.
		await.Snd(t, first.reply, concurrencyWaitResult{resp: &fcpb.ConcurrencyServiceWaitResponse{Generation: 7}})
		second := await.Rcv(t, calls)
		protorequire.ProtoEqual(t, &fcpb.ConcurrencyServiceWaitRequest{
			NamespaceId: testNsID.String(), Key: testConKey, Generation: 7,
			WakePriority: int64(highPriority), RequestedWakeTokens: 2,
		}, second.req)
		await.Snd(t, second.reply, concurrencyWaitResult{resp: &fcpb.ConcurrencyServiceWaitResponse{Generation: 8, WakeTokens: 1}})
		await.Rcv(t, highReady)
		synctest.Wait()
		require.Empty(t, lowReady)

		retry := r.NewTx(testNsID, newConcurrencyTestTask(), high)
		defer retry.Close()
		require.NoError(t, retry.Check())
		require.Zero(t, getTokens(r))
	})
}

func TestConcurrencyWaitCancelsWhenLastCallbackIsRemoved(t *testing.T) {
	t.Parallel()
	r, lim, calls := newWaitingConcurrencyLimiter()
	t.Cleanup(r.Stop)
	cb := &testCallback{}
	_, err := lim.check(cb, 1)
	require.ErrorIs(t, err, ErrConcurrencyBlocked)
	call := await.Rcv(t, calls)

	r.CancelAllCallbacks(testNsID, cb)
	require.ErrorIs(t, call.ctx.Err(), context.Canceled)
}

func TestConcurrencyWaitCancelsOnReadinessStop(t *testing.T) {
	t.Parallel()
	r, lim, calls := newWaitingConcurrencyLimiter()
	t.Cleanup(r.Stop)
	_, err := lim.check(&testCallback{}, 1)
	require.ErrorIs(t, err, ErrConcurrencyBlocked)
	call := await.Rcv(t, calls)

	r.Stop()
	require.ErrorIs(t, call.ctx.Err(), context.Canceled)
}

func TestConcurrencyWaitRestartsForHigherPriority(t *testing.T) {
	t.Parallel()
	r, lim, calls := newWaitingConcurrencyLimiter()
	t.Cleanup(r.Stop)
	age := time.Unix(100, 0)
	highPriority := makeWakePriority(1, age)
	lowPriority := makeWakePriority(2, age)
	_, err := lim.check(&testCallback{}, lowPriority)
	require.ErrorIs(t, err, ErrConcurrencyBlocked)
	first := await.Rcv(t, calls)
	require.EqualValues(t, lowPriority, first.req.WakePriority)

	_, err = lim.check(&testCallback{}, highPriority)
	require.ErrorIs(t, err, ErrConcurrencyBlocked)
	require.ErrorIs(t, first.ctx.Err(), context.Canceled)
	replacement := await.Rcv(t, calls)
	require.NoError(t, replacement.ctx.Err())
	require.EqualValues(t, highPriority, replacement.req.WakePriority)
	require.EqualValues(t, 2, replacement.req.RequestedWakeTokens)
}

func TestConcurrencyWaitNotFoundAllowsReserve(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		client := &testClient{
			wait: func(context.Context, *fcpb.ConcurrencyServiceWaitRequest) (*fcpb.ConcurrencyServiceWaitResponse, error) {
				return nil, serviceerror.NewNotFound("limiter not found")
			},
			batch: func(context.Context, *fcpb.ConcurrencyServiceBatchRequest) (*fcpb.ConcurrencyServiceBatchResponse, error) {
				return &fcpb.ConcurrencyServiceBatchResponse{Generation: 7, ReserveSuccess: []bool{true}}, nil
			},
		}
		r := NewReadiness(client, NewReadinessConfig(dynamicconfig.NewNoopCollection()))
		defer r.Stop()
		setTokens(r, 7, 0)
		ready := make(chan struct{}, 1)
		cb := &testCallback{onReady: func() { ready <- struct{}{} }}
		task := newConcurrencyTestTask()
		require.ErrorIs(t, r.NewTx(testNsID, task, cb).Check(), ErrConcurrencyBlocked)
		await.Rcv(t, ready)

		retry := r.NewTx(testNsID, task, cb)
		defer retry.Close()
		require.NoError(t, retry.Check())
		require.Zero(t, getTokens(r))
		require.NoError(t, retry.Reserve(t.Context()))
	})
}

func TestConcurrencyWaitRetriesTransientError(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r, lim, calls := newWaitingConcurrencyLimiter()
		defer r.Stop()
		_, err := lim.check(&testCallback{}, 1)
		require.ErrorIs(t, err, ErrConcurrencyBlocked)
		first := await.Rcv(t, calls)
		await.Snd(t, first.reply, concurrencyWaitResult{err: serviceerror.NewUnavailable("try again")})
		synctest.Wait()
		select {
		case <-calls:
			t.Fatal("Wait retried before backing off")
		default:
		}

		retry := await.Rcv(t, calls)
		protorequire.ProtoEqual(t, first.req, retry.req)
	})
}
