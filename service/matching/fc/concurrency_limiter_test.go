package fc

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc"
)

const (
	testNsID   = namespace.ID("nsid")
	testConKey = "conkey"
)

type testClient struct {
	fcpb.ConcurrencyServiceClient
	batch func(context.Context, *fcpb.ConcurrencyServiceBatchRequest) (*fcpb.ConcurrencyServiceBatchResponse, error)
	wait  func(context.Context, *fcpb.ConcurrencyServiceWaitRequest) (*fcpb.ConcurrencyServiceWaitResponse, error)
}

func (c *testClient) Batch(ctx context.Context, req *fcpb.ConcurrencyServiceBatchRequest, _ ...grpc.CallOption) (*fcpb.ConcurrencyServiceBatchResponse, error) {
	return c.batch(ctx, req)
}

func (c *testClient) Wait(ctx context.Context, req *fcpb.ConcurrencyServiceWaitRequest, _ ...grpc.CallOption) (*fcpb.ConcurrencyServiceWaitResponse, error) {
	return c.wait(ctx, req)
}

func TestOptimisticConcurrencyReserveAllowed(t *testing.T) {
	t.Parallel()
	testOptimisticConcurrencyReserve(t, true)
}

func TestOptimisticConcurrencyReserveBlocked(t *testing.T) {
	t.Parallel()
	testOptimisticConcurrencyReserve(t, false)
}

func testOptimisticConcurrencyReserve(t *testing.T, allow bool) {
	t.Helper()
	const generation = 7
	var batches []*fcpb.ConcurrencyServiceBatchRequest
	client := &testClient{batch: func(_ context.Context, req *fcpb.ConcurrencyServiceBatchRequest) (*fcpb.ConcurrencyServiceBatchResponse, error) {
		batches = append(batches, req)
		if len(req.ReserveSlots) > 0 {
			return &fcpb.ConcurrencyServiceBatchResponse{Generation: generation, ReserveSuccess: []bool{allow}}, nil
		}
		return &fcpb.ConcurrencyServiceBatchResponse{Generation: generation, AvailableSlotsHint: 2, CommitSuccess: []bool{true}}, nil
	}}
	r := NewReadiness(client, NewReadinessConfig(dynamicconfig.NewNoopCollection()))
	t.Cleanup(r.Stop)
	tx := r.NewTx(testNsID, newConcurrencyTestTask(), nil)
	defer tx.Close()
	require.EqualValues(t, 1, getTokens(r))
	require.NoError(t, tx.Check())
	require.Empty(t, batches)

	err := tx.Reserve(t.Context())
	require.Len(t, batches, 1)
	require.Zero(t, getTokens(r))
	require.EqualValues(t, generation, r.getConcurrencyLimiter(testNsID, testConKey).generation)
	if !allow {
		require.ErrorAs(t, err, new(*serviceerrors.FlowControlBlocked))
		return
	}
	require.NoError(t, err)
	require.Len(t, batches[0].ReserveSlots, 1)
	slotID := batches[0].ReserveSlots[0]
	require.NotEmpty(t, slotID)
	refs := tx.LimiterRefs()
	require.Len(t, refs, 1)
	protorequire.ProtoEqual(t, &taskqueuespb.LimiterRef{
		LimiterType: enumsspb.LIMITER_TYPE_CONCURRENCY, Key: testConKey, SlotId: slotID,
	}, refs[0])
	require.NoError(t, tx.Commit(t.Context()))
	require.Len(t, batches, 2)
	require.Equal(t, []string{slotID}, batches[1].CommitSlots)
	require.EqualValues(t, 2, getTokens(r))
}

func TestConcurrencyRollbackRefundsCheckToken(t *testing.T) {
	t.Parallel()
	r, tx := checkedConcurrencyTx(t)
	tx.Rollback(t.Context())
	require.EqualValues(t, 1, getTokens(r))
}

func TestConcurrencyRollbackPreservesReplacementTokens(t *testing.T) {
	t.Parallel()
	r, tx := checkedConcurrencyTx(t)
	r.getConcurrencyLimiter(testNsID, testConKey).reportSlotsHint(1, 3, notifySync)
	tx.Rollback(t.Context())
	require.EqualValues(t, 3, getTokens(r))
}

func TestConcurrencyIgnoresOlderGenerationHint(t *testing.T) {
	t.Parallel()
	r, tx := checkedConcurrencyTx(t)
	r.getConcurrencyLimiter(testNsID, testConKey).reportSlotsHint(0, 3, notifySync)
	require.Zero(t, getTokens(r))
	tx.Rollback(t.Context())
	require.EqualValues(t, 1, getTokens(r))
}

func checkedConcurrencyTx(t *testing.T) (*Readiness, *Tx) {
	t.Helper()
	r := NewReadiness(nil, NewReadinessConfig(dynamicconfig.NewNoopCollection()))
	t.Cleanup(r.Stop)
	setTokens(r, 1, 1)
	tx := r.NewTx(testNsID, newConcurrencyTestTask(), nil)
	require.NoError(t, tx.Check())
	require.Zero(t, getTokens(r))
	return r, tx
}

func TestConcurrencyRollbackCancelsReservationAfterCallerCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		const timeout = 5 * time.Second
		type cancelCall struct {
			req         *fcpb.ConcurrencyServiceBatchRequest
			contextErr  error
			deadline    time.Time
			hasDeadline bool
		}
		cancels := make(chan cancelCall, 1)
		var reservedSlot string
		client := &testClient{batch: func(ctx context.Context, req *fcpb.ConcurrencyServiceBatchRequest) (*fcpb.ConcurrencyServiceBatchResponse, error) {
			if len(req.ReserveSlots) == 1 {
				reservedSlot = req.ReserveSlots[0]
				return &fcpb.ConcurrencyServiceBatchResponse{Generation: 1, ReserveSuccess: []bool{true}}, nil
			}
			deadline, ok := ctx.Deadline()
			cancels <- cancelCall{req: req, contextErr: ctx.Err(), deadline: deadline, hasDeadline: ok}
			return &fcpb.ConcurrencyServiceBatchResponse{Generation: 1, AvailableSlotsHint: 1}, nil
		}}
		r := NewReadiness(client, readinessConfig{cancelReservationTimeout: func() time.Duration { return timeout }})
		defer r.Stop()
		tx := r.NewTx(testNsID, newConcurrencyTestTask(), nil)
		defer tx.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		require.NoError(t, tx.Check())
		require.NoError(t, tx.Reserve(ctx))
		require.NotEmpty(t, reservedSlot)
		refs := tx.LimiterRefs()
		require.Len(t, refs, 1)
		require.Equal(t, reservedSlot, refs[0].SlotId)
		cancel()
		deadline := time.Now().Add(timeout)
		tx.Rollback(ctx)

		call := await.Rcv(t, cancels)
		protorequire.ProtoEqual(t, &fcpb.ConcurrencyServiceBatchRequest{
			NamespaceId: testNsID.String(), Key: testConKey, CancelReservationSlots: []string{reservedSlot},
		}, call.req)
		require.NoError(t, call.contextErr)
		require.True(t, call.hasDeadline)
		require.Equal(t, deadline, call.deadline)
		synctest.Wait()
		require.EqualValues(t, 1, getTokens(r))
	})
}

func TestSkipCheck_DoesNotConsumeOrReturnTokens(t *testing.T) {
	r := NewReadiness(nil, NewReadinessConfig(dynamicconfig.NewNoopCollection()))
	t.Cleanup(r.Stop)
	setTokens(r, 1, 5)

	tx := r.NewTx(testNsID, newConcurrencyTestTask(), &testCallback{})
	require.NoError(t, tx.SkipCheck())
	require.Equal(t, int32(5), getTokens(r))

	tx.Rollback(context.Background())
	require.Equal(t, int32(5), getTokens(r))
}

func newConcurrencyTestTask() *testTask {
	return &testTask{limiters: []Limiter{{
		Type:   enumsspb.LIMITER_TYPE_CONCURRENCY,
		Key:    testConKey,
		Source: LimiterSourceConfig,
	}}}
}

func setTokens(r *Readiness, gen int64, tokens int32) {
	cs := r.getConcurrencyLimiter(testNsID, testConKey)
	cs.reportWaitResponse(gen, tokens, notifySync)
}

func getTokens(r *Readiness) int32 {
	rs := r.getShard(testNsID)
	rs.lock.Lock()
	defer rs.lock.Unlock()
	return rs.concurrencyLimiters[testNsID.String()+testConKey].tokens
}
