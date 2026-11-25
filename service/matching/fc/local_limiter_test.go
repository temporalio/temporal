package fc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/matching/simplelimiter"
)

func TestCheckWithoutCallbackRollsBackWithoutRegisteringWaiters(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource().Update(time.Unix(1, 0))
	ts.UseAsyncTimers(true)
	r := NewReadiness(ts)
	t.Cleanup(r.Stop)
	const testNsID = namespace.ID("nsid")
	task := &testTask{limiters: []Limiter{
		{Type: enumsspb.LIMITER_TYPE_LOCAL_RATE_LIMIT, Key: "first", Source: LimiterSourceConfig, Config: new(simplelimiter.MakeParams(1, 0))},
		{Type: enumsspb.LIMITER_TYPE_LOCAL_RATE_LIMIT, Key: "second", Source: LimiterSourceConfig, Config: new(simplelimiter.MakeParams(0, 0))},
	}}
	tx := r.NewTx(testNsID, task, nil)
	require.ErrorIs(t, tx.Check(), ErrLocalLimiterBlocked)
	require.True(t, tx.closed)
	rs := r.getShard(testNsID)
	func() {
		rs.lock.Lock()
		defer rs.lock.Unlock()
		require.Empty(t, rs.fwd)
		require.Empty(t, rs.rev)
	}()

	// The first limiter's token must have been returned when the second blocked.
	task.limiters = task.limiters[:1]
	tx = r.NewTx(testNsID, task, nil)
	require.NoError(t, tx.Check())
	tx.Close()
}

func TestLocalLimiterWakesOneWaiterPerInterval(t *testing.T) {
	t.Parallel()
	r, ts := newLocalLimiterTestReadiness(t)
	high := newLocalLimiterTestTask(1)
	low := newLocalLimiterTestTask(1)
	low.priority = 2
	notifications := make(chan string, 2)
	highReady := &testCallback{onReady: func() { notifications <- "high" }}
	lowReady := &testCallback{onReady: func() { notifications <- "low" }}

	tx := r.NewTx(localLimiterTestNamespace, high, nil)
	require.NoError(t, tx.Check())
	tx.Close()
	require.ErrorIs(t, r.NewTx(localLimiterTestNamespace, low, lowReady).Check(), ErrLocalLimiterBlocked)
	require.ErrorIs(t, r.NewTx(localLimiterTestNamespace, high, highReady).Check(), ErrLocalLimiterBlocked)

	ts.Advance(time.Second - time.Nanosecond)
	require.Equal(t, 1, ts.NumTimers())
	require.Empty(t, notifications)
	ts.Advance(time.Nanosecond)
	require.Equal(t, "high", await.Rcv(t, notifications))
	require.Equal(t, 1, ts.NumTimers())

	// Waking a waiter does not spend its token. The fresh Check spends it.
	tx = r.NewTx(localLimiterTestNamespace, high, highReady)
	require.NoError(t, tx.Check())
	tx.Close()
	ts.Advance(time.Second)
	require.Equal(t, "low", await.Rcv(t, notifications))
	require.Zero(t, ts.NumTimers())
}

func TestLocalLimiterCancelLastWaiterStopsTimer(t *testing.T) {
	t.Parallel()
	r, ts := newLocalLimiterTestReadiness(t)
	task := newLocalLimiterTestTask(1)
	tx := r.NewTx(localLimiterTestNamespace, task, nil)
	require.NoError(t, tx.Check())
	tx.Close()
	notifications := make(chan struct{}, 1)
	cb := &testCallback{onReady: func() { notifications <- struct{}{} }}
	require.ErrorIs(t, r.NewTx(localLimiterTestNamespace, task, cb).Check(), ErrLocalLimiterBlocked)
	require.Equal(t, 1, ts.NumTimers())

	r.CancelAllCallbacks(localLimiterTestNamespace, cb)
	require.Zero(t, ts.NumTimers())
	ts.Advance(time.Second)
	require.Empty(t, notifications)
}

func TestLocalLimiterStopCancelsPendingTimers(t *testing.T) {
	t.Parallel()
	r, ts := newLocalLimiterTestReadiness(t)
	task := newLocalLimiterTestTask(0)
	notifications := make(chan struct{}, 1)
	cb := &testCallback{onReady: func() { notifications <- struct{}{} }}
	require.ErrorIs(t, r.NewTx(localLimiterTestNamespace, task, cb).Check(), ErrLocalLimiterBlocked)
	require.Equal(t, 1, ts.NumTimers())

	r.Stop()
	require.Zero(t, ts.NumTimers())
	require.Empty(t, notifications)
}

func TestLocalLimiterRollbackWakesWaiter(t *testing.T) {
	t.Parallel()
	r, ts := newLocalLimiterTestReadiness(t)
	task := newLocalLimiterTestTask(1)
	notifications := make(chan struct{}, 1)
	cb := &testCallback{onReady: func() { notifications <- struct{}{} }}
	tx := r.NewTx(localLimiterTestNamespace, task, nil)
	require.NoError(t, tx.Check())
	require.ErrorIs(t, r.NewTx(localLimiterTestNamespace, task, cb).Check(), ErrLocalLimiterBlocked)
	require.Equal(t, 1, ts.NumTimers())

	tx.Rollback(t.Context())
	await.Rcv(t, notifications)
	require.Zero(t, ts.NumTimers())
	retry := r.NewTx(localLimiterTestNamespace, task, cb)
	require.NoError(t, retry.Check())
	retry.Close()
}

func TestLocalLimiterCommitKeepsOneTokenConsumed(t *testing.T) {
	t.Parallel()
	r, ts := newLocalLimiterTestReadiness(t)
	task := newLocalLimiterTestTask(1)
	tx := r.NewTx(localLimiterTestNamespace, task, nil)
	require.NoError(t, tx.Check())
	require.NoError(t, tx.Reserve(t.Context()))
	require.NoError(t, tx.Commit(t.Context()))
	tx.Close()

	ts.Advance(time.Second - time.Nanosecond)
	require.ErrorIs(t, r.NewTx(localLimiterTestNamespace, task, nil).Check(), ErrLocalLimiterBlocked)
	ts.Advance(time.Nanosecond)
	retry := r.NewTx(localLimiterTestNamespace, task, nil)
	require.NoError(t, retry.Check())
	retry.Close()
}

func TestLocalLimiterIncreaseFromZeroReschedulesWaiter(t *testing.T) {
	t.Parallel()
	r, ts := newLocalLimiterTestReadiness(t)
	checkLocalLimiterRateIncrease(t, r, ts, newLocalLimiterTestTask(0))
}

func TestLocalLimiterIncreaseFromOnePerDayReschedulesWaiter(t *testing.T) {
	t.Parallel()
	r, ts := newLocalLimiterTestReadiness(t)
	slow := newLocalLimiterTestTask(1.0 / (24 * time.Hour).Seconds())
	tx := r.NewTx(localLimiterTestNamespace, slow, nil)
	require.NoError(t, tx.Check())
	tx.Close()
	checkLocalLimiterRateIncrease(t, r, ts, slow)
}

func checkLocalLimiterRateIncrease(t *testing.T, r *Readiness, ts *clock.EventTimeSource, slow *localLimiterTestTask) {
	t.Helper()
	notifications := make(chan struct{}, 1)
	cb := &testCallback{onReady: func() { notifications <- struct{}{} }}
	require.ErrorIs(t, r.NewTx(localLimiterTestNamespace, slow, cb).Check(), ErrLocalLimiterBlocked)
	require.Equal(t, 1, ts.NumTimers())

	// A fresh Check installs the new rate. It clips existing debt to one interval and leaves
	// the original waiter registered.
	fast := newLocalLimiterTestTask(1)
	require.ErrorIs(t, r.NewTx(localLimiterTestNamespace, fast, nil).Check(), ErrLocalLimiterBlocked)
	ts.Advance(time.Second - time.Nanosecond)
	require.Equal(t, 1, ts.NumTimers())
	require.Empty(t, notifications)
	ts.Advance(time.Nanosecond)
	await.Rcv(t, notifications)
	require.Zero(t, ts.NumTimers())
	retry := r.NewTx(localLimiterTestNamespace, fast, cb)
	require.NoError(t, retry.Check())
	retry.Close()
}

func TestLocalLimiterDecreasePreservesDebt(t *testing.T) {
	t.Parallel()
	r, ts := newLocalLimiterTestReadiness(t)
	tx := r.NewTx(localLimiterTestNamespace, newLocalLimiterTestTask(1), nil)
	require.NoError(t, tx.Check())
	tx.Close()

	slow := newLocalLimiterTestTask(0.5)
	require.ErrorIs(t, r.NewTx(localLimiterTestNamespace, slow, nil).Check(), ErrLocalLimiterBlocked)
	ts.Advance(time.Second)
	tx = r.NewTx(localLimiterTestNamespace, slow, nil)
	require.NoError(t, tx.Check())
	tx.Close()

	ts.Advance(time.Second)
	require.ErrorIs(t, r.NewTx(localLimiterTestNamespace, slow, nil).Check(), ErrLocalLimiterBlocked)
	ts.Advance(time.Second)
	tx = r.NewTx(localLimiterTestNamespace, slow, nil)
	require.NoError(t, tx.Check())
	tx.Close()
}

const localLimiterTestNamespace = namespace.ID("nsid")

type localLimiterTestTask struct {
	testTask
	priority int32
}

func (t *localLimiterTestTask) PriorityAndAge() (int32, time.Time) {
	return t.priority, time.Unix(1, 0)
}

func newLocalLimiterTestTask(rate float64) *localLimiterTestTask {
	return &localLimiterTestTask{
		testTask: testTask{limiters: []Limiter{{
			Type:   enumsspb.LIMITER_TYPE_LOCAL_RATE_LIMIT,
			Key:    "queue",
			Source: LimiterSourceConfig,
			Config: new(simplelimiter.MakeParams(rate, 0)),
		}}},
		priority: 1,
	}
}

func newLocalLimiterTestReadiness(t *testing.T) (*Readiness, *clock.EventTimeSource) {
	t.Helper()
	ts := clock.NewEventTimeSource().Update(time.Unix(1, 0))
	ts.UseAsyncTimers(true)
	r := NewReadiness(ts)
	t.Cleanup(r.Stop)
	return r, ts
}
