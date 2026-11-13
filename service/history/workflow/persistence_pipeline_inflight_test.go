package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

// asyncWrite is one submission seen by the fake async shard.
type asyncWrite struct {
	version     int64
	historySize int64
	condition   int64
	receipt     *asyncTestReceipt
}

type asyncTestReceipt struct {
	done chan struct{}
	err  error
}

func newAsyncTestReceipt() *asyncTestReceipt {
	return &asyncTestReceipt{done: make(chan struct{})}
}

func (r *asyncTestReceipt) complete(err error) {
	r.err = err
	close(r.done)
}

func (r *asyncTestReceipt) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type asyncPipelineFixture struct {
	c       *ContextImpl
	submits chan asyncWrite
	// submitErr, when set, fails the submission with this condition.
	submitErr map[int64]error
	// published records the DBRecordVersion of each successful publication, in order.
	published []int64
}

func newAsyncPipelineFixture(t *testing.T, maxInFlight int, coalescing bool) *asyncPipelineFixture {
	t.Helper()
	f := &asyncPipelineFixture{submits: make(chan asyncWrite, 16), submitErr: map[int64]error{}}
	ctrl := gomock.NewController(t)
	shard := historyi.NewMockShardContext(ctrl)
	engine := historyi.NewMockEngine(ctrl)
	registry := namespace.NewMockRegistry(ctrl)
	shard.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	shard.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
	shard.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	shard.EXPECT().GetLifecycleContext().Return(t.Context()).AnyTimes()
	shard.EXPECT().GetNamespaceRegistry().Return(registry).AnyTimes()
	shard.EXPECT().SubmitWorkflowExecutionUpdate(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, r *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, persistence.WorkflowExecutionUpdateReceipt, error) {
			m := r.UpdateWorkflowMutation
			if err := f.submitErr[m.Condition]; err != nil {
				return nil, nil, err
			}
			receipt := newAsyncTestReceipt()
			f.submits <- asyncWrite{version: m.DBRecordVersion, historySize: m.ExecutionInfo.ExecutionStats.HistorySize, condition: m.Condition, receipt: receipt}
			return pipelineTestResponse(10), receipt, nil
		},
	).AnyTimes()
	registry.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, serviceerror.NewNotFound("namespace")).AnyTimes()
	engine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	engine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	config := tests.NewDynamicConfig()
	config.WorkflowPersistencePipeline = true
	config.WorkflowPersistenceCoalescing = coalescing
	config.WorkflowPersistencePipelineMaxInFlight = maxInFlight
	f.c = NewContext(config, tests.WorkflowKey, chasm.WorkflowArchetypeID, log.NewNoopLogger(), log.NewNoopLogger(), metrics.NoopMetricsHandler, nil, testhooks.TestHooks{})
	f.c.pipelineShard = shard
	f.c.pipeline.committedDBRecordVersion = 41
	f.c.pipeline.committedHistorySize = 100

	state := historyi.NewMockMutableState(ctrl)
	next, version := int64(10), int64(41)
	state.EXPECT().GetUpdateCondition().DoAndReturn(func() (int64, int64) { return next, version }).AnyTimes()
	state.EXPECT().SetUpdateCondition(gomock.Any(), gomock.Any()).Do(func(n, v int64) {
		next, version = n, v
		f.published = append(f.published, v)
	}).AnyTimes()
	state.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{ExecutionStats: &persistencespb.ExecutionStats{HistorySize: 100}}).AnyTimes()
	f.c.MutableState = state
	return f
}

// submit queues a request and drives it from its own goroutine, as a signal handler does after Unlock.
func (f *asyncPipelineFixture) submit(ctx context.Context, t *testing.T, condition int64) (*submission, chan error) {
	t.Helper()
	s, err := f.c.pipeline.submit(ctx, 1, pipelineTestRequest(t, condition))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- f.c.completeSubmission(s) }()
	return s, done
}

func (f *asyncPipelineFixture) waitSettled(t *testing.T, inFlight, pending int) {
	t.Helper()
	p := f.c.pipeline
	waitPipeline(t, p, func() bool {
		return !p.dispatching && len(p.inFlight) == inFlight && len(p.pending) == pending
	})
}

func (f *asyncPipelineFixture) publishedVersions() []int64 {
	f.c.pipeline.mu.Lock()
	defer f.c.pipeline.mu.Unlock()
	return append([]int64(nil), f.published...)
}

func TestPersistencePipelineMaxInFlightRespected(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, false)
	a, aDone := f.submit(t.Context(), t, 10)
	wa := await.Rcv(t, f.submits)
	_, bDone := f.submit(t.Context(), t, 11)
	wb := await.Rcv(t, f.submits)
	_, cDone := f.submit(t.Context(), t, 12)
	f.waitSettled(t, 2, 1)
	require.Empty(t, f.submits, "a third group was submitted with max in flight 2")

	require.Equal(t, asyncWrite{version: 42, historySize: 100, condition: 10, receipt: wa.receipt}, wa)
	require.Equal(t, asyncWrite{version: 43, historySize: 110, condition: 11, receipt: wb.receipt}, wb)

	wa.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, aDone))
	require.True(t, a.completion.Ready())
	wc := await.Rcv(t, f.submits)
	require.Equal(t, int64(44), wc.version)
	require.Equal(t, int64(120), wc.historySize)

	wb.receipt.complete(nil)
	wc.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, bDone))
	require.NoError(t, await.Rcv(t, cDone))
	require.Equal(t, []int64{42, 43, 44}, f.publishedVersions())
	require.Equal(t, int64(44), f.c.pipeline.committedDBRecordVersion)
	require.Equal(t, int64(130), f.c.pipeline.committedHistorySize)
	require.Empty(t, f.c.pipeline.inFlight)
}

func TestPersistencePipelineCoalescesBehindInFlightGroups(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, true)
	_, aDone := f.submit(t.Context(), t, 10)
	wa := await.Rcv(t, f.submits)
	_, bDone := f.submit(t.Context(), t, 11)
	wb := await.Rcv(t, f.submits)
	// Both slots are taken, so c and d queue and are claimed together.
	c, err := f.c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 12))
	require.NoError(t, err)
	d, err := f.c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 13))
	require.NoError(t, err)
	cdDone := make(chan error, 1)
	go func() {
		cdDone <- f.c.completeOperation(&operation{ctx: t.Context(), submissions: []*submission{c, d}})
	}()
	f.waitSettled(t, 2, 2)

	wa.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, aDone))
	wcd := await.Rcv(t, f.submits)
	require.Equal(t, int64(44), wcd.version)
	require.Equal(t, int64(12), wcd.condition)
	wb.receipt.complete(nil)
	wcd.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, bDone))
	require.NoError(t, await.Rcv(t, cdDone))
	require.Equal(t, []int64{42, 43, 44}, f.publishedVersions())
}

func TestPersistencePipelineReceiptsCompleteOutOfOrder(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 3, false)
	a, aDone := f.submit(t.Context(), t, 10)
	wa := await.Rcv(t, f.submits)
	b, bDone := f.submit(t.Context(), t, 11)
	wb := await.Rcv(t, f.submits)
	c, cDone := f.submit(t.Context(), t, 12)
	wc := await.Rcv(t, f.submits)

	wc.receipt.complete(nil)
	wb.receipt.complete(nil)
	require.False(t, a.completion.Ready())
	require.False(t, b.completion.Ready())
	require.False(t, c.completion.Ready())
	require.Empty(t, f.publishedVersions())

	wa.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, aDone))
	require.NoError(t, await.Rcv(t, bDone))
	require.NoError(t, await.Rcv(t, cDone))
	require.Equal(t, []int64{42, 43, 44}, f.publishedVersions())
	require.Equal(t, int64(44), f.c.pipeline.committedDBRecordVersion)
}

func TestPersistencePipelineFailureFailsLaterInFlightGroups(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, false)
	_, aDone := f.submit(t.Context(), t, 10)
	wa := await.Rcv(t, f.submits)
	_, bDone := f.submit(t.Context(), t, 11)
	wb := await.Rcv(t, f.submits)
	pendingC, cDone := f.submit(t.Context(), t, 12)
	f.waitSettled(t, 2, 1)

	// b becomes durable before a fails. b must still fail because it depends on a.
	wb.receipt.complete(nil)
	writeErr := serviceerror.NewUnavailable("write failed")
	wa.receipt.complete(writeErr)
	require.ErrorIs(t, await.Rcv(t, aDone), writeErr)
	bErr := await.Rcv(t, bDone)
	require.Error(t, bErr)
	require.NotErrorIs(t, bErr, writeErr)
	require.Error(t, await.Rcv(t, cDone))
	require.True(t, pendingC.completion.Ready())
	require.Empty(t, f.submits, "a submission queued behind a failed group was dispatched")

	require.Empty(t, f.publishedVersions())
	require.Equal(t, int64(41), f.c.pipeline.committedDBRecordVersion)
	require.Error(t, f.c.pipeline.err)
	require.Empty(t, f.c.pipeline.inFlight)
	_, err := f.c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 13))
	require.Error(t, err)
}

func TestPersistencePipelineLaterGroupFailureKeepsEarlierGroup(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, false)
	_, aDone := f.submit(t.Context(), t, 10)
	wa := await.Rcv(t, f.submits)
	_, bDone := f.submit(t.Context(), t, 11)
	wb := await.Rcv(t, f.submits)

	writeErr := serviceerror.NewUnavailable("write failed")
	wb.receipt.complete(writeErr)
	waitPipeline(t, f.c.pipeline, func() bool { return f.c.pipeline.err != nil })
	wa.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, aDone))
	require.ErrorIs(t, await.Rcv(t, bDone), writeErr)
	require.Equal(t, []int64{42}, f.publishedVersions())
	require.Equal(t, int64(42), f.c.pipeline.committedDBRecordVersion)
	require.Error(t, f.c.pipeline.err)
}

func TestPersistencePipelineSubmitFailureKeepsEarlierGroup(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, false)
	submitErr := &persistence.WorkflowConditionFailedError{Msg: "conflict"}
	f.submitErr[11] = submitErr
	_, aDone := f.submit(t.Context(), t, 10)
	wa := await.Rcv(t, f.submits)
	_, bDone := f.submit(t.Context(), t, 11)
	waitPipeline(t, f.c.pipeline, func() bool { return f.c.pipeline.err != nil })
	_, err := f.c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 12))
	require.Error(t, err)

	wa.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, aDone))
	require.ErrorIs(t, await.Rcv(t, bDone), submitErr)
	require.Equal(t, []int64{42}, f.publishedVersions())
	require.Empty(t, f.c.pipeline.inFlight)
}

func TestPersistencePipelineCancelBeforeClaimWithGroupsInFlight(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, false)
	_, aDone := f.submit(t.Context(), t, 10)
	wa := await.Rcv(t, f.submits)
	_, bDone := f.submit(t.Context(), t, 11)
	wb := await.Rcv(t, f.submits)
	ctx, cancel := context.WithCancel(t.Context())
	_, cDone := f.submit(ctx, t, 12)
	_, dDone := f.submit(t.Context(), t, 13)
	f.waitSettled(t, 2, 2)

	cancel()
	require.ErrorIs(t, await.Rcv(t, cDone), context.Canceled)
	require.Error(t, await.Rcv(t, dDone))
	wa.receipt.complete(nil)
	wb.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, aDone))
	require.NoError(t, await.Rcv(t, bDone))
	require.Empty(t, f.submits, "a canceled pending submission was dispatched")
	require.Equal(t, []int64{42, 43}, f.publishedVersions())
	require.Error(t, f.c.pipeline.err)
}

func TestPersistencePipelineCancelAfterClaimStillPublishes(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, true)
	ctx, cancel := context.WithCancel(t.Context())
	a, err := f.c.pipeline.submit(ctx, 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	b, err := f.c.pipeline.submit(ctx, 1, pipelineTestRequest(t, 11))
	require.NoError(t, err)
	aDone := make(chan error, 1)
	go func() { aDone <- f.c.completeSubmission(a) }()
	wab := await.Rcv(t, f.submits)
	require.Equal(t, int64(42), wab.version)

	cancel()
	require.ErrorIs(t, f.c.completeSubmission(b), context.Canceled)
	require.False(t, b.completion.Ready())
	// The receipt honors its wait context, so success here shows the owner waited with the
	// shard-lifetime context rather than the canceled caller context.
	wab.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, aDone))
	_, err = b.completion.Get(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{42}, f.publishedVersions())
}

func lockHolders(capture *metricstest.Capture) []string {
	return recordedTags(capture, metrics.WorkflowContextLockHoldDuration.Name(), "holder")
}

func recordedTags(capture *metricstest.Capture, metric, tag string) []string {
	var values []string
	for _, r := range capture.SnapshotMetric(metric) {
		values = append(values, r.Tags[tag])
	}
	return values
}

// captureWaits sends the pipeline's metrics to a capture and signals blocked each time a speculative lock
// waits for admission.
func captureWaits(c *ContextImpl) (*metricstest.Capture, chan struct{}) {
	handler := metricstest.NewCaptureHandler()
	c.metricsHandler = handler
	c.pipeline.metricsHandler = handler
	blocked := make(chan struct{}, 1)
	c.testHooks = testhooks.NewTestHooks()
	testhooks.Set(c.testHooks, testhooks.HistoryPersistencePipelineAdmissionBlocked, func() {
		select {
		case blocked <- struct{}{}:
		default:
		}
	}, namespace.ID(tests.WorkflowKey.NamespaceID))
	return handler.StartCapture(), blocked
}

func requirePositiveDurations(t *testing.T, recordings []*metricstest.CapturedRecording) {
	t.Helper()
	for _, r := range recordings {
		require.Positive(t, r.Value.(time.Duration))
	}
}

func TestPersistencePipelineRecordsWaits(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, false)
	capture, blocked := captureWaits(f.c)

	// A committed lock waits for the group in flight, and a speculative lock waits behind it.
	_, aDone := f.submit(t.Context(), t, 10)
	wa := await.Rcv(t, f.submits)
	locked := make(chan error, 1)
	go func() { locked <- f.c.Lock(t.Context(), locks.PriorityHigh) }()
	// The committed lock records a recheck hold when it releases the lock to wait for the group.
	await.RequireTrue(t, func() bool { return len(lockHolders(capture)) == 1 }, 5*time.Second, time.Millisecond)
	admitted := make(chan error, 1)
	go func() { admitted <- f.c.LockSpeculative(t.Context(), locks.PriorityHigh) }()
	await.Rcv(t, blocked)
	wa.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, aDone))
	require.NoError(t, await.Rcv(t, locked))
	require.NoError(t, f.c.Unlock())
	require.NoError(t, await.Rcv(t, admitted))
	require.NoError(t, f.c.Unlock())

	// Making a speculative operation committed waits for the group in flight.
	_, bDone := f.submit(t.Context(), t, 11)
	wb := await.Rcv(t, f.submits)
	require.NoError(t, f.c.LockSpeculative(t.Context(), locks.PriorityHigh))
	committed := make(chan error, 1)
	go func() { committed <- f.c.makeOperationCommitted(t.Context(), false) }()
	waitPipeline(t, f.c.pipeline, func() bool { return len(f.c.pipeline.waiters) == 1 })
	wb.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, bDone))
	require.NoError(t, await.Rcv(t, committed))
	require.NoError(t, f.c.Unlock())

	require.Len(t, capture.SnapshotMetric(metrics.WorkflowPersistencePipelineClaimLatency.Name()), 2)
	mutexWaits := recordedTags(capture, metrics.WorkflowPersistencePipelineMutexWait.Name(), "site")
	require.ElementsMatch(t, []string{mutexSiteSubmit, mutexSiteSubmit, mutexSiteAdmit, mutexSiteAdmit, mutexSiteAdmit, mutexSiteAdmit}, mutexWaits)
	drains := capture.SnapshotMetric(metrics.WorkflowPersistencePipelineDrainWait.Name())
	require.Equal(t, []string{drainSiteCommitted, drainSiteMakeCommitted}, recordedTags(capture, metrics.WorkflowPersistencePipelineDrainWait.Name(), "site"))
	requirePositiveDurations(t, drains)
	admissions := capture.SnapshotMetric(metrics.WorkflowPersistencePipelineAdmissionWait.Name())
	require.Equal(t, []string{admissionReasonCommittedWaiter}, recordedTags(capture, metrics.WorkflowPersistencePipelineAdmissionWait.Name(), "reason"))
	requirePositiveDurations(t, admissions)
}

func TestPersistencePipelineRecordsSubmissionLimitWait(t *testing.T) {
	t.Parallel()
	c := pipelineTestContext(t, false, func(context.Context, *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		return pipelineTestResponse(0), nil
	})
	capture, blocked := captureWaits(c)
	for i := range maxPipelineSubmissions {
		_, err := c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, int64(10+i)))
		require.NoError(t, err)
	}
	locked := make(chan error, 1)
	go func() { locked <- c.LockSpeculative(t.Context(), locks.PriorityHigh) }()
	await.Rcv(t, blocked)
	// Invalidating the queue drops the submissions, so the caller takes a committed lock instead.
	c.pipeline.mu.Lock()
	c.pipeline.invalidate(0, pipelineDependencyError())
	c.pipeline.mu.Unlock()
	require.NoError(t, await.Rcv(t, locked))
	require.NoError(t, c.Unlock())

	require.Equal(t, []string{admissionReasonSubmissionLimit}, recordedTags(capture, metrics.WorkflowPersistencePipelineAdmissionWait.Name(), "reason"))
	requirePositiveDurations(t, capture.SnapshotMetric(metrics.WorkflowPersistencePipelineAdmissionWait.Name()))
}

func TestPersistencePipelineRecordsLockHoldByHolder(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, false)
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	f.c.metricsHandler = handler

	require.NoError(t, f.c.LockSpeculative(t.Context(), locks.PriorityHigh))
	require.NoError(t, f.c.Unlock())

	// A committed lock taken while a group is in flight rechecks, waits for the publish, then holds.
	_, done := f.submit(t.Context(), t, 10)
	w := await.Rcv(t, f.submits)
	locked := make(chan error, 1)
	go func() { locked <- f.c.Lock(t.Context(), locks.PriorityHigh) }()
	await.RequireTrue(t, func() bool { return len(lockHolders(capture)) == 2 }, 5*time.Second, time.Millisecond)
	w.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, done))
	require.NoError(t, await.Rcv(t, locked))
	require.NoError(t, f.c.Unlock())

	require.NoError(t, f.c.LockSpeculative(t.Context(), locks.PriorityHigh))
	require.NoError(t, f.c.makeOperationCommitted(t.Context(), false))
	require.NoError(t, f.c.Unlock())

	require.True(t, f.c.TryLock(locks.PriorityLow))
	require.NoError(t, f.c.Unlock())

	require.Equal(t, []string{
		lockHolderSpeculative,
		lockHolderRecheck, lockHolderPublish, lockHolderCommitted,
		lockHolderSpeculative, lockHolderCommitted,
		lockHolderTryLock,
	}, lockHolders(capture))
	// A TryLock records no acquire wait.
	require.Equal(t, []string{
		lockAcquirerSpeculative,
		lockAcquirerCommitted, lockAcquirerPublish, lockAcquirerCommitted,
		lockAcquirerSpeculative, lockAcquirerCommitted,
	}, recordedTags(capture, metrics.WorkflowContextLockAcquireWait.Name(), "acquirer"))
	dispatches := capture.SnapshotMetric(metrics.WorkflowPersistencePipelineDispatchLatency.Name())
	require.Len(t, dispatches, 1)
	require.IsType(t, time.Duration(0), dispatches[0].Value)
}

func TestContextRecordsLockHoldWithoutPipeline(t *testing.T) {
	t.Parallel()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	c := NewContext(tests.NewDynamicConfig(), tests.WorkflowKey, chasm.WorkflowArchetypeID, log.NewNoopLogger(), log.NewNoopLogger(), handler, nil, testhooks.TestHooks{})
	require.NoError(t, c.Lock(t.Context(), locks.PriorityHigh))
	require.NoError(t, c.Unlock())
	require.Equal(t, []string{lockHolderOperation}, lockHolders(capture))
	require.Equal(t, []string{lockAcquirerOperation}, recordedTags(capture, metrics.WorkflowContextLockAcquireWait.Name(), "acquirer"))
}

func TestPersistencePipelineAdmissionWakesOnlyOnAdmissionChange(t *testing.T) {
	t.Parallel()
	c := pipelineTestContext(t, false, func(context.Context, *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
		return pipelineTestResponse(0), nil
	})
	p := c.pipeline
	admission := func() chan struct{} {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.admission
	}
	change := func(f func()) {
		p.mu.Lock()
		defer p.mu.Unlock()
		f()
		p.notify()
	}
	closed := func(ch chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}

	before := admission()
	change(func() {})
	require.False(t, closed(before), "a notify without an admission change woke blocked admissions")

	waiter := make(chan struct{})
	change(func() { p.waiters = append(p.waiters, waiter) })
	require.True(t, closed(before), "a committed waiter arriving didn't wake blocked admissions")
	before = admission()
	change(func() {})
	require.False(t, closed(before))
	change(func() { p.waiters = nil })
	require.True(t, closed(before), "the last committed waiter leaving didn't wake blocked admissions")

	before = admission()
	for i := range maxPipelineSubmissions - 1 {
		_, err := p.submit(t.Context(), 1, pipelineTestRequest(t, int64(10+i)))
		require.NoError(t, err)
	}
	require.False(t, closed(before), "submissions under the limit woke blocked admissions")
	_, err := p.submit(t.Context(), 1, pipelineTestRequest(t, int64(10+maxPipelineSubmissions)))
	require.NoError(t, err)
	require.True(t, closed(before), "reaching the submission limit didn't wake blocked admissions")

	before = admission()
	change(func() { p.invalidate(0, pipelineDependencyError()) })
	require.True(t, closed(before), "invalidating the queue didn't wake blocked admissions")
}

func TestPersistencePipelineCommittedWriteReleasesLockBeforeDurable(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, false)
	f.c.pipeline.taskCompletions = true
	require.NoError(t, f.c.Lock(t.Context(), locks.PriorityHigh))
	require.True(t, f.c.PipelineCommittedWrite())
	// Queue the operation's write as executeWorkflowTransaction does for a pipelined operation.
	s, err := f.c.pipeline.submit(t.Context(), 1, pipelineTestRequest(t, 10))
	require.NoError(t, err)
	f.c.operation.submissions = append(f.c.operation.submissions, s)

	unlocked := make(chan error, 1)
	go func() { unlocked <- f.c.Unlock() }()
	w := await.Rcv(t, f.submits)
	require.True(t, f.c.TryLock(locks.PriorityHigh), "the lock is still held while the write is in flight")
	require.NoError(t, f.c.Unlock())
	select {
	case err := <-unlocked:
		require.Failf(t, "Unlock returned before the write was durable", "error: %v", err)
	default:
	}
	w.receipt.complete(nil)
	require.NoError(t, await.Rcv(t, unlocked))
	require.Equal(t, []int64{42}, f.publishedVersions())
}

func TestPersistencePipelineCommittedWriteRefusals(t *testing.T) {
	t.Parallel()
	f := newAsyncPipelineFixture(t, 2, false)
	require.NoError(t, f.c.Lock(t.Context(), locks.PriorityHigh))
	require.False(t, f.c.PipelineCommittedWrite(), "the setting is off")
	require.NoError(t, f.c.Unlock())

	f.c.pipeline.taskCompletions = true
	require.False(t, f.c.PipelineCommittedWrite(), "no operation holds the lock")

	require.NoError(t, f.c.LockSpeculative(t.Context(), locks.PriorityHigh))
	require.False(t, f.c.PipelineCommittedWrite(), "a speculative operation is already pipelined")
	require.NoError(t, f.c.Unlock())

	require.NoError(t, f.c.Lock(t.Context(), locks.PriorityHigh))
	f.c.pipeline.mu.Lock()
	f.c.pipeline.err = pipelineDependencyError()
	f.c.pipeline.mu.Unlock()
	require.False(t, f.c.PipelineCommittedWrite(), "the pipeline is invalid")
	f.c.pipeline.mu.Lock()
	f.c.pipeline.err = nil
	f.c.pipeline.mu.Unlock()
	require.NoError(t, f.c.Unlock())
}
