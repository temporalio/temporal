package shard

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

const admissionObservationWindow = 300 * time.Millisecond

// rangeCompletionSemaphore observes admission attempts while retaining the real semaphore.
type rangeCompletionSemaphore struct {
	locks.PrioritySemaphore
	acquiring chan locks.Priority
}

func (s *rangeCompletionSemaphore) Acquire(ctx context.Context, priority locks.Priority, n int) error {
	s.acquiring <- priority
	return s.PrioritySemaphore.Acquire(ctx, priority, n)
}

func (s *contextSuite) newRangeCompletionRequest() *persistence.RangeCompleteHistoryTasksRequest {
	// Deliberately differs from the shard's current range ID (1): the captured epoch must be
	// forwarded as-is, never refreshed by the shard context.
	return &persistence.RangeCompleteHistoryTasksRequest{
		ShardID:             s.shardID,
		RangeID:             7,
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(time.Unix(0, 0), 0),
		ExclusiveMaxTaskKey: tasks.NewKey(time.Unix(10, 0), 0),
	}
}

func (s *contextSuite) newForegroundUpdateRequest() *persistence.UpdateWorkflowExecutionRequest {
	return &persistence.UpdateWorkflowExecutionRequest{
		ShardID: s.shardID,
		Mode:    persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(),
				WorkflowId:  tests.WorkflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: tests.RunID},
			Tasks:          map[tasks.Category][]tasks.Task{},
		},
	}
}

func queueStyleContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	return headers.SetCallerInfo(ctx, headers.SystemBackgroundHighCallerInfo), cancel
}

func (s *contextSuite) foregroundUpdate(req *persistence.UpdateWorkflowExecutionRequest) error {
	_, err := s.mockShard.UpdateWorkflowExecution(context.Background(), req)
	return err
}

// The foreground write is inside persistence (holding the shard I/O semaphore). Cleanup must not
// reach persistence until that write returns.
func (s *contextSuite) TestRangeCompleteHistoryTasks_WaitsForInFlightForegroundWrite() {
	s.mockShard.serializeRangeCompletion = true
	s.assertForegroundThenCleanup(false)
}

// Negative control: without admission (SQL behaviour) cleanup reaches
// persistence while the foreground write is still in flight.
func (s *contextSuite) TestRangeCompleteHistoryTasks_NegativeControl_OverlapsWithoutAdmission() {
	s.mockShard.serializeRangeCompletion = false
	s.assertForegroundThenCleanup(true)
}

func (s *contextSuite) assertForegroundThenCleanup(expectOverlap bool) {
	updateReq := s.newForegroundUpdateRequest()
	cleanupReq := s.newRangeCompletionRequest()
	fgEntered := make(chan struct{})
	releaseFg := make(chan struct{})
	finishForeground := sync.OnceFunc(func() { close(releaseFg) })
	defer finishForeground()
	var fgInPersistence, cleanupInPersistence, overlapped atomic.Bool
	var seenRequest atomic.Pointer[persistence.RangeCompleteHistoryTasksRequest]

	s.mockExecutionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), updateReq).DoAndReturn(
		func(context.Context, *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			fgInPersistence.Store(true)
			close(fgEntered)
			<-releaseFg
			fgInPersistence.Store(false)
			return &persistence.UpdateWorkflowExecutionResponse{}, nil
		})
	s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *persistence.RangeCompleteHistoryTasksRequest) error {
			seenRequest.Store(req)
			if fgInPersistence.Load() {
				overlapped.Store(true)
			}
			cleanupInPersistence.Store(true)
			return nil
		})

	fgErr := make(chan error, 1)
	cleanupErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() { fgErr <- s.foregroundUpdate(updateReq) })
	await.Rcv(s.T(), fgEntered)
	wg.Go(func() {
		ctx, cancel := queueStyleContext()
		defer cancel()
		// Route through the package helper, as queueBase and replication cleanup do.
		cleanupErr <- RangeCompleteHistoryTasks(ctx, s.mockShard, cleanupReq)
	})

	if expectOverlap {
		await.RequireTrue(s.T(), cleanupInPersistence.Load, admissionObservationWindow, time.Millisecond)
	} else {
		s.Never(cleanupInPersistence.Load, admissionObservationWindow, time.Millisecond)
	}
	finishForeground()
	s.NoError(await.Rcv(s.T(), fgErr))
	s.NoError(await.Rcv(s.T(), cleanupErr))
	wg.Wait()
	s.True(cleanupInPersistence.Load())
	s.Equal(expectOverlap, overlapped.Load())
	s.Same(cleanupReq, seenRequest.Load())
	s.Equal(int64(7), seenRequest.Load().RangeID)
}

// Cleanup is inside persistence. A foreground write must not reach persistence until it returns.
func (s *contextSuite) TestRangeCompleteHistoryTasks_BlocksForegroundWriteWhileInFlight() {
	s.mockShard.serializeRangeCompletion = true
	updateReq := s.newForegroundUpdateRequest()
	cleanupReq := s.newRangeCompletionRequest()
	cleanupEntered := make(chan struct{})
	releaseCleanup := make(chan struct{})
	finishCleanup := sync.OnceFunc(func() { close(releaseCleanup) })
	defer finishCleanup()
	var cleanupInPersistence, fgInPersistence, overlapped atomic.Bool

	s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), cleanupReq).DoAndReturn(
		func(context.Context, *persistence.RangeCompleteHistoryTasksRequest) error {
			cleanupInPersistence.Store(true)
			close(cleanupEntered)
			<-releaseCleanup
			cleanupInPersistence.Store(false)
			return nil
		})
	s.mockExecutionManager.EXPECT().UpdateWorkflowExecution(gomock.Any(), updateReq).DoAndReturn(
		func(context.Context, *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			if cleanupInPersistence.Load() {
				overlapped.Store(true)
			}
			fgInPersistence.Store(true)
			return &persistence.UpdateWorkflowExecutionResponse{}, nil
		})

	fgErr := make(chan error, 1)
	cleanupErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		ctx, cancel := queueStyleContext()
		defer cancel()
		cleanupErr <- s.mockShard.RangeCompleteHistoryTasks(ctx, cleanupReq)
	})
	await.Rcv(s.T(), cleanupEntered)
	wg.Go(func() { fgErr <- s.foregroundUpdate(updateReq) })
	s.Never(fgInPersistence.Load, admissionObservationWindow, time.Millisecond)
	finishCleanup()
	s.NoError(await.Rcv(s.T(), cleanupErr))
	s.NoError(await.Rcv(s.T(), fgErr))
	wg.Wait()
	s.True(fgInPersistence.Load())
	s.False(overlapped.Load())
}

// Replication cleanup uses a preemptable caller. Range completion must still be admitted at
// PriorityHigh, not PriorityLow, which can be starved indefinitely.
func (s *contextSuite) TestRangeCompleteHistoryTasks_PreemptableCallerAdmittedAtHighPriority() {
	s.mockShard.serializeRangeCompletion = true
	semaphore := &rangeCompletionSemaphore{
		PrioritySemaphore: s.mockShard.ioSemaphore,
		acquiring:         make(chan locks.Priority, 1),
	}
	s.mockShard.ioSemaphore = semaphore
	s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), gomock.Any()).Return(nil)

	cleanupErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ctx = headers.SetCallerInfo(ctx, headers.SystemPreemptableCallerInfo)
		cleanupErr <- RangeCompleteHistoryTasks(ctx, s.mockShard, s.newRangeCompletionRequest())
	}()
	s.Equal(locks.PriorityHigh, await.Rcv(s.T(), semaphore.acquiring))
	s.NoError(await.Rcv(s.T(), cleanupErr))
}

// A shard that is stopping must not issue the conditional delete.
func (s *contextSuite) TestRangeCompleteHistoryTasks_NotIssuedWhenShardStopping() {
	s.mockShard.serializeRangeCompletion = true
	s.mockShard.stateLock.Lock()
	s.mockShard.state = contextStateStopping
	s.mockShard.stateLock.Unlock()

	ctx, cancel := queueStyleContext()
	defer cancel()
	err := s.mockShard.RangeCompleteHistoryTasks(ctx, s.newRangeCompletionRequest())
	var ownershipLost *persistence.ShardOwnershipLostError
	s.ErrorAs(err, &ownershipLost)
	// No RangeCompleteHistoryTasks expectation: gomock fails on any persistence call.
}

// A shard that is re-acquiring (after an ambiguous write) must not issue the conditional delete.
func (s *contextSuite) TestRangeCompleteHistoryTasks_NotIssuedWhileShardAcquiring() {
	s.mockShard.serializeRangeCompletion = true
	s.mockShard.stateLock.Lock()
	s.mockShard.state = contextStateAcquiring
	s.mockShard.stateLock.Unlock()

	ctx, cancel := queueStyleContext()
	defer cancel()
	s.ErrorIs(s.mockShard.RangeCompleteHistoryTasks(ctx, s.newRangeCompletionRequest()), ErrShardStatusUnknown)
}

// Waiting for admission ends when the shard lifecycle ends, without a persistence call or a leaked token.
func (s *contextSuite) TestRangeCompleteHistoryTasks_LifecycleCancelWhileWaiting() {
	s.mockShard.serializeRangeCompletion = true
	s.NoError(s.mockShard.ioSemaphore.Acquire(context.Background(), locks.PriorityHigh, 1))
	release := sync.OnceFunc(func() { s.mockShard.ioSemaphore.Release(1) })
	defer release()
	semaphore := &rangeCompletionSemaphore{
		PrioritySemaphore: s.mockShard.ioSemaphore,
		acquiring:         make(chan locks.Priority, 1),
	}
	s.mockShard.ioSemaphore = semaphore

	done := make(chan error, 1)
	go func() {
		ctx, cancel := queueStyleContext()
		defer cancel()
		done <- s.mockShard.RangeCompleteHistoryTasks(ctx, s.newRangeCompletionRequest())
	}()
	await.Rcv(s.T(), semaphore.acquiring)
	s.mockShard.lifecycleCancel()
	select {
	case err := <-done:
		s.ErrorIs(err, context.Canceled)
	case <-time.After(time.Second):
		s.Fail("range completion did not stop waiting after shard lifecycle cancellation")
	}
	release()
	s.True(s.mockShard.ioSemaphore.TryAcquire(locks.PriorityHigh, 1), "semaphore token leaked")
	s.mockShard.ioSemaphore.Release(1)
}

// Caller deadline expiry while waiting returns DeadlineExceeded and leaves the semaphore balanced.
// newDetachedContext measures the remaining time with the shard time source and floors it at
// minContextTimeout, so the effective deadline is max(caller deadline, minContextTimeout).
func (s *contextSuite) TestRangeCompleteHistoryTasks_DeadlineWhileWaiting() {
	s.mockShard.serializeRangeCompletion = true
	s.timeSource.Update(time.Now()) // the suite's EventTimeSource otherwise starts at Unix 0
	s.NoError(s.mockShard.ioSemaphore.Acquire(context.Background(), locks.PriorityHigh, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	s.ErrorIs(s.mockShard.RangeCompleteHistoryTasks(ctx, s.newRangeCompletionRequest()), context.DeadlineExceeded)
	s.GreaterOrEqual(time.Since(start), minContextTimeout-100*time.Millisecond)
	s.Less(time.Since(start), minContextTimeout+2*time.Second)
	s.mockShard.ioSemaphore.Release(1)
	s.True(s.mockShard.ioSemaphore.TryAcquire(locks.PriorityHigh, 1), "semaphore token leaked")
	s.mockShard.ioSemaphore.Release(1)
}

// Persistence errors, including ShardOwnershipLostError from the fence, are returned unchanged.
// They do not stop or re-acquire the shard (no handleWriteError).
func (s *contextSuite) TestRangeCompleteHistoryTasks_PersistenceErrorReturnedWithoutShardTransition() {
	s.mockShard.serializeRangeCompletion = true
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"ownership lost", &persistence.ShardOwnershipLostError{ShardID: s.shardID, Msg: "fenced"}},
		{"ambiguous write", serviceerror.NewUnavailable("write outcome unknown")},
	} {
		s.Run(tc.name, func() {
			req := s.newRangeCompletionRequest()
			// A mismatched epoch would make handleWriteError return early, hiding an unintended call.
			req.RangeID = s.mockShard.GetRangeID()
			s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), req).Return(tc.err)
			ctx, cancel := queueStyleContext()
			defer cancel()
			s.Same(tc.err, s.mockShard.RangeCompleteHistoryTasks(ctx, req))
			s.NoError(s.mockShard.errorByState())
			s.True(s.mockShard.ioSemaphore.TryAcquire(locks.PriorityHigh, 1), "semaphore token leaked")
			s.mockShard.ioSemaphore.Release(1)
		})
	}
}

// Without admission (SQL default), range completion is not gated by the semaphore. The SQL path
// therefore retains direct execution-manager access.
func (s *contextSuite) TestRangeCompleteHistoryTasks_NotAdmittedWhenSerializationDisabled() {
	s.mockShard.serializeRangeCompletion = false
	s.NoError(s.mockShard.ioSemaphore.Acquire(context.Background(), locks.PriorityHigh, 1))
	defer s.mockShard.ioSemaphore.Release(1)
	req := s.newRangeCompletionRequest()
	s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), req).Return(nil)
	ctx, cancel := queueStyleContext()
	defer cancel()
	s.NoError(RangeCompleteHistoryTasks(ctx, s.mockShard, req))
}

// For shard contexts other than *ContextImpl (mocks, wrappers), the helper falls back to the
// execution manager with the same, unchanged request. The fence is kept; only admission is skipped.
func TestRangeCompleteHistoryTasks_FallbackKeepsFencedRequest(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	shardContext := historyi.NewMockShardContext(ctrl)
	executionManager := persistence.NewMockExecutionManager(ctrl)
	req := &persistence.RangeCompleteHistoryTasksRequest{ShardID: 3, RangeID: 11, TaskCategory: tasks.CategoryTransfer}
	var seen *persistence.RangeCompleteHistoryTasksRequest
	shardContext.EXPECT().GetExecutionManager().Return(executionManager)
	executionManager.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), gomock.Eq(req)).DoAndReturn(
		func(_ context.Context, got *persistence.RangeCompleteHistoryTasksRequest) error {
			seen = got
			return nil
		})
	require.NoError(t, RangeCompleteHistoryTasks(t.Context(), shardContext, req))
	require.Same(t, req, seen)
	require.Equal(t, int64(11), seen.RangeID)
}
