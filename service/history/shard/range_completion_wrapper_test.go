package shard

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
)

// executionManagerOverridingShardContext models a downstream ShardContext wrapper that embeds
// *ContextImpl and decorates the execution manager.
type executionManagerOverridingShardContext struct {
	*ContextImpl
	decorated persistence.ExecutionManager
}

func (c *executionManagerOverridingShardContext) GetExecutionManager() persistence.ExecutionManager {
	return c.decorated
}

// Before admission was added, cleanup callers used shardContext.GetExecutionManager(). A wrapper's
// override must still receive the unchanged, fenced request with and without admission.
func (s *contextSuite) TestRangeCompleteHistoryTasks_WrapperExecutionManagerOverrideHonored() {
	for _, tc := range []struct {
		name      string
		serialize bool
	}{
		{"sql (no admission)", false},
		{"cassandra (admission)", true},
	} {
		s.Run(tc.name, func() {
			s.mockShard.serializeRangeCompletion = tc.serialize
			decorated := persistence.NewMockExecutionManager(s.controller)
			wrapper := &executionManagerOverridingShardContext{ContextImpl: s.mockShard.ContextImpl, decorated: decorated}
			req := s.newRangeCompletionRequest()
			var seen atomic.Pointer[persistence.RangeCompleteHistoryTasksRequest]
			// No expectation on the embedded s.mockExecutionManager: gomock fails if it is used.
			decorated.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), req).DoAndReturn(
				func(_ context.Context, got *persistence.RangeCompleteHistoryTasksRequest) error {
					seen.Store(got)
					return nil
				})

			if tc.serialize {
				// The wrapper still gets admission: cleanup waits while the token is held.
				s.NoError(s.mockShard.ioSemaphore.Acquire(context.Background(), locks.PriorityHigh, 1))
				release := sync.OnceFunc(func() { s.mockShard.ioSemaphore.Release(1) })
				defer release()
				done := make(chan error, 1)
				go func() {
					ctx, cancel := queueStyleContext()
					defer cancel()
					done <- RangeCompleteHistoryTasks(ctx, wrapper, req)
				}()
				s.Never(func() bool { return seen.Load() != nil }, admissionObservationWindow, time.Millisecond)
				release()
				s.NoError(await.Rcv(s.T(), done))
			} else {
				ctx, cancel := queueStyleContext()
				defer cancel()
				s.NoError(RangeCompleteHistoryTasks(ctx, wrapper, req))
			}
			s.Same(req, seen.Load())
			s.Equal(int64(7), seen.Load().RangeID)
		})
	}
}
