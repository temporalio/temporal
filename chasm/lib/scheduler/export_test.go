package scheduler

import (
	"time"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
)

// Export unexported methods for testing.

func (s *Scheduler) RecordCompletedAction(
	ctx chasm.MutableContext,
	completed *schedulespb.CompletedResult,
	requestID string,
) time.Time {
	invoker := s.Invoker.Get(ctx)
	return invoker.recordCompletedAction(ctx, completed, requestID)
}

func (i *Invoker) RunningWorkflowID(requestID string) string {
	return i.runningWorkflowID(requestID)
}
