package scheduler

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
)

// Export unexported methods for testing.

func (s *Scheduler) RecordCompletedAction(
	ctx chasm.MutableContext,
	scheduleTime time.Time,
	workflowID string,
	workflowStatus enumspb.WorkflowExecutionStatus,
) {
	s.recordCompletedAction(scheduleTime, workflowID, workflowStatus)
}
