package fact

import (
	historyv1 "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateAccepted represents a workflow update being accepted by a worker.
type WorkflowUpdateAccepted struct {
	Attributes *historyv1.WorkflowExecutionUpdateAcceptedEventAttributes
	Identity   *umpire.Identity
}

func (e *WorkflowUpdateAccepted) Name() string {
	return "WorkflowUpdateAccepted"
}

func (e *WorkflowUpdateAccepted) TargetEntity() *umpire.Identity {
	return e.Identity
}
