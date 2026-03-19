package fact

import (
	historyv1 "go.temporal.io/api/history/v1"
	updatev1 "go.temporal.io/api/update/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateCompleted represents a workflow update being completed.
type WorkflowUpdateCompleted struct {
	Attributes *historyv1.WorkflowExecutionUpdateCompletedEventAttributes
	Identity   *umpire.Identity
}

func (e *WorkflowUpdateCompleted) Name() string {
	return "WorkflowUpdateCompleted"
}

func (e *WorkflowUpdateCompleted) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowUpdateCompleted) UpdateID() string {
	return e.Attributes.GetMeta().GetUpdateId()
}

func (e *WorkflowUpdateCompleted) IsSuccess() bool {
	_, ok := e.Attributes.GetOutcome().GetValue().(*updatev1.Outcome_Success)
	return ok
}
