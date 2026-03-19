package fact

import (
	historyv1 "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateAdmitted represents a workflow update being admitted.
type WorkflowUpdateAdmitted struct {
	Attributes *historyv1.WorkflowExecutionUpdateAdmittedEventAttributes
	Identity   *umpire.Identity
}

func (e *WorkflowUpdateAdmitted) Name() string {
	return "WorkflowUpdateAdmitted"
}

func (e *WorkflowUpdateAdmitted) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowUpdateAdmitted) UpdateID() string {
	return e.Attributes.GetRequest().GetMeta().GetUpdateId()
}

func (e *WorkflowUpdateAdmitted) HandlerName() string {
	return e.Attributes.GetRequest().GetInput().GetName()
}
