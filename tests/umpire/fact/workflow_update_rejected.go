package fact

import (
	historyv1 "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateRejected represents a workflow update being rejected.
type WorkflowUpdateRejected struct {
	Attributes *historyv1.WorkflowExecutionUpdateRejectedEventAttributes
	Identity   *umpire.Identity
}

func (e *WorkflowUpdateRejected) Name() string {
	return "WorkflowUpdateRejected"
}

func (e *WorkflowUpdateRejected) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowUpdateRejected) UpdateID() string {
	return e.Attributes.GetRejectedRequest().GetMeta().GetUpdateId()
}
