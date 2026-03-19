package fact

import (
	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowStarted represents a workflow being started.
type WorkflowStarted struct {
	Request  *historyservice.StartWorkflowExecutionRequest
	Identity *umpire.Identity
}

func (e *WorkflowStarted) Name() string {
	return "WorkflowStarted"
}

func (e *WorkflowStarted) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowStarted) ImportRequest(request any) bool {
	req, ok := request.(*historyservice.StartWorkflowExecutionRequest)
	if !ok || req == nil || req.GetStartRequest().GetWorkflowId() == "" {
		return false
	}
	e.Request = req
	wfID := umpire.NewEntityID(WorkflowType, req.GetStartRequest().GetWorkflowId())
	e.Identity = &umpire.Identity{EntityID: wfID}
	return true
}
