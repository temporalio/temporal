package fact

import (
	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowStarted represents a workflow being started.
type WorkflowStarted struct {
	Request    *historyservice.StartWorkflowExecutionRequest
	EntityPath *umpire.EntityPath
}

func (e *WorkflowStarted) Name() string {
	return "WorkflowStarted"
}

func (e *WorkflowStarted) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowStarted) ImportRequest(request any) bool {
	req, ok := request.(*historyservice.StartWorkflowExecutionRequest)
	if !ok || req == nil || req.GetStartRequest().GetWorkflowId() == "" {
		return false
	}
	e.Request = req
	wfID := umpire.NewEntityID(WorkflowType, req.GetStartRequest().GetWorkflowId())
	e.EntityPath = &umpire.EntityPath{EntityID: wfID}
	return true
}
