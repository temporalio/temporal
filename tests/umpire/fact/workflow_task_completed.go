package fact

import (
	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowTaskCompleted represents a workflow task completion response.
type WorkflowTaskCompleted struct {
	Request  *historyservice.RespondWorkflowTaskCompletedRequest
	Identity *umpire.Identity
}

func (e *WorkflowTaskCompleted) Name() string {
	return "WorkflowTaskCompleted"
}

func (e *WorkflowTaskCompleted) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowTaskCompleted) ImportRequest(request any) bool {
	req, ok := request.(*historyservice.RespondWorkflowTaskCompletedRequest)
	if !ok || req == nil || req.GetCompleteRequest() == nil {
		return false
	}
	e.Request = req
	return true
}
