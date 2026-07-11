package fact

import (
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowTaskPolled represents a workflow task being polled.
type WorkflowTaskPolled struct {
	Request      *matchingservice.PollWorkflowTaskQueueRequest
	EntityPath   *umpire.EntityPath
	TaskReturned bool
}

func (e *WorkflowTaskPolled) Name() string {
	return "WorkflowTaskPolled"
}

func (e *WorkflowTaskPolled) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowTaskPolled) ImportRequest(request any) bool {
	req, ok := request.(*matchingservice.PollWorkflowTaskQueueRequest)
	if !ok || req == nil || req.GetPollRequest().GetTaskQueue().GetName() == "" {
		return false
	}
	e.Request = req
	tqID := umpire.NewEntityID(TaskQueueType, req.GetPollRequest().GetTaskQueue().GetName())
	e.EntityPath = &umpire.EntityPath{EntityID: tqID}
	return true
}
