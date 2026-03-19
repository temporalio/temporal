package fact

import (
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowTaskPolled represents a workflow task being polled.
type WorkflowTaskPolled struct {
	Request      *matchingservice.PollWorkflowTaskQueueRequest
	Identity     *umpire.Identity
	TaskReturned bool
}

func (e *WorkflowTaskPolled) Name() string {
	return "WorkflowTaskPolled"
}

func (e *WorkflowTaskPolled) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowTaskPolled) ImportRequest(request any) bool {
	req, ok := request.(*matchingservice.PollWorkflowTaskQueueRequest)
	if !ok || req == nil || req.GetPollRequest().GetTaskQueue().GetName() == "" {
		return false
	}
	e.Request = req
	tqID := umpire.NewEntityID(TaskQueueType, req.GetPollRequest().GetTaskQueue().GetName())
	e.Identity = &umpire.Identity{EntityID: tqID}
	return true
}
