package fact

import (
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// ActivityTaskPolled represents an activity task being polled.
type ActivityTaskPolled struct {
	Request      *matchingservice.PollActivityTaskQueueRequest
	Identity     *umpire.Identity
	TaskReturned bool
}

func (e *ActivityTaskPolled) Name() string {
	return "ActivityTaskPolled"
}

func (e *ActivityTaskPolled) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *ActivityTaskPolled) ImportRequest(request any) bool {
	req, ok := request.(*matchingservice.PollActivityTaskQueueRequest)
	if !ok || req == nil || req.GetPollRequest().GetTaskQueue().GetName() == "" {
		return false
	}
	e.Request = req
	tqID := umpire.NewEntityID(TaskQueueType, req.GetPollRequest().GetTaskQueue().GetName())
	e.Identity = &umpire.Identity{EntityID: tqID}
	return true
}
