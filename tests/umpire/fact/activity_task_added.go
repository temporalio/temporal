package fact

import (
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// ActivityTaskAdded represents an activity task being added to matching.
type ActivityTaskAdded struct {
	Request  *matchingservice.AddActivityTaskRequest
	Identity *umpire.Identity
}

func (e *ActivityTaskAdded) Name() string {
	return "ActivityTaskAdded"
}

func (e *ActivityTaskAdded) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *ActivityTaskAdded) ImportRequest(request any) bool {
	req, ok := request.(*matchingservice.AddActivityTaskRequest)
	if !ok || req == nil || req.GetTaskQueue().GetName() == "" {
		return false
	}
	e.Request = req
	tqName := req.GetTaskQueue().GetName()
	wfID := req.GetExecution().GetWorkflowId()
	runID := req.GetExecution().GetRunId()
	if wfID != "" && runID != "" {
		atID := umpire.NewEntityID(ActivityTaskType, tqName+":"+wfID+":"+runID)
		tqID := umpire.NewEntityID(TaskQueueType, tqName)
		e.Identity = &umpire.Identity{EntityID: atID, ParentID: &tqID}
	}
	return true
}
