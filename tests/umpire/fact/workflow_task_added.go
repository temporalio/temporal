package fact

import (
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowTaskAdded represents a workflow task being added to matching.
type WorkflowTaskAdded struct {
	Request  *matchingservice.AddWorkflowTaskRequest
	Identity *umpire.Identity
}

func (e *WorkflowTaskAdded) Name() string {
	return "WorkflowTaskAdded"
}

func (e *WorkflowTaskAdded) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowTaskAdded) ImportRequest(request any) bool {
	req, ok := request.(*matchingservice.AddWorkflowTaskRequest)
	if !ok || req == nil || req.GetTaskQueue().GetName() == "" {
		return false
	}
	e.Request = req
	tqName := req.GetTaskQueue().GetName()
	wfID := req.GetExecution().GetWorkflowId()
	runID := req.GetExecution().GetRunId()
	if wfID != "" && runID != "" {
		wtID := umpire.NewEntityID(WorkflowTaskType, tqName+":"+wfID+":"+runID)
		tqID := umpire.NewEntityID(TaskQueueType, tqName)
		e.Identity = &umpire.Identity{EntityID: wtID, ParentID: &tqID}
	}
	return true
}
