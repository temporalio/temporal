package fact

import (
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowTaskAdded represents a workflow task being added to matching.
type WorkflowTaskAdded struct {
	Request    *matchingservice.AddWorkflowTaskRequest
	EntityPath *umpire.EntityPath
}

func (e *WorkflowTaskAdded) Name() string {
	return "WorkflowTaskAdded"
}

func (e *WorkflowTaskAdded) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
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
		e.EntityPath = nsPath(req.GetNamespaceId(), wtID, tqID)
	}
	return true
}
