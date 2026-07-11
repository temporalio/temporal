package fact

import (
	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateRequested represents a workflow update request.
type WorkflowUpdateRequested struct {
	Request    *historyservice.UpdateWorkflowExecutionRequest
	EntityPath *umpire.EntityPath
}

func (e *WorkflowUpdateRequested) Name() string {
	return "WorkflowUpdateRequested"
}

func (e *WorkflowUpdateRequested) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowUpdateRequested) ImportRequest(request any) bool {
	// Only the internal historyservice request carries the namespace ID (a UUID)
	// that roots the entity consistently with the update-lifecycle span facts.
	// The frontend workflowservice request carries only the namespace name, so
	// observing it would split the update into a second, differently-rooted entity.
	req, ok := request.(*historyservice.UpdateWorkflowExecutionRequest)
	if !ok || req.GetRequest().GetRequest().GetMeta().GetUpdateId() == "" {
		return false
	}
	e.Request = req
	updateID := umpire.NewEntityID(WorkflowUpdateType, e.UpdateID())
	var parents []umpire.EntityID
	if wfID := e.WorkflowID(); wfID != "" {
		parents = append(parents, umpire.NewEntityID(WorkflowType, wfID))
	}
	e.EntityPath = nsPath(e.Request.GetNamespaceId(), updateID, parents...)
	return true
}

func (e *WorkflowUpdateRequested) UpdateID() string {
	return e.Request.GetRequest().GetRequest().GetMeta().GetUpdateId()
}

func (e *WorkflowUpdateRequested) WorkflowID() string {
	return e.Request.GetRequest().GetWorkflowExecution().GetWorkflowId()
}

func (e *WorkflowUpdateRequested) HandlerName() string {
	return e.Request.GetRequest().GetRequest().GetInput().GetName()
}
