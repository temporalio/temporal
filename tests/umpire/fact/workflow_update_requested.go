package fact

import (
	workflowservice "go.temporal.io/api/workflowservice/v1"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateRequested represents a workflow update request.
type WorkflowUpdateRequested struct {
	Request  *historyservice.UpdateWorkflowExecutionRequest
	Identity *umpire.Identity
}

func (e *WorkflowUpdateRequested) Name() string {
	return "WorkflowUpdateRequested"
}

func (e *WorkflowUpdateRequested) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowUpdateRequested) ImportRequest(request any) bool {
	switch req := request.(type) {
	case *historyservice.UpdateWorkflowExecutionRequest:
		if req.GetRequest().GetRequest().GetMeta().GetUpdateId() == "" {
			return false
		}
		e.Request = req
	case *workflowservice.UpdateWorkflowExecutionRequest:
		if req.GetRequest().GetMeta().GetUpdateId() == "" {
			return false
		}
		e.Request = &historyservice.UpdateWorkflowExecutionRequest{
			NamespaceId: req.Namespace,
			Request:     req,
		}
	default:
		return false
	}
	updateID := umpire.NewEntityID(WorkflowUpdateType, e.UpdateID())
	var parentID *umpire.EntityID
	if wfID := e.WorkflowID(); wfID != "" {
		id := umpire.NewEntityID(WorkflowType, wfID)
		parentID = &id
	}
	e.Identity = &umpire.Identity{EntityID: updateID, ParentID: parentID}
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
