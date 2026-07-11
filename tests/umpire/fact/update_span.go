package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// importUpdateSpanEvent extracts the update ID, workflow ID, and the routed
// entity identity (WorkflowUpdate keyed under its parent Workflow) shared by
// all update-lifecycle span facts. Returns an empty updateID when the event
// carries no update ID, in which case the fact should be discarded.
func importUpdateSpanEvent(attrs attribute.Set) (updateID, workflowID string, ident *umpire.EntityPath) {
	if v, ok := attrs.Value(telemetry.AttrUpdateID); ok {
		updateID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		workflowID = v.AsString()
	}
	if updateID == "" {
		return "", "", nil
	}
	uid := umpire.NewEntityID(WorkflowUpdateType, updateID)
	var parentID *umpire.EntityID
	if workflowID != "" {
		id := umpire.NewEntityID(WorkflowType, workflowID)
		parentID = &id
	}
	return updateID, workflowID, &umpire.EntityPath{EntityID: uid, ParentID: parentID}
}
