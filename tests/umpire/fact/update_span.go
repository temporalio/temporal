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
func importUpdateSpanEvent(attrs attribute.Set) (updateID, workflowID string, path *umpire.EntityPath) {
	if v, ok := attrs.Value(telemetry.AttrUpdateID); ok {
		updateID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		workflowID = v.AsString()
	}
	var namespaceID string
	if v, ok := attrs.Value(telemetry.AttrNamespaceID); ok {
		namespaceID = v.AsString()
	}
	if updateID == "" {
		return "", "", nil
	}
	uid := umpire.NewEntityID(WorkflowUpdateType, updateID)
	var parents []umpire.EntityID
	if workflowID != "" {
		parents = append(parents, umpire.NewEntityID(WorkflowType, workflowID))
	}
	return updateID, workflowID, nsPath(namespaceID, uid, parents...)
}
