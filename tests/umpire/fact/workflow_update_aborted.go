package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateAborted represents a workflow update being aborted by the
// history service (e.g., workflow closed, registry cleared).
type WorkflowUpdateAborted struct {
	UpdateID    string
	WorkflowID  string
	AbortReason string
	Identity    *umpire.Identity
}

func (e *WorkflowUpdateAborted) Name() string {
	return telemetry.EventWorkflowUpdateAborted
}

func (e *WorkflowUpdateAborted) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowUpdateAborted) ImportSpanEvent(attrs attribute.Set) bool {
	if v, ok := attrs.Value(telemetry.AttrUpdateID); ok {
		e.UpdateID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		e.WorkflowID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrAbortReason); ok {
		e.AbortReason = v.AsString()
	}
	if e.UpdateID == "" {
		return false
	}
	updateID := umpire.NewEntityID(WorkflowUpdateType, e.UpdateID)
	var parentID *umpire.EntityID
	if e.WorkflowID != "" {
		id := umpire.NewEntityID(WorkflowType, e.WorkflowID)
		parentID = &id
	}
	e.Identity = &umpire.Identity{EntityID: updateID, ParentID: parentID}
	return true
}
