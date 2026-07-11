package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateAdmitted represents a workflow update being admitted to the
// history update registry.
type WorkflowUpdateAdmitted struct {
	UpdateID   string
	WorkflowID string
	EntityPath *umpire.EntityPath
}

func (e *WorkflowUpdateAdmitted) Name() string {
	return telemetry.EventWorkflowUpdateAdmitted
}

func (e *WorkflowUpdateAdmitted) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowUpdateAdmitted) ImportSpanEvent(attrs attribute.Set) bool {
	e.UpdateID, e.WorkflowID, e.EntityPath = importUpdateSpanEvent(attrs)
	return e.UpdateID != ""
}
