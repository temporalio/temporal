package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateAccepted represents a workflow update being accepted by a worker.
type WorkflowUpdateAccepted struct {
	UpdateID   string
	WorkflowID string
	EntityPath *umpire.EntityPath
}

func (e *WorkflowUpdateAccepted) Name() string {
	return telemetry.EventWorkflowUpdateAccepted
}

func (e *WorkflowUpdateAccepted) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowUpdateAccepted) ImportSpanEvent(attrs attribute.Set) bool {
	e.UpdateID, e.WorkflowID, e.EntityPath = importUpdateSpanEvent(attrs)
	return e.UpdateID != ""
}
