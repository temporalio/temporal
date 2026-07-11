package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateRejected represents a workflow update being rejected by a worker.
type WorkflowUpdateRejected struct {
	UpdateID   string
	WorkflowID string
	EntityPath *umpire.EntityPath
}

func (e *WorkflowUpdateRejected) Name() string {
	return telemetry.EventWorkflowUpdateRejected
}

func (e *WorkflowUpdateRejected) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowUpdateRejected) ImportSpanEvent(attrs attribute.Set) bool {
	e.UpdateID, e.WorkflowID, e.EntityPath = importUpdateSpanEvent(attrs)
	return e.UpdateID != ""
}
