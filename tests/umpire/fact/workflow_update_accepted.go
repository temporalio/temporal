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
	Identity   *umpire.Identity
}

func (e *WorkflowUpdateAccepted) Name() string {
	return telemetry.EventWorkflowUpdateAccepted
}

func (e *WorkflowUpdateAccepted) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowUpdateAccepted) ImportSpanEvent(attrs attribute.Set) bool {
	e.UpdateID, e.WorkflowID, e.Identity = importUpdateSpanEvent(attrs)
	return e.UpdateID != ""
}
