package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowUpdateCompleted represents a workflow update being completed.
type WorkflowUpdateCompleted struct {
	UpdateID   string
	WorkflowID string
	Success    bool
	Identity   *umpire.Identity
}

func (e *WorkflowUpdateCompleted) Name() string {
	return telemetry.EventWorkflowUpdateCompleted
}

func (e *WorkflowUpdateCompleted) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowUpdateCompleted) IsSuccess() bool {
	return e.Success
}

func (e *WorkflowUpdateCompleted) ImportSpanEvent(attrs attribute.Set) bool {
	e.UpdateID, e.WorkflowID, e.Identity = importUpdateSpanEvent(attrs)
	if v, ok := attrs.Value(telemetry.AttrUpdateOutcome); ok {
		e.Success = v.AsString() == telemetry.UpdateOutcomeSuccess
	}
	return e.UpdateID != ""
}
