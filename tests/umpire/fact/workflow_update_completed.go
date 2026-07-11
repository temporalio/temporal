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
	EntityPath *umpire.EntityPath
}

func (e *WorkflowUpdateCompleted) Name() string {
	return telemetry.EventWorkflowUpdateCompleted
}

func (e *WorkflowUpdateCompleted) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowUpdateCompleted) IsSuccess() bool {
	return e.Success
}

func (e *WorkflowUpdateCompleted) ImportSpanEvent(attrs attribute.Set) bool {
	e.UpdateID, e.WorkflowID, e.EntityPath = importUpdateSpanEvent(attrs)
	if v, ok := attrs.Value(telemetry.AttrUpdateOutcome); ok {
		e.Success = v.AsString() == telemetry.UpdateOutcomeSuccess
	}
	return e.UpdateID != ""
}
