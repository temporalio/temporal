package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowTerminated represents a workflow reaching a terminal state.
// Broadcast to all WorkflowTask entities so they can transition to a terminal state.
type WorkflowTerminated struct {
	WorkflowID string
	RunID      string
	TaskQueue  string
}

func (e *WorkflowTerminated) Name() string {
	return telemetry.EventWorkflowTerminated
}

func (e *WorkflowTerminated) TargetEntity() *umpire.Identity {
	return nil
}

func (e *WorkflowTerminated) BroadcastType() umpire.EntityType {
	return WorkflowTaskType
}

func (e *WorkflowTerminated) ImportSpanEvent(attrs attribute.Set) bool {
	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		e.WorkflowID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrRunID); ok {
		e.RunID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrTaskQueue); ok {
		e.TaskQueue = v.AsString()
	}
	return e.WorkflowID != ""
}
