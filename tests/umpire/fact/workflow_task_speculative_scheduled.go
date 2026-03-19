package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// SpeculativeWorkflowTaskScheduled is the event produced when a speculative
// workflow task is dispatched (e.g., during UpdateWorkflowExecution).
type SpeculativeWorkflowTaskScheduled struct {
	WorkflowID  string
	RunID       string
	NamespaceID string
	TaskQueue   string
	Identity    *umpire.Identity
}

func (e *SpeculativeWorkflowTaskScheduled) Name() string {
	return telemetry.EventSpeculativeWorkflowTaskScheduled
}

func (e *SpeculativeWorkflowTaskScheduled) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *SpeculativeWorkflowTaskScheduled) ImportSpanEvent(attrs attribute.Set) bool {
	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		e.WorkflowID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrRunID); ok {
		e.RunID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrNamespaceID); ok {
		e.NamespaceID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrTaskQueue); ok {
		e.TaskQueue = v.AsString()
	}
	if e.WorkflowID == "" {
		return false
	}
	wfID := umpire.NewEntityID(WorkflowType, e.WorkflowID)
	wtID := umpire.NewEntityID(WorkflowTaskType, e.TaskQueue+":"+e.WorkflowID+":"+e.RunID)
	e.Identity = &umpire.Identity{
		EntityID: wtID,
		ParentID: &wfID,
	}
	return true
}
