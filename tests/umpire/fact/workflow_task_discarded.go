package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowTaskDiscarded represents a workflow task being discarded by matching
// (e.g., expired in memory before being polled).
type WorkflowTaskDiscarded struct {
	WorkflowID string
	RunID      string
	TaskQueue  string
	EntityPath *umpire.EntityPath
}

func (e *WorkflowTaskDiscarded) Name() string {
	return telemetry.EventWorkflowTaskDiscarded
}

func (e *WorkflowTaskDiscarded) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowTaskDiscarded) ImportSpanEvent(attrs attribute.Set) bool {
	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		e.WorkflowID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrRunID); ok {
		e.RunID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrTaskQueue); ok {
		e.TaskQueue = v.AsString()
	}
	if e.WorkflowID == "" {
		return false
	}
	wtID := umpire.NewEntityID(WorkflowTaskType, e.TaskQueue+":"+e.WorkflowID+":"+e.RunID)
	tqID := umpire.NewEntityID(TaskQueueType, e.TaskQueue)
	e.EntityPath = &umpire.EntityPath{EntityID: wtID, ParentID: &tqID}
	return true
}
