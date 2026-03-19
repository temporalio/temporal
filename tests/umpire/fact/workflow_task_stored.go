package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowTaskStored represents a workflow task being stored to persistence.
type WorkflowTaskStored struct {
	TaskQueue  string
	Identity   *umpire.Identity
	WorkflowID string
	RunID      string
}

func (e *WorkflowTaskStored) Name() string {
	return telemetry.EventWorkflowTaskStored
}

func (e *WorkflowTaskStored) TargetEntity() *umpire.Identity {
	return e.Identity
}

func (e *WorkflowTaskStored) ImportSpanEvent(attrs attribute.Set) bool {
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
	e.Identity = &umpire.Identity{EntityID: wtID, ParentID: &tqID}
	return true
}
