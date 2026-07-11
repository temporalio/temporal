package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowTaskStored represents a workflow task being stored to persistence.
type WorkflowTaskStored struct {
	TaskQueue  string
	EntityPath *umpire.EntityPath
	WorkflowID string
	RunID      string
}

func (e *WorkflowTaskStored) Name() string {
	return telemetry.EventWorkflowTaskStored
}

func (e *WorkflowTaskStored) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
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
	e.EntityPath = &umpire.EntityPath{EntityID: wtID, ParentID: &tqID}
	return true
}
