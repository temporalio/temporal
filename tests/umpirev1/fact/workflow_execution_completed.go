package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowExecutionCompleted represents a workflow execution closing via a
// CompleteWorkflowExecution command.
type WorkflowExecutionCompleted struct {
	WorkflowID  string
	RunID       string
	NamespaceID string
	EntityPath  *umpire.EntityPath
}

func (e *WorkflowExecutionCompleted) Name() string {
	return telemetry.EventWorkflowExecutionCompleted
}

func (e *WorkflowExecutionCompleted) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowExecutionCompleted) ImportSpanEvent(attrs attribute.Set) bool {
	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		e.WorkflowID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrRunID); ok {
		e.RunID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrNamespaceID); ok {
		e.NamespaceID = v.AsString()
	}
	if e.WorkflowID == "" {
		return false
	}
	wfID := umpire.NewEntityID(WorkflowType, e.WorkflowID)
	e.EntityPath = nsPath(e.NamespaceID, wfID)
	return true
}
