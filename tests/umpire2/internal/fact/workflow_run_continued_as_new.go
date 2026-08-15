package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowRunContinuedAsNew represents a run closing via continue-as-new — the predecessor of a
// continued_as_new edge. It targets that run's WorkflowRun so it reaches a continued_as_new
// terminal rather than staying started (its successor start carries the matching edge label). See
// UMPIRE.md.
type WorkflowRunContinuedAsNew struct {
	WorkflowID  string
	RunID       string
	NamespaceID string
	EntityPath  *umpire.EntityPath
}

func (e *WorkflowRunContinuedAsNew) Name() string {
	return "WorkflowRunContinuedAsNew" // identity; decodes from EventWorkflowExecutionContinuedAsNew
}

func (e *WorkflowRunContinuedAsNew) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowRunContinuedAsNew) ImportSpanEvent(attrs attribute.Set) bool {
	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		e.WorkflowID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrRunID); ok {
		e.RunID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrNamespaceID); ok {
		e.NamespaceID = v.AsString()
	}
	if e.WorkflowID == "" || e.RunID == "" {
		return false
	}
	self := umpire.NewEntityID(WorkflowRunType, e.RunID)
	parent := umpire.NewEntityID(WorkflowType, e.WorkflowID)
	e.EntityPath = nsPath(e.NamespaceID, self, parent)
	return true
}
