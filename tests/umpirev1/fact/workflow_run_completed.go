package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowRunCompleted represents one *execution* (WorkflowID + RunID) closing via a
// CompleteWorkflowExecution command. It decodes from the same span event as
// WorkflowExecutionCompleted but targets the run-precise WorkflowRun entity (keyed by RunID under
// its Workflow), so multiple runs of one WorkflowID (continue-as-new / retry / reset) are modeled
// distinctly. The decoder emits both facts for the event; the Workflow (by id) aggregate is
// unchanged.
type WorkflowRunCompleted struct {
	WorkflowID  string
	RunID       string
	NamespaceID string
	EntityPath  *umpire.EntityPath
}

func (e *WorkflowRunCompleted) Name() string {
	return "WorkflowRunCompleted" // the fact's identity (must match the struct name); the OTEL event
	// it decodes from is EventWorkflowExecutionCompleted, wired in the decoder (registerSpanFactAs).
}

func (e *WorkflowRunCompleted) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowRunCompleted) ImportSpanEvent(attrs attribute.Set) bool {
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
		return false // a run entity needs both ids
	}
	// The run is keyed by RunID, nested under its Workflow (auto-created as the parent).
	self := umpire.NewEntityID(WorkflowRunType, e.RunID)
	parent := umpire.NewEntityID(WorkflowType, e.WorkflowID)
	e.EntityPath = nsPath(e.NamespaceID, self, parent)
	return true
}
