package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// WorkflowRunStarted represents one execution (WorkflowID + RunID) starting. It decodes from the
// EventWorkflowExecutionStarted span event, carrying the run's lineage (FirstRunID = chain root,
// PreviousRunID = immediate predecessor for continue-as-new / reset / retry; empty for a first
// run). It targets the run-precise WorkflowRun entity, so a run is observed — with its lineage — at
// start, not only at completion. See UMPIRE_IDENTITY.md.
type WorkflowRunStarted struct {
	WorkflowID    string
	RunID         string
	FirstRunID    string
	PreviousRunID string
	Initiator     string // how this run was created (RunInitiator*); the typed edge from PreviousRunID
	NamespaceID   string
	EntityPath    *umpire.EntityPath
}

func (e *WorkflowRunStarted) Name() string {
	return "WorkflowRunStarted" // identity; decodes from EventWorkflowExecutionStarted (registerSpanFactAs)
}

func (e *WorkflowRunStarted) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func (e *WorkflowRunStarted) ImportSpanEvent(attrs attribute.Set) bool {
	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		e.WorkflowID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrRunID); ok {
		e.RunID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrFirstRunID); ok {
		e.FirstRunID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrPreviousRunID); ok {
		e.PreviousRunID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrRunInitiator); ok {
		e.Initiator = v.AsString()
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
