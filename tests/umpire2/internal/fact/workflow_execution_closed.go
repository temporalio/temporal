package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

type workflowCloseFact struct {
	EventTimeCarrier
	NamespaceID    string
	WorkflowID     string
	RunID          string
	Outcome        string
	SuccessorRunID string
	EntityPath     *umpire.EntityPath
}

func (e *workflowCloseFact) TargetEntity() *umpire.EntityPath { return e.EntityPath }

func (e *workflowCloseFact) importSelf(attrs attribute.Set, runScoped bool) bool {
	if value, ok := attrs.Value(telemetry.AttrNamespaceID); ok {
		e.NamespaceID = value.AsString()
	}
	if value, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		e.WorkflowID = value.AsString()
	}
	if value, ok := attrs.Value(telemetry.AttrRunID); ok {
		e.RunID = value.AsString()
	}
	if value, ok := attrs.Value(telemetry.AttrWorkflowCloseOutcome); ok {
		e.Outcome = value.AsString()
	}
	if value, ok := attrs.Value(telemetry.AttrWorkflowSuccessorRunID); ok {
		e.SuccessorRunID = value.AsString()
	}
	if e.NamespaceID == "" || e.WorkflowID == "" || e.RunID == "" || !validWorkflowCloseOutcome(e.Outcome) {
		return false
	}
	if runScoped {
		e.EntityPath = nsPath(
			e.NamespaceID,
			umpire.NewEntityID(WorkflowRunType, e.RunID),
			umpire.NewEntityID(WorkflowType, e.WorkflowID),
		)
	} else {
		e.EntityPath = nsPath(e.NamespaceID, umpire.NewEntityID(WorkflowType, e.WorkflowID))
	}
	return true
}

func validWorkflowCloseOutcome(outcome string) bool {
	switch outcome {
	case telemetry.WorkflowCloseOutcomeCompleted,
		telemetry.WorkflowCloseOutcomeFailed,
		telemetry.WorkflowCloseOutcomeCanceled,
		telemetry.WorkflowCloseOutcomeTerminated,
		telemetry.WorkflowCloseOutcomeTimedOut,
		telemetry.WorkflowCloseOutcomeContinuedAsNew:
		return true
	default:
		return false
	}
}

// WorkflowExecutionClosed is the WorkflowID-chain view of a normalized run close.
type WorkflowExecutionClosed struct{ workflowCloseFact }

func (*WorkflowExecutionClosed) Name() string { return "WorkflowExecutionClosed" }
func (e *WorkflowExecutionClosed) ImportSpanEvent(attrs attribute.Set) bool {
	return e.importSelf(attrs, false)
}

// WorkflowRunClosed is the run-precise view of the same normalized close observation.
type WorkflowRunClosed struct{ workflowCloseFact }

func (*WorkflowRunClosed) Name() string { return "WorkflowRunClosed" }
func (e *WorkflowRunClosed) ImportSpanEvent(attrs attribute.Set) bool {
	return e.importSelf(attrs, true)
}
