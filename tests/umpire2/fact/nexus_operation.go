package fact

import (
	"go.opentelemetry.io/otel/attribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/umpire"
)

// importNexusSpanEvent extracts the scheduled-event ID, caller workflow ID, and the
// routed entity identity (NexusOperation keyed under its parent Workflow) shared by
// all Nexus-operation lifecycle span facts. Returns an empty scheduledEventID when
// the event carries no scheduled-event ID, in which case the fact is discarded.
//
// Identity mirrors the operation HSM node: an operation is the node whose ID is the
// scheduled-event ID, under its caller workflow run — so the entity ID is
// "<workflowID>:<scheduledEventID>" (see UMPIRE.md).
func importNexusSpanEvent(attrs attribute.Set) (scheduledEventID, workflowID string, path *umpire.EntityPath) {
	if v, ok := attrs.Value(telemetry.AttrNexusScheduledEventID); ok {
		scheduledEventID = v.AsString()
	}
	if v, ok := attrs.Value(telemetry.AttrWorkflowID); ok {
		workflowID = v.AsString()
	}
	var namespaceID string
	if v, ok := attrs.Value(telemetry.AttrNamespaceID); ok {
		namespaceID = v.AsString()
	}
	if scheduledEventID == "" {
		return "", "", nil
	}
	self := umpire.NewEntityID(NexusOperationType, workflowID+":"+scheduledEventID)
	var parents []umpire.EntityID
	if workflowID != "" {
		parents = append(parents, umpire.NewEntityID(WorkflowType, workflowID))
	}
	return scheduledEventID, workflowID, nsPath(namespaceID, self, parents...)
}

// nexusOperationFact is the shared payload of every Nexus-operation span fact: the
// operation identity plus (for terminal events) the outcome. Each concrete fact
// embeds it and supplies only its own Name(), so the fact type ⇔ span-event name
// mapping stays one-to-one (the type-switch discriminator entities rely on).
type nexusOperationFact struct {
	EventTimeCarrier
	ScheduledEventID string
	WorkflowID       string
	Outcome          string // set for terminal events from AttrNexusOutcome; "" otherwise
	EntityPath       *umpire.EntityPath
}

func (e *nexusOperationFact) TargetEntity() *umpire.EntityPath { return e.EntityPath }

func (e *nexusOperationFact) importSelf(attrs attribute.Set) bool {
	e.ScheduledEventID, e.WorkflowID, e.EntityPath = importNexusSpanEvent(attrs)
	if v, ok := attrs.Value(telemetry.AttrNexusOutcome); ok {
		e.Outcome = v.AsString()
	}
	return e.ScheduledEventID != ""
}

// NexusOperationScheduled: UNSPECIFIED/BACKING_OFF -> SCHEDULED.
type NexusOperationScheduled struct{ nexusOperationFact }

func (e *NexusOperationScheduled) Name() string { return telemetry.EventNexusOperationScheduled }
func (e *NexusOperationScheduled) ImportSpanEvent(attrs attribute.Set) bool {
	return e.importSelf(attrs)
}

// NexusOperationAttemptFailed: SCHEDULED -> BACKING_OFF (retryable attempt failure).
type NexusOperationAttemptFailed struct{ nexusOperationFact }

func (e *NexusOperationAttemptFailed) Name() string {
	return telemetry.EventNexusOperationAttemptFailed
}
func (e *NexusOperationAttemptFailed) ImportSpanEvent(attrs attribute.Set) bool {
	return e.importSelf(attrs)
}

// NexusOperationStarted: SCHEDULED/BACKING_OFF -> STARTED (async handler ack).
type NexusOperationStarted struct{ nexusOperationFact }

func (e *NexusOperationStarted) Name() string { return telemetry.EventNexusOperationStarted }
func (e *NexusOperationStarted) ImportSpanEvent(attrs attribute.Set) bool {
	return e.importSelf(attrs)
}

// NexusOperationSucceeded: -> SUCCEEDED (terminal; sync completion may skip STARTED).
type NexusOperationSucceeded struct{ nexusOperationFact }

func (e *NexusOperationSucceeded) Name() string { return telemetry.EventNexusOperationSucceeded }
func (e *NexusOperationSucceeded) ImportSpanEvent(attrs attribute.Set) bool {
	return e.importSelf(attrs)
}

// NexusOperationFailed: -> FAILED (terminal).
type NexusOperationFailed struct{ nexusOperationFact }

func (e *NexusOperationFailed) Name() string { return telemetry.EventNexusOperationFailed }
func (e *NexusOperationFailed) ImportSpanEvent(attrs attribute.Set) bool {
	return e.importSelf(attrs)
}

// NexusOperationCanceled: -> CANCELED (terminal).
type NexusOperationCanceled struct{ nexusOperationFact }

func (e *NexusOperationCanceled) Name() string { return telemetry.EventNexusOperationCanceled }
func (e *NexusOperationCanceled) ImportSpanEvent(attrs attribute.Set) bool {
	return e.importSelf(attrs)
}

// NexusOperationTimedOut: -> TIMED_OUT (terminal).
type NexusOperationTimedOut struct{ nexusOperationFact }

func (e *NexusOperationTimedOut) Name() string { return telemetry.EventNexusOperationTimedOut }
func (e *NexusOperationTimedOut) ImportSpanEvent(attrs attribute.Set) bool {
	return e.importSelf(attrs)
}
