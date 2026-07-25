package model

import (
	"context"
	"fmt"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

const NexusOperationType = fact.NexusOperationType

var _ umpire.Entity = (*NexusOperation)(nil)
var _ umpire.Lifecycled = (*NexusOperation)(nil)

// NexusOperation mirrors the Nexus-operation HSM
// (components/nexusoperations/statemachine.go) as a Lifecycled entity. Its ID is
// "<callerWorkflowID>:<scheduledEventID>" and it is rooted under the caller
// Workflow (see UMPIRE_NEXUS.md).
type NexusOperation struct {
	ScheduledEventID string
	WorkflowID       string
	Outcome          string // set on a terminal transition, from the span's nexus.outcome
	FSM              *umpire.Lifecycle
}

func NewNexusOperation() *NexusOperation {
	op := &NexusOperation{}
	op.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: NexusUnspecified,
		Transitions: []umpire.Transition{
			// schedule fires on init and again on each retry out of backing_off.
			{Event: NexusSchedule, From: []string{NexusUnspecified, NexusBackingOff}, To: NexusScheduled},
			// attempt_failed: a retryable attempt failure sends it into backoff.
			{Event: NexusAttemptFailed, From: []string{NexusScheduled}, To: NexusBackingOff},
			// start: the async handler acknowledged (sync completion skips this).
			{Event: NexusStart, From: []string{NexusScheduled, NexusBackingOff}, To: NexusStarted},
			// Terminal transitions may fire from scheduled/backing_off/started;
			// "started precedes succeeded" is NOT an invariant (sync completes direct).
			{Event: NexusSucceed, From: []string{NexusScheduled, NexusBackingOff, NexusStarted}, To: NexusSucceeded},
			{Event: NexusFail, From: []string{NexusScheduled, NexusBackingOff, NexusStarted}, To: NexusFailed},
			{Event: NexusCancel, From: []string{NexusScheduled, NexusBackingOff, NexusStarted}, To: NexusCanceled},
			{Event: NexusTimeout, From: []string{NexusScheduled, NexusBackingOff, NexusStarted}, To: NexusTimedOut},
		},
		// A scheduled/backing_off/started operation must eventually settle;
		// terminal states derive automatically.
		MustProgress: []string{NexusScheduled, NexusBackingOff, NexusStarted},
	})
	return op
}

func (op *NexusOperation) Type() umpire.EntityType { return NexusOperationType }

// Lifecycle exposes the operation's state machine to generic lifecycle rules.
func (op *NexusOperation) Lifecycle() *umpire.Lifecycle { return op.FSM }

// The *At accessors are derived from the lifecycle's per-state entry times, so
// "state reached ⇔ timestamp set" holds by construction.
func (op *NexusOperation) ScheduledAt() time.Time { t, _ := op.FSM.EnteredAt(NexusScheduled); return t }
func (op *NexusOperation) StartedAt() time.Time   { t, _ := op.FSM.EnteredAt(NexusStarted); return t }

// SettledAt returns when the operation reached a terminal state, and whether it has.
func (op *NexusOperation) SettledAt() (time.Time, bool) {
	for _, s := range []string{NexusSucceeded, NexusFailed, NexusCanceled, NexusTimedOut} {
		if t, ok := op.FSM.EnteredAt(s); ok {
			return t, true
		}
	}
	return time.Time{}, false
}

func (op *NexusOperation) OnFact(ctx context.Context, ident *umpire.EntityPath, events iter.Seq[umpire.Fact]) error {
	if op.WorkflowID == "" && ident != nil {
		if parent := ident.Parent(); parent != nil && parent.EntityID.Type == WorkflowType {
			op.WorkflowID = parent.EntityID.ID
		}
	}

	for ev := range events {
		switch e := ev.(type) {
		case *fact.NexusOperationScheduled:
			op.capture(e.ScheduledEventID, e.WorkflowID)
			op.FSM.Fire(ctx, NexusSchedule)
		case *fact.NexusOperationAttemptFailed:
			op.FSM.Fire(ctx, NexusAttemptFailed)
		case *fact.NexusOperationStarted:
			op.FSM.Fire(ctx, NexusStart)
		case *fact.NexusOperationSucceeded:
			op.capture(e.ScheduledEventID, e.WorkflowID)
			if op.FSM.Fire(ctx, NexusSucceed) {
				op.Outcome = e.Outcome
			}
		case *fact.NexusOperationFailed:
			op.capture(e.ScheduledEventID, e.WorkflowID)
			if op.FSM.Fire(ctx, NexusFail) {
				op.Outcome = e.Outcome
			}
		case *fact.NexusOperationCanceled:
			op.capture(e.ScheduledEventID, e.WorkflowID)
			if op.FSM.Fire(ctx, NexusCancel) {
				op.Outcome = e.Outcome
			}
		case *fact.NexusOperationTimedOut:
			op.capture(e.ScheduledEventID, e.WorkflowID)
			if op.FSM.Fire(ctx, NexusTimeout) {
				op.Outcome = e.Outcome
			}
		case *fact.ChasmTransition:
			// A real CHASM operation, observed via the generic chasm.transition
			// telemetry. Its component path is the operation's identity.
			op.capture(e.ComponentPath, e.WorkflowID)
			if event := nexusEventForStatus(e.Destination); event != "" {
				if op.FSM.Fire(ctx, event) && op.FSM.IsTerminal() {
					op.Outcome = e.Destination
				}
			}
		}
	}
	return nil
}

// nexusEventForStatus maps a CHASM OperationStatus destination to the FSM event
// that reaches the corresponding model state. The chasm.transition telemetry
// stringifies the status with fmt %v, which for OperationStatus is its custom
// stringer (e.g. "Scheduled"); the proto enum-name form ("OPERATION_STATUS_...")
// is accepted too so the mapping survives a stringer change. Unknown statuses
// yield "" (ignored).
func nexusEventForStatus(destination string) string {
	switch destination {
	case "Scheduled", "OPERATION_STATUS_SCHEDULED":
		return NexusSchedule
	case "BackingOff", "OPERATION_STATUS_BACKING_OFF":
		return NexusAttemptFailed
	case "Started", "OPERATION_STATUS_STARTED":
		return NexusStart
	case "Succeeded", "OPERATION_STATUS_SUCCEEDED":
		return NexusSucceed
	case "Failed", "OPERATION_STATUS_FAILED":
		return NexusFail
	case "Canceled", "OPERATION_STATUS_CANCELED":
		return NexusCancel
	case "TimedOut", "OPERATION_STATUS_TIMED_OUT":
		return NexusTimeout
	default:
		return ""
	}
}

func (op *NexusOperation) capture(scheduledEventID, workflowID string) {
	if op.ScheduledEventID == "" {
		op.ScheduledEventID = scheduledEventID
	}
	if op.WorkflowID == "" {
		op.WorkflowID = workflowID
	}
}

func (op *NexusOperation) String() string {
	return fmt.Sprintf("NexusOperation{workflowID=%s, scheduledEventID=%s, state=%s}",
		op.WorkflowID, op.ScheduledEventID, op.FSM.Current())
}

// Lifecycle states and events for NexusOperation (aliased to string; see Workflow).
type (
	NexusState = string
	NexusEvent = string
)

const (
	NexusUnspecified NexusState = "unspecified"
	NexusScheduled   NexusState = "scheduled"
	NexusBackingOff  NexusState = "backing_off"
	NexusStarted     NexusState = "started"
	NexusSucceeded   NexusState = "succeeded"
	NexusFailed      NexusState = "failed"
	NexusCanceled    NexusState = "canceled"
	NexusTimedOut    NexusState = "timed_out"

	NexusSchedule      NexusEvent = "schedule"
	NexusAttemptFailed NexusEvent = "attempt_failed"
	NexusStart         NexusEvent = "start"
	NexusSucceed       NexusEvent = "succeed"
	NexusFail          NexusEvent = "fail"
	NexusCancel        NexusEvent = "cancel"
	NexusTimeout       NexusEvent = "timeout"
)
