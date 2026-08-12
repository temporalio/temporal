package model

import (
	"context"
	"fmt"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
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
	Attempt          int    // observed retry attempt, from chasm.transition telemetry
	FSM              *umpire.Lifecycle
}

func NewNexusOperation() *NexusOperation {
	op := &NexusOperation{}
	// active are the in-flight states an operation must eventually settle out of.
	// timeout and terminate can fire from any of them (a timer, or an external RPC).
	active := []string{NexusScheduled, NexusBackingOff, NexusStarted}
	// settleable are the states from which a handler/attempt outcome settles the
	// operation: an attempt result (from scheduled) or an async completion (from started).
	// backing_off is excluded — the op leaves it via the backoff timer (backing_off
	// --schedule--> scheduled), and that reschedule is what emits the retry attempt, so no
	// attempt or completion ever runs while backing_off. Mirroring CHASM's From-sets here
	// (which include BACKING_OFF defensively) would over-approximate the machine.
	settleable := []string{NexusScheduled, NexusStarted}
	op.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: NexusUnspecified,
		// Each state carries its traits: the in-flight states MustProgress; the
		// terminals carry their modeled outcome. succeeded is a clean completion;
		// failed and timed_out are acceptable failure terminals (a fault reaching one
		// is degradation, not a bug). canceled and terminated are left untagged: both are
		// user-driven decisions, not a success or a failure of the operation. Terminal-ness
		// itself derives from the transition graph.
		States: umpire.States{
			NexusUnspecified: {},
			NexusScheduled:   {umpire.MustProgress},
			NexusBackingOff:  {umpire.MustProgress},
			NexusStarted:     {umpire.MustProgress},
			NexusSucceeded:   {umpire.Success},
			NexusFailed:      {umpire.Failure},
			NexusCanceled:    {}, // user-driven decision, not a failure
			NexusTimedOut:    {umpire.Failure},
			NexusTerminated:  {}, // user-driven forceful termination, not an operation failure
			// rejected: the start request was refused synchronously (invalid input, unknown
			// endpoint, …) — a modeled Failure terminal reached before the operation ever exists.
			NexusRejected: {umpire.Failure},
		},
		// Edge traits declare the drive-capability each edge needs: most are reachable
		// with ordinary API traffic (RPCDrive — a handler response or client call);
		// timing out can only be reached deterministically by firing the timer early,
		// so that edge needs Faults and is unreachable in an observe-only environment.
		Transitions: []umpire.Transition{
			// schedule fires on init and again on each retry out of backing_off.
			{
				Event:  NexusSchedule,
				From:   []string{NexusUnspecified, NexusBackingOff},
				To:     NexusScheduled,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
			// attempt_failed: a retryable attempt failure sends it into backoff.
			{
				Event:  NexusAttemptFailed,
				From:   []string{NexusScheduled},
				To:     NexusBackingOff,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
			// start: the async handler acknowledged. Fires from scheduled only — the attempt
			// that carries the ack runs from scheduled (sync completion skips this).
			{
				Event:  NexusStart,
				From:   []string{NexusScheduled},
				To:     NexusStarted,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
			// succeed/fail/cancel settle the operation from a running attempt (scheduled) or
			// an async completion (started); "started precedes succeeded" is NOT an invariant
			// (sync completes direct). They cannot fire from backing_off (see settleable).
			{
				Event:  NexusSucceed,
				From:   settleable,
				To:     NexusSucceeded,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
			{
				Event:  NexusFail,
				From:   settleable,
				To:     NexusFailed,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
			{
				Event:  NexusCancel,
				From:   settleable,
				To:     NexusCanceled,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
			// timeout fires from any active state when the schedule-to-close timer elapses —
			// including backing_off (the timer is independent of the retry cycle). It needs a
			// timer trigger (a fault/hook), not ordinary API traffic.
			{
				Event:  NexusTimeout,
				From:   active,
				To:     NexusTimedOut,
				Traits: umpire.Traits{umpire.Needs(umpire.Faults)},
			},
			// terminate is an external TerminateNexusOperationExecution RPC; it can force any
			// active operation to the terminated terminal — but only for a Standalone operation
			// (its own root execution). An Embedded (workflow-child) operation has no such RPC:
			// terminating the caller workflow does not cascade to it (the framework's Terminate
			// hook fires only on an execution's root component).
			{
				Event:  NexusTerminate,
				From:   active,
				To:     NexusTerminated,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive), umpire.RequiresHosting(umpire.Standalone)},
			},
			// reject: the start request was refused synchronously, before the operation was
			// created — it only ever fires from unspecified, and reaches the rejected terminal.
			{
				Event:  NexusReject,
				From:   []string{NexusUnspecified},
				To:     NexusRejected,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
		},
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
	for _, s := range []string{NexusSucceeded, NexusFailed, NexusCanceled, NexusTimedOut, NexusTerminated} {
		if t, ok := op.FSM.EnteredAt(s); ok {
			return t, true
		}
	}
	return time.Time{}, false
}

func (op *NexusOperation) OnFact(ctx context.Context, ident *umpire.EntityPath, facts iter.Seq[umpire.Fact]) error {
	if op.WorkflowID == "" && ident != nil {
		if parent := ident.Parent(); parent != nil && parent.EntityID.Type == WorkflowType {
			op.WorkflowID = parent.EntityID.ID
		}
	}

	for f := range facts {
		switch e := f.(type) {
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
		case *fact.NexusOperationRejected:
			// A synchronous rejection: no telemetry, so the operation's identity is its request id
			// (the field the Oracle keys on), and the outcome is the gRPC status class.
			op.capture("", e.RequestID)
			if op.FSM.Fire(ctx, NexusReject) {
				op.Outcome = e.Code
			}
		case *fact.ChasmTransition:
			// A real CHASM operation, observed via the generic chasm.transition
			// telemetry. Its stable request ID is the operation's identity (present
			// on every transition, so the scheduling one is observed too).
			op.capture(e.RequestID, e.WorkflowID)
			if e.Attempt > op.Attempt {
				op.Attempt = e.Attempt // attempt count is monotonic
			}
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
	case "Terminated", "OPERATION_STATUS_TERMINATED":
		return NexusTerminate
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

// Lifecycle states and facts for NexusOperation (aliased to string; see Workflow).
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
	NexusTerminated  NexusState = "terminated"
	NexusRejected    NexusState = "rejected"

	NexusSchedule      NexusEvent = "schedule"
	NexusAttemptFailed NexusEvent = "attempt_failed"
	NexusStart         NexusEvent = "start"
	NexusSucceed       NexusEvent = "succeed"
	NexusFail          NexusEvent = "fail"
	NexusCancel        NexusEvent = "cancel"
	NexusTimeout       NexusEvent = "timeout"
	NexusTerminate     NexusEvent = "terminate"
	NexusReject        NexusEvent = "reject"
)

// NexusTransition / the SAA-style total transition function was removed: the
// total-transition oracle already lives in the generic Lifecycle.Classify, and the
// only thing this layer added — the config-dependent retry-budget prediction — had no
// consumer (it is a drive-side prediction, unobservable on the wire). See the
// conformance discussion; Classify remains the single per-edge oracle.
