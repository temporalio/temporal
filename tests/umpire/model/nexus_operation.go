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
	Attempt          int    // observed retry attempt, from chasm.transition telemetry
	FSM              *umpire.Lifecycle
}

func NewNexusOperation() *NexusOperation {
	op := &NexusOperation{}
	// active are the in-flight states an operation must eventually settle out of.
	active := []string{NexusScheduled, NexusBackingOff, NexusStarted}
	op.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: NexusUnspecified,
		// Each state carries its traits: the in-flight states MustProgress; the
		// terminals carry their modeled outcome. succeeded is a clean completion;
		// failed and timed_out are acceptable failure terminals (a fault reaching one
		// is degradation, not a bug). canceled is left untagged: it is a user-driven
		// decision, not a success or a failure of the operation. Terminal-ness itself
		// derives from the transition graph.
		States: umpire.States{
			NexusUnspecified: {},
			NexusScheduled:   {umpire.MustProgress},
			NexusBackingOff:  {umpire.MustProgress},
			NexusStarted:     {umpire.MustProgress},
			NexusSucceeded:   {umpire.Success},
			NexusFailed:      {umpire.Failure},
			NexusCanceled:    {}, // user-driven decision, not a failure
			NexusTimedOut:    {umpire.Failure},
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
			// start: the async handler acknowledged (sync completion skips this).
			{
				Event:  NexusStart,
				From:   []string{NexusScheduled, NexusBackingOff},
				To:     NexusStarted,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
			// Terminal transitions may fire from any active state;
			// "started precedes succeeded" is NOT an invariant (sync completes direct).
			{
				Event:  NexusSucceed,
				From:   active,
				To:     NexusSucceeded,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
			{
				Event:  NexusFail,
				From:   active,
				To:     NexusFailed,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
			{
				Event:  NexusCancel,
				From:   active,
				To:     NexusCanceled,
				Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)},
			},
			{
				Event:  NexusTimeout,
				From:   active,
				To:     NexusTimedOut,
				Traits: umpire.Traits{umpire.Needs(umpire.Faults)},
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

// This file gives NexusOperation an SAA-style *total transition function* on top of its
// Lifecycle: NexusTransition(config, abstractState, event) -> NexusOutcome predicts, for
// every input, the next abstract state, the rejection (if any), and the observable side
// effects. It is the "complete edge contract" the SAA model has and a bare FSM lacks (see
// UMPIRE_PRIOR_ART.md, the SAA section). The lifecycle-state part is delegated to the
// generic Lifecycle's Classify so the two can never disagree; this layer adds the
// archetype's reject/side-effect contract and the config-dependent retry branch.

// NexusConfig is the start-time configuration a NexusOperation's behaviour branches on —
// the analog of SAA's model.Config. It is what makes the transition function total over a
// *family* of operations rather than one fixed graph.
type NexusConfig struct {
	// MaxAttempts caps retries; 0 means unlimited. When the budget is exhausted a
	// retryable failure settles the operation instead of backing off.
	MaxAttempts int
}

// NexusAbstract is the observable abstract state of a NexusOperation: its lifecycle state
// plus the attempt count (the retry loop, which the bare FSM graph does not track). The
// analog of SAA's AbstractState.
type NexusAbstract struct {
	State   string
	Attempt int
}

// NexusReject is the API/validation error an event would produce when it is not a legal
// advance — the analog of SAA's reject ErrorKind. Empty means no rejection.
type NexusReject string

const (
	NexusRejectNone         NexusReject = ""
	NexusRejectPrecondition NexusReject = "FailedPrecondition"
)

// NexusOutcome is the full predicted contract of applying one event to a NexusOperation in
// a given abstract state under a config: the transition kind, the next abstract state, the
// rejection (if any), and the observable side effects. A superset of the generic
// Lifecycle's state-only Outcome.
type NexusOutcome struct {
	Kind         umpire.TransitionKind
	From, Next   NexusAbstract
	Reject       NexusReject
	AttemptDelta int  // +1 when this edge schedules a new attempt
	BackoffArmed bool // a retry backoff timer is armed after this edge
	Terminal     bool // the operation is settled after this edge
}

// NexusTransition predicts the full outcome of applying event to a NexusOperation in
// abstract state cur under cfg. Total: every (cfg, state, event) yields a defined outcome.
func NexusTransition(cfg NexusConfig, cur NexusAbstract, event string) NexusOutcome {
	lc := NewNexusOperation().Lifecycle()
	lc.SetState(cur.State)
	base := lc.Classify(event) // Advance / NoOp / Illegal + target state

	out := NexusOutcome{From: cur, Next: cur, Kind: base.Kind}
	switch base.Kind {
	case umpire.Illegal:
		// An impossible edge: the server would reject it on a precondition.
		out.Reject = NexusRejectPrecondition
		out.Terminal = nexusIsTerminal(cur.State)
		return out
	case umpire.NoOp:
		// Benign re-observation (incl. any event once settled); no side effects.
		out.Terminal = nexusIsTerminal(cur.State)
		return out
	}

	// Advance: the lifecycle moves to base.To; enrich with the archetype contract.
	out.Next.State = base.To
	switch event {
	case NexusSchedule:
		// Initial schedule (attempt 1) or a retry out of backing_off (attempt+1).
		out.Next.Attempt = cur.Attempt + 1
		out.AttemptDelta = 1
	case NexusAttemptFailed:
		// Retryable failure → backoff, unless the retry budget is exhausted, in which
		// case the operation fails terminally instead (the config-dependent branch).
		if cfg.MaxAttempts > 0 && cur.Attempt >= cfg.MaxAttempts {
			out.Next.State = NexusFailed
		} else {
			out.BackoffArmed = true
		}
	}
	out.Terminal = nexusIsTerminal(out.Next.State)
	return out
}

func nexusIsTerminal(state string) bool {
	switch state {
	case NexusSucceeded, NexusFailed, NexusCanceled, NexusTimedOut:
		return true
	default:
		return false
	}
}
