package chasm

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// Time skipping is a two-sided contract between the CHASM framework and a root component:
//
//   - TimeSkippingController (this interface) is PROVIDED BY the framework and CALLED BY the
//     component. It is embedded in MutableContext: the component opts in / adjusts the config via
//     SetTimeSkippingConfig. (The framework reads the config back via NodeBackend.GetTimeSkippingInfo,
//     not through this component-facing surface.)
//   - TimeSkippable (below) is IMPLEMENTED BY the root component and CALLED BY the framework, so the
//     framework can ask whether the execution is idle enough to fast-forward, and where to skip to.
//
// A root component that wants time skipping does both: implement TimeSkippable, and call
// SetTimeSkippingConfig when the execution is created.

// TimeSkippingController is the framework-provided surface through which a component controls time
// skipping for the whole execution.
// The interface is embedded in MutableContext (the component-facing surface).
// The backing mutable state (NodeBackend) implements the mutators and exposes the full TimeSkippingInfo
// to the framework via GetTimeSkippingInfo.
type TimeSkippingController interface {
	// SetTimeSkippingConfig sets the execution's time-skipping config: the first call establishes it,
	// later calls update it in place (preserving the accumulated skipped duration).
	SetTimeSkippingConfig(config *commonpb.TimeSkippingConfig)
}

// TimeSkippable is the component-implemented half of the time-skipping contract (see
// TimeSkippingController above for the framework-provided half). A root component that opts into time
// skipping (by calling MutableContext.SetTimeSkippingConfig) implements this so the CHASM framework
// can call HasInflightWork during CloseTransaction to decide whether the execution is idle enough to
// fast-forward virtual time.
//
// ROOT COMPONENT ONLY. Time skipping is a per-execution concern decided once per transaction, so the
// framework only ever consults the ROOT component's TimeSkippable implementation (resolved via the
// root ComponentRef in closeTransactionHandleTimeSkipping). Implementing this interface on a non-root
// (child) component has NO effect — its methods are never called. Most components therefore should not
// implement it; only the root archetype of an execution that wants time skipping does.
//
// Time skipping only advances virtual time while the execution is idle and there is a fast-forward target time.
// When virtual time is advanced, the framework will regenerate all scheduled tasks so that their visibility timestamps
// are updated to the new virtual time.
//
// The framework consults two methods at the END of CloseTransaction when time skipping is enabled:
//
//	HasInflightWork — the mandatory idle gate. It must return true whenever advancing virtual time would
//	skip past real work (e.g. a standalone activity whose attempt has been dispatched to a worker or is
//	currently running). When it returns true the framework does not skip.
//
//	FindNextTargetTime — choose where to skip to. The component contributes its next wake time into
//	target (via CompareAndSet, optionally capped by GateByExecutionTimeout); the framework then combines
//	the result with the configured fast-forward target. This is component-only by design: only the
//	component knows which of its pending timers is a legitimate wake (a retry backoff / next dispatch)
//	versus an execution/run timeout that virtual time must never be advanced to. The framework only
//	knows a task's category (CategoryTimer), not its meaning, so it cannot choose the target itself and
//	provides no default scan.
type TimeSkippable interface {
	HasInflightWork(ctx Context) bool
	FindNextTargetTime(ctx Context, target *TimeSkippingTargetTime)
}

// TimeSkippingTargetTime is a nil-safe accumulator passed to TimeSkippable.FindNextTargetTime for a
// component to contribute candidate skip targets (virtual time). It is the easy way to implement
// FindNextTargetTime, and is intentionally kept separate from TimeSkippingTransition (which is the
// decision the framework records into the history substrate). A zero value means "no candidate yet":
// CompareAndSet keeps the earliest candidate, GateByExecutionTimeout caps it at an execution deadline.
// It is a defined type over time.Time; the mutating methods use a pointer receiver so the component's
// calls are visible to the framework through the shared pointer.
type TimeSkippingTargetTime time.Time

// CompareAndSet keeps the earlier candidate: initializes when unset, otherwise keeps the earlier of the
// current value and other. A zero other is ignored.
func (n *TimeSkippingTargetTime) CompareAndSet(other time.Time) {
	if other.IsZero() {
		return
	}
	current := n.GetTime()
	if current.IsZero() || other.Before(current) {
		*n = TimeSkippingTargetTime(other)
	}
}

// GateByExecutionTimeout caps the candidate at executionTimeout so a skip never advances past it. No-op
// if there is no candidate yet or the timeout is unset.
func (n *TimeSkippingTargetTime) GateByExecutionTimeout(executionTimeout time.Time) {
	current := n.GetTime()
	if current.IsZero() || executionTimeout.IsZero() {
		return
	}
	if current.After(executionTimeout) {
		*n = TimeSkippingTargetTime(executionTimeout)
	}
}

// GetTime returns the accumulated candidate (zero if unset). Nil-safe.
func (n *TimeSkippingTargetTime) GetTime() time.Time {
	if n == nil {
		return time.Time{}
	}
	return time.Time(*n)
}

// TimeSkippingTransition is a time-skipping decision: the virtual time the transition is recorded at
// (CurrentTime), the virtual time to advance to (TargetTime), and whether reaching it should disable
// time skipping because the fast-forward budget was reached (DisabledAfterFastForward). The applied
// skip delta is TargetTime - CurrentTime. It is an internal struct shared between the CHASM framework
// and the mutable-state backend — NOT part of the component-facing contract.
//
// The framework builds it directly: it seeds TargetTime with the fast-forward target and folds in each
// valid pending timer via considerTarget (keeping the earliest), then finalizes CurrentTime and
// DisabledAfterFastForward. The workflow (event-based) path uses the same struct.
type TimeSkippingTransition struct {
	// CurrentTime is the virtual "now" at which the transition is recorded (virtual frame, not wall
	// clock). The skip delta added to the accumulated skipped duration is TargetTime - CurrentTime.
	CurrentTime              time.Time
	TargetTime               time.Time
	DisabledAfterFastForward bool
}

// considerTarget folds a candidate skip target into the transition, keeping the earliest non-zero one.
// The framework calls it to combine the fast-forward target and each valid pending timer into a single
// TargetTime; zero candidates are ignored.
func (t *TimeSkippingTransition) considerTarget(candidate time.Time) {
	if candidate.IsZero() {
		return
	}
	if t.TargetTime.IsZero() || candidate.Before(t.TargetTime) {
		t.TargetTime = candidate
	}
}

// IsValid reports whether the transition represents something to apply: a non-zero target to skip
// to, or a disable-after-fast-forward signal. It is nil-safe — a nil transition is not valid.
func (t *TimeSkippingTransition) IsValid() bool {
	if t == nil {
		return false
	}
	return !t.TargetTime.IsZero() || t.DisabledAfterFastForward
}

func IsActiveFastForward(ff *persistencespb.FastForwardInfo) bool {
	return ff != nil && !ff.HasReached && ff.TargetTime != nil && !ff.TargetTime.AsTime().IsZero()
}
