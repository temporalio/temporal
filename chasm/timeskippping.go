package chasm

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// Time skipping is a two-sided contract between the CHASM framework and a root component:
//
//   - TimeSkippingController (this interface) is PROVIDED BY the framework and CALLED BY the
//     component. It is embedded in MutableContext: the component opts in via InitTimeSkippingConfig,
//     adjusts the config with UpdateTimeSkippingConfig, and reads it back with GetTimeSkippingConfig.
//   - TimeSkippable (below) is IMPLEMENTED BY the root component and CALLED BY the framework, so the
//     framework can ask whether the execution is idle enough to fast-forward, and where to skip to.
//
// A root component that wants time skipping does both: implement TimeSkippable, and call
// InitTimeSkippingConfig when the execution is created.

// TimeSkippingController is the framework-provided surface through which a component controls time
// skipping for the whole execution.
// The interface is embedded in MutableContext (the component-facing surface).
// The backing mutable state (NodeBackend) implements the mutators and exposes the full TimeSkippingInfo
// to the framework via GetTimeSkippingInfo.
type TimeSkippingController interface {
	InitTimeSkippingConfig(config *commonpb.TimeSkippingConfig)
	UpdateTimeSkippingConfig(config *commonpb.TimeSkippingConfig)
	GetTimeSkippingConfig() *commonpb.TimeSkippingConfig
}

// TimeSkippable is the component-implemented half of the time-skipping contract (see
// TimeSkippingController above for the framework-provided half). A root component that opts into time
// skipping (by calling MutableContext.InitTimeSkippingConfig) implements this so the CHASM framework
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
// The framework drives two calls per transaction, in order, at the END of CloseTransaction if the execution has
// time skipping enabled:
//  1. HasInflightWork — the mandatory idle gate. It must return true whenever advancing virtual time
//     would skip past real work (e.g. a standalone activity whose attempt has been dispatched to a
//     worker or is currently running). When it returns true the framework does not skip and does not
//     consult NextTimePoint.
//     HasInflightWork is kept as a distinct method (rather than folded into FindNextTargetTime) to make the
//     idle check an explicit, mandatory part of the contract for implementors.
//  2. FindNextTargetTime — the mandatory method to find the next target time to skip to.
//     The input parameter is pre-seeded with the configured fast-forward target if there is a valid one.
//     The implementation should directly call CompareAndSet to set a new possible target time point.
//     A timeout time point is not a good target time point to skip to, but it is usually a good practice
//     to call GateByExecutionTimeout to cap the target time point at the entire execution timeout if time skipping
//     is not expected to jump past the execution timeout.
type TimeSkippable interface {
	HasInflightWork(ctx Context) bool
	FindNextTargetTime(ctx Context, nextTargetTime *TimeSkippingTargetTime)
}

type TimeSkippingTargetTime time.Time

func (n *TimeSkippingTargetTime) CompareAndSet(other time.Time) {
	if other.IsZero() {
		return
	}
	// Initialize when there is no candidate yet (current is zero), otherwise keep the earlier one.
	current := n.GetTime()
	if current.IsZero() || other.Before(current) {
		*n = TimeSkippingTargetTime(other)
	}
}

func (n *TimeSkippingTargetTime) GateByExecutionTimeout(executionTimeout time.Time) {
	current := n.GetTime()
	if current.IsZero() || executionTimeout.IsZero() {
		return
	}
	if current.After(executionTimeout) {
		*n = TimeSkippingTargetTime(executionTimeout)
	}
}

func (n *TimeSkippingTargetTime) GetTime() time.Time {
	if n == nil {
		return time.Time{}
	}
	return time.Time(*n)
}

func (n *TimeSkippingTargetTime) IsZero() bool {
	return n.GetTime().IsZero()
}

// TimeSkippingTransition is the result of a time-skipping decision: the virtual time the transition
// is recorded at (CurrentTime), the virtual time to advance to (TargetTime), and whether reaching it
// should disable time skipping because the fast-forward budget was reached (DisabledAfterFastForward).
// The applied skip delta is TargetTime - CurrentTime. It is an internal struct shared between the
// CHASM framework and the mutable-state backend — it is NOT part of the component-facing TimeSkippable
// contract. A component only returns a candidate time point (from NextTimePoint); the framework combines
// that with the configured fast-forward target/bound into this struct when recording the skip. The
// workflow (event-based) time-skipping path uses the same struct.
type TimeSkippingTransition struct {
	// CurrentTime is the virtual "now" at which the transition is recorded (virtual frame, not wall
	// clock). The skip delta added to the accumulated skipped duration is TargetTime - CurrentTime.
	CurrentTime              time.Time
	TargetTime               time.Time
	DisabledAfterFastForward bool
}

// IsValid reports whether the transition represents something to apply: a non-zero target to skip
// to, or a disable-after-fast-forward signal. It is nil-safe — a nil transition is not valid.
func (t *TimeSkippingTransition) IsValid() bool {
	if t == nil {
		return false
	}
	return !t.TargetTime.IsZero() || t.DisabledAfterFastForward
}

// buildTimeSkippingTransition turns the component's resolved candidate (targetTime, already seeded with and
// min'd against the fast-forward target) into a TimeSkippingTransition to record, or nil if there is nothing
// to do. currentTime is virtual "now"; ffTarget is the configured fast-forward target (zero if none).
func buildTimeSkippingTransition(
	currentTime time.Time,
	targetTime *TimeSkippingTargetTime,
	ffTarget time.Time,
) *TimeSkippingTransition {
	target := targetTime.GetTime()

	// No valid future target to skip to.
	if target.IsZero() || !target.After(currentTime) {
		// Even with nothing to skip to, if the fast-forward target has already been reached, signal
		// that time skipping should be disabled.
		if !ffTarget.IsZero() && !ffTarget.After(currentTime) {
			return &TimeSkippingTransition{
				CurrentTime:              currentTime,
				DisabledAfterFastForward: true,
			}
		}
		return nil
	}

	// We have a future target. Reaching it disables time skipping when it lands at or past the
	// fast-forward target (the seed guarantees target never exceeds ffTarget when ffTarget is set).
	disabledAfterFastForward := false
	if !ffTarget.IsZero() {
		disabledAfterFastForward = !target.Before(ffTarget)
	}
	return &TimeSkippingTransition{
		CurrentTime:              currentTime,
		TargetTime:               target,
		DisabledAfterFastForward: disabledAfterFastForward,
	}
}

func IsActiveFastForward(ff *persistencespb.FastForwardInfo) bool {
	return ff != nil && !ff.HasReached && ff.TargetTime != nil && !ff.TargetTime.AsTime().IsZero()
}
