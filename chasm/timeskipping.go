package chasm

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// =============================================================================
// Time Skipping CHASM Contract
// =============================================================================
// This contract is between the CHASM framework and a root component, split along the
// configuration / runtime axis:
//
//   - TimeSkippingConfigurator (PROVIDED BY the framework, CALLED BY the component) is the
//     configuration-time surface. It is embedded in MutableContext and lets the component opt in /
//     adjust the config via SetTimeSkippingConfig.
//   - TimeSkippingRuntimeGate (component-implemented, MANDATORY) is the runtime surface the framework
//     consults each transaction to decide whether the execution is idle enough to skip. The framework
//     consults only the root component for this interface.

type TimeSkippingConfigurator interface {
	// SetTimeSkippingConfig sets the execution's time-skipping config: the first call establishes it,
	// later calls update it in place (preserving the accumulated skipped duration).
	SetTimeSkippingConfig(config *commonpb.TimeSkippingConfig)
}

type TimeSkippingRuntimeGate interface {
	// IsExecutionSkippable reports whether the execution may have its virtual clock fast-forwarded right
	// now. The framework skips no time while it returns false.
	//
	// A recommended implementation composes:
	//   1. status check: if the execution is not paused nor completed, time may not need to skip.
	//   2. in-flight work check: if the execution has in-flight work, skip to next timer task may
	//     cause timeout and the worker may not have a chance to finish its work.
	//   3. other execution-specific preconditions, as needed.
	IsExecutionSkippable(ctx Context) bool
}

// =============================================================================
// Time Skipping Data Structure
// =============================================================================

type TimeSkippingTransition struct {
	CurrentTime              time.Time
	TargetTime               time.Time
	DisabledAfterFastForward bool
}

// NewTimeSkippingTransition creates a new time-skipping transition with the current time.
// Methods provided by this data structure cannot be used without a current time.
//
// todo@time-skipping: the methods will be used by CHASM so keep as public.
func NewTimeSkippingTransition(currentTime time.Time) *TimeSkippingTransition {
	return &TimeSkippingTransition{CurrentTime: currentTime}
}

// IsValid reports whether the transition is worth applying: a real skip target, or a bare disable
// signal. Nil-safe. A transition without a current time is never valid — every meaningful field is
// derived relative to the current time, so without it there is nothing to apply.
func (t *TimeSkippingTransition) IsValid() bool {
	return t != nil && !t.CurrentTime.IsZero() && (!t.TargetTime.IsZero() || t.DisabledAfterFastForward)
}

func (t *TimeSkippingTransition) TrackEarliestFutureTime(candidate time.Time) {
	if t == nil || t.CurrentTime.IsZero() || candidate.IsZero() || candidate.Before(t.CurrentTime) {
		return
	}
	if t.TargetTime.IsZero() || candidate.Before(t.TargetTime) {
		t.TargetTime = candidate
	}
}

func (t *TimeSkippingTransition) GateByFastForward(ff *persistencespb.FastForwardInfo) {
	if t == nil || t.CurrentTime.IsZero() {
		return
	}
	if ff == nil || ff.GetHasReached() || ff.GetTargetTime() == nil ||
		ff.GetTargetTime().AsTime().IsZero() {
		return
	}
	ffTargetTime := ff.GetTargetTime().AsTime()
	// If a real candidate is scheduled strictly before the fast-forward target, we skip to
	// that and the fast-forward budget is not yet exhausted — leave time skipping enabled.
	if !t.TargetTime.IsZero() && t.TargetTime.Before(ffTargetTime) {
		return
	}
	// Otherwise the fast-forward target is the earliest target: skip to it (clamped to the
	// present by TrackEarliestFutureTime) and disable time skipping — the budget is reached.
	// This is what lets the budget cap a chain of runs: a run with no earlier candidate
	// consumes the remaining budget by skipping to the fast-forward and disabling.
	t.TrackEarliestFutureTime(ffTargetTime)
	t.DisabledAfterFastForward = true
}
