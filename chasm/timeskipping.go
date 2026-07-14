package chasm

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
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
	// later calls update it in place (preserving the accumulated skipped duration). This method also
	// validates the config and returns an invalid argument error when needed.
	SetTimeSkippingConfig(config *commonpb.TimeSkippingConfig) error
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
// Time Skipping Data Structure and Utils
// =============================================================================

type TimeSkippingTransition struct {
	CurrentTime time.Time
	// targetTime is intentionally private: it may only be written through TrackEarliestFutureTime
	// and GateByFastForward, both of which reject any candidate before CurrentTime. That guarantee
	// is what keeps GetSkippedDuration from ever going negative. Read it via GetTargetTime.
	targetTime               time.Time
	DisabledAfterFastForward bool
}

// NewTimeSkippingTransition creates a new time-skipping transition with the current time.
// Methods provided by this data structure cannot be used without a current time.
func NewTimeSkippingTransition(currentTime time.Time) *TimeSkippingTransition {
	return &TimeSkippingTransition{CurrentTime: currentTime}
}

// IsValid reports whether the transition is worth applying: a real skip target, or a bare disable
// signal. Nil-safe. A new transition without any field is not a valid one.
func (t *TimeSkippingTransition) IsValid() bool {
	return t != nil && !t.CurrentTime.IsZero() && (!t.targetTime.IsZero() || t.DisabledAfterFastForward)
}

// GetTargetTime returns the earliest tracked skip target, or the zero time when none has been set.
// Nil-safe. This is the only way to read the target: it is written exclusively through
// TrackEarliestFutureTime and GateByFastForward, which never accept a time before CurrentTime.
func (t *TimeSkippingTransition) GetTargetTime() time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.targetTime
}

func (t *TimeSkippingTransition) TrackEarliestFutureTime(candidate time.Time) {
	if t == nil || t.CurrentTime.IsZero() || candidate.IsZero() || candidate.Before(t.CurrentTime) {
		return
	}
	if t.targetTime.IsZero() || candidate.Before(t.targetTime) {
		t.targetTime = candidate
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
	if !t.targetTime.IsZero() && t.targetTime.Before(ffTargetTime) {
		return
	}
	// Otherwise the fast-forward target is the earliest target: skip to it (clamped to the
	// present by TrackEarliestFutureTime) and disable time skipping — the budget is reached.
	// This is what lets the budget cap a chain of runs: a run with no earlier candidate
	// consumes the remaining budget by skipping to the fast-forward and disabling.
	t.TrackEarliestFutureTime(ffTargetTime)
	t.DisabledAfterFastForward = true
}

func (t *TimeSkippingTransition) GetSkippedDuration() time.Duration {
	if t == nil || t.CurrentTime.IsZero() || t.targetTime.IsZero() {
		return 0
	}
	return t.targetTime.Sub(t.CurrentTime)
}

func ValidateTimeSkippingConfig(tsc *commonpb.TimeSkippingConfig) error {
	if !tsc.GetEnabled() {
		if tsc.GetFastForward() != nil {
			return serviceerror.NewInvalidArgument("time_skipping_config: cannot set fast_forward when enabled is false")
		}
		return nil
	}
	if ff := tsc.GetFastForward(); ff != nil && ff.AsDuration() < 0 {
		return serviceerror.NewInvalidArgument("time_skipping_config: fast_forward must be positive")
	}
	return nil
}
