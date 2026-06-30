package chasm

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// Time skipping is a contract between the CHASM framework and a root component, split along the
// configuration / runtime axis:
//
//   - TimeSkippingConfigurator (PROVIDED BY the framework, CALLED BY the component) is the
//     configuration-time surface. It is embedded in MutableContext and lets the component opt in /
//     adjust the config via SetTimeSkippingConfig.
//   - TimeSkippingRuntimeGate (component-implemented, MANDATORY) is the runtime surface the framework
//     consults each transaction to decide whether the execution is idle enough to skip.
//   - TimeSkippingRuntimeTargetProvider (component-implemented, OPTIONAL) lets a component nominate the
//     next skip target itself, in place of the framework's default timer-task tree scan.
//
// The two runtime interfaces are ROOT COMPONENT ONLY — the framework consults only the root component.

// TimeSkippingConfigurator is the framework-provided, configuration-time surface (embedded in
// MutableContext) through which a root component opts into and adjusts time skipping for the whole
// execution.
type TimeSkippingConfigurator interface {
	// SetTimeSkippingConfig sets the execution's time-skipping config: the first call establishes it,
	// later calls update it in place (preserving the accumulated skipped duration).
	SetTimeSkippingConfig(config *commonpb.TimeSkippingConfig)
}

// TimeSkippingRuntimeGate is the mandatory, runtime half of the time-skipping contract: the framework
// asks it, once per transaction, whether the execution may be skipped at all.
type TimeSkippingRuntimeGate interface {
	// IsExecutionSkippable reports whether the execution may have its virtual clock fast-forwarded right
	// now. It returns true only when the execution is idle. The framework skips no time while it returns
	// false, so in-flight work runs at real speed.
	//
	// A recommended implementation composes:
	//   1. status check (optional): only states that can be idle are skip candidates; any active or
	//      terminal-but-still-working status returns false.
	//   2. in-flight work check (mandatory): false whenever real work is dispatched or running (e.g. a
	//      scheduled attempt whose dispatch time is at or before now).
	//   3. other execution-specific preconditions, as needed.
	IsExecutionSkippable(ctx Context) bool
}

// TimeSkippingRuntimeTargetProvider is the optional, by default the framework scans the tree's
// all pure and side-effect tasks that are future-scheduled and takes the earliest one as the skip target.
// If the execution needs a customized strategy, the root component can implement this interface
// and it will override the default strategy.
type TimeSkippingRuntimeTargetProvider interface {
	// FindNextTargetTime provides a customized strategy to find the next skip target.
	// The framework will provided a initilalized transition, and the implementation should just call
	// transition.TrackEarliestFutureTime to set the target time, and it is allowed to leave the transition unchanged.
	FindNextTargetTime(ctx Context, transition *TimeSkippingTransition)
}

// TimeSkippingTransition is a time-skipping decision and is shared between the CHASM framework and
// the history substrate.
type TimeSkippingTransition struct {
	CurrentTime              time.Time
	TargetTime               time.Time
	DisabledAfterFastForward bool
}

// NewTimeSkippingTransition creates a new time-skipping transition with the given current time.
// A valid transition always has a non-zero CurrentTime; the fold methods below no-op without it.
func NewTimeSkippingTransition(currentTime time.Time) *TimeSkippingTransition {
	return &TimeSkippingTransition{CurrentTime: currentTime}
}

// IsValid reports whether the transition is worth applying: a real skip target, or a bare disable
// signal. Nil-safe. A transition without a current time is never valid — every meaningful field is
// derived relative to the current time, so without it there is nothing to apply.
func (t *TimeSkippingTransition) IsValid() bool {
	return t != nil && !t.CurrentTime.IsZero() && (!t.TargetTime.IsZero() || t.DisabledAfterFastForward)
}

// TrackEarliestFutureTime folds a business wake candidate (virtual time) in, keeping the earliest one at
// or after CurrentTime. Zero or past candidates (and a nil/uninitialized transition) are ignored. It
// never touches DisabledAfterFastForward — only the fast-forward budget disables time skipping.
func (t *TimeSkippingTransition) TrackEarliestFutureTime(candidate time.Time) {
	if t == nil || t.CurrentTime.IsZero() || candidate.IsZero() || candidate.Before(t.CurrentTime) {
		return
	}
	if t.TargetTime.IsZero() || candidate.Before(t.TargetTime) {
		t.TargetTime = candidate
	}
}

// GateByFastForward folds the fast-forward target (the skip budget) in and decides whether
// reaching it disables time skipping. It MUST be called AFTER all business candidates have been folded,
// because the budget is a ceiling.
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
