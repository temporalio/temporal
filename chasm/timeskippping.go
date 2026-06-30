package chasm

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// Time skipping is a two-sided contract between the CHASM framework and a root component, split along
// the configuration / runtime axis:
//
//   - TimeSkippingConfigurator is PROVIDED BY the framework and CALLED BY the component. It is embedded
//     in MutableContext and lets the component opt in / adjust the config via SetTimeSkippingConfig.
//   - TimeSkippingRuntime is the mandatory component-implemented half: the root component implements it
//     so the framework can decide, at runtime, whether the execution may be skipped.

// TimeSkippingConfigurator is the framework-provided, configuration-time surface (embedded in
// MutableContext) through which a root component opts into and adjusts time skipping for the whole
// execution.
type TimeSkippingConfigurator interface {
	// SetTimeSkippingConfig sets the execution's time-skipping config: the first call establishes it,
	// later calls update it in place (preserving the accumulated skipped duration).
	SetTimeSkippingConfig(config *commonpb.TimeSkippingConfig)
}

// TimeSkippingRuntime is the mandatory component-implemented, runtime half of the time-skipping
// contract. The root component of an execution that opts into time skipping implements it so the
// framework can decide, once per transaction, whether the execution is idle enough to fast-forward
// virtual time. ROOT COMPONENT ONLY — the framework consults only the root component's implementation.
type TimeSkippingRuntime interface {
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
// signal. Nil-safe.
func (t *TimeSkippingTransition) IsValid() bool {
	return t != nil && (!t.TargetTime.IsZero() || t.DisabledAfterFastForward)
}

// CompareAndSetTargetTime folds a business wake candidate (virtual time) in, keeping the earliest one at
// or after CurrentTime. Zero or past candidates (and a nil/uninitialized transition) are ignored. It
// never touches DisabledAfterFastForward — only the fast-forward budget disables time skipping.
func (t *TimeSkippingTransition) CompareAndSetTargetTime(candidate time.Time) {
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
	if ff.GetHasReached() || ff.GetTargetTime().AsTime().IsZero() {
		return
	}
	if t == nil || t.CurrentTime.IsZero() {
		return
	}
	ffTargetTime := ff.GetTargetTime().AsTime()
	t.CompareAndSetTargetTime(ffTargetTime)
	if !ffTargetTime.After(t.CurrentTime) {
		t.DisabledAfterFastForward = true
	}
}
