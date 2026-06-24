package chasm

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// Time skipping is a two-sided contract between the CHASM framework and a root component:
//
//   - TimeSkippingController is PROVIDED BY the framework and CALLED BY the
//     component. It is embedded in MutableContext and allows the component to opt in / adjust the config via
//     SetTimeSkippingConfig.
//   - TimeSkippable is the mandatory component-implemented interface that must be implemented by the root
//     component of an execution to make time skipping work.

// TimeSkippingController is the framework-provided surface (embedded in MutableContext) through which a
// root component opts into and adjusts time skipping for the whole execution.
type TimeSkippingController interface {
	// SetTimeSkippingConfig sets the execution's time-skipping config: the first call establishes it,
	// later calls update it in place (preserving the accumulated skipped duration).
	SetTimeSkippingConfig(config *commonpb.TimeSkippingConfig)
}

// TimeSkippable is the mandatory component-implemented half of the time-skipping contract. The root
// component of an execution that opts into time skipping implements it so the framework can decide, once
// per transaction, whether the execution is idle enough to fast-forward virtual time. ROOT COMPONENT
// ONLY — the framework consults only the root component's implementation.
type TimeSkippable interface {
	// HasInflightWork returns true when the execution has in-flight work (e.g. a standalone activity
	// whose attempt has been dispatched to a worker or is currently running). The framework skips no
	// time while it returns true, so in-flight work runs at real speed.
	HasInflightWork(ctx Context) bool
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
