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

type TimeSkippingController interface {

	// SetTimeSkippingConfig sets the execution's time-skipping config: the first call establishes it,
	// later calls update it in place (preserving the accumulated skipped duration).
	SetTimeSkippingConfig(config *commonpb.TimeSkippingConfig)
}

type TimeSkippable interface {

	// HasInflightWork returns true when an execution has in-flight work (e.g. a standalone activity
	// whose attempt has been dispatched to a worker or is currently running). Time skipping won't skip
	// any time when it returns true so that in-flight work has enough time to complete.
	HasInflightWork(ctx Context) bool
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
	CurrentTime              time.Time
	TargetTime               time.Time
	DisabledAfterFastForward bool
}

// IsValid reports whether the transition represents something to record and apply.
func (t *TimeSkippingTransition) IsValid() bool {
	return t != nil && (!t.TargetTime.IsZero() || t.DisabledAfterFastForward)
}

func IsActiveFastForward(ff *persistencespb.FastForwardInfo) bool {
	return ff != nil && !ff.HasReached && ff.TargetTime != nil && !ff.TargetTime.AsTime().IsZero()
}
