package workflow

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
)

// TimeSkippingTimeSource wraps a real TimeSource and adds a virtual time offset.
// When time skipping is enabled for a workflow, the workflow's "now" is real time + offset.
// The offset accumulates across skip events and is reconstructed from persisted
// TimeSkippedDetails on workflow reload — no separate field needs to be persisted.
type TimeSkippingTimeSource struct {
	base   clock.TimeSource
	offset time.Duration
}

var _ clock.TimeSource = (*TimeSkippingTimeSource)(nil)

// newTimeSkippingTimeSource creates a TimeSkippingTimeSource whose offset is the sum of all
// previously skipped durations. Pass nil or empty details when starting a new workflow.
func newTimeSkippingTimeSource(base clock.TimeSource, details []*persistencespb.TimeSkippedDetails) *TimeSkippingTimeSource {
	return &TimeSkippingTimeSource{
		base:   base,
		offset: computeTotalSkippedOffset(details),
	}
}

// Now returns the current virtual time (real time + accumulated skipped offset).
func (ts *TimeSkippingTimeSource) Now() time.Time {
	return ts.base.Now().Add(ts.offset)
}

// Since returns the time elapsed since t in virtual time.
func (ts *TimeSkippingTimeSource) Since(t time.Time) time.Duration {
	return ts.Now().Sub(t)
}

// AfterFunc delegates to the base time source. OS-level timers run on real wall clock.
func (ts *TimeSkippingTimeSource) AfterFunc(d time.Duration, f func()) clock.Timer {
	return ts.base.AfterFunc(d, f)
}

// NewTimer delegates to the base time source. OS-level timers run on real wall clock.
func (ts *TimeSkippingTimeSource) NewTimer(d time.Duration) (<-chan time.Time, clock.Timer) {
	return ts.base.NewTimer(d)
}

// advance increases the virtual time offset by d.
func (ts *TimeSkippingTimeSource) advance(d time.Duration) {
	ts.offset += d
}

// computeTotalSkippedOffset sums all DurationToSkip values from persisted TimeSkippedDetails.
func computeTotalSkippedOffset(details []*persistencespb.TimeSkippedDetails) time.Duration {
	var total time.Duration
	for _, d := range details {
		total += timeSkippedDurationFromTimestamp(d.GetDurationToSkip())
	}
	return total
}
