package clock

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// TimeSkippingTimeSource is a TimeSource decorator for workflows with time skipping enabled.
//
// Virtual time = real time + offset, where offset is the total duration skipped so far.
// The offset starts at zero and grows each time a WorkflowExecutionTimeSkipped event is applied
// (via Advance). It is never stored as its own field — on workflow reload it is re-derived from
// the persisted TimeSkippedDetails by summing all DurationToSkip values.
//
// Timer methods (AfterFunc, NewTimer) are intentionally not virtualized: OS-level timers always
// fire on real wall clock time. Only Now and Since reflect virtual time.
type TimeSkippingTimeSource struct {
	base   TimeSource
	offset time.Duration
}

var _ TimeSource = (*TimeSkippingTimeSource)(nil)

// NewTimeSkippingTimeSource creates a TimeSkippingTimeSource whose initial offset is the sum of
// all previously skipped durations. Pass nil or empty details for a brand-new workflow (offset 0).
func NewTimeSkippingTimeSource(base TimeSource, details []*persistencespb.TimeSkippedDetails) *TimeSkippingTimeSource {
	return &TimeSkippingTimeSource{
		base:   base,
		offset: ComputeTotalSkippedOffset(details),
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

// AfterFunc schedules f to run after duration d on the real wall clock.
// TODO(@feiyang): explore if this method needs to respect virtual time — currently it delegates
// to the base clock so f fires after d of wall time, not virtual time.
func (ts *TimeSkippingTimeSource) AfterFunc(d time.Duration, f func()) Timer {
	return ts.base.AfterFunc(d, f)
}

// NewTimer creates a timer that fires after duration d on the real wall clock.
// TODO(@feiyang): explore if this method needs to respect virtual time — currently it delegates
// to the base clock so the timer fires after d of wall time, not virtual time.
func (ts *TimeSkippingTimeSource) NewTimer(d time.Duration) (<-chan time.Time, Timer) {
	return ts.base.NewTimer(d)
}

// Advance increases the virtual time offset by d.
// Called each time a WorkflowExecutionTimeSkipped event is applied to keep the
// in-memory clock in sync without rebuilding it from the full persisted history.
func (ts *TimeSkippingTimeSource) Advance(d time.Duration) {
	ts.offset += d
}

// ComputeTotalSkippedOffset sums the DurationToSkip of each persisted TimeSkippedDetails entry.
// This reconstructs the total virtual time offset after a workflow is loaded from the database.
func ComputeTotalSkippedOffset(details []*persistencespb.TimeSkippedDetails) time.Duration {
	var total time.Duration
	for _, d := range details {
		total += d.GetDuration().AsDuration()
	}
	return total
}
