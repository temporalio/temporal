package clock

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TimeSkippingTimeSource wraps a real TimeSource and adds a virtual time offset.
// When time skipping is enabled for a workflow, the workflow's "now" is real time + offset.
// The offset accumulates across skip events and is reconstructed from persisted
// TimeSkippedDetails on workflow reload — no separate field needs to be persisted.
type TimeSkippingTimeSource struct {
	base   TimeSource
	offset time.Duration
}

var _ TimeSource = (*TimeSkippingTimeSource)(nil)

// newTimeSkippingTimeSource creates a TimeSkippingTimeSource whose offset is the sum of all
// previously skipped durations. Pass nil or empty details when starting a new workflow.
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

// AfterFunc delegates to the base time source. OS-level timers run on real wall clock.
func (ts *TimeSkippingTimeSource) AfterFunc(d time.Duration, f func()) Timer {
	return ts.base.AfterFunc(d, f)
}

// NewTimer delegates to the base time source. OS-level timers run on real wall clock.
func (ts *TimeSkippingTimeSource) NewTimer(d time.Duration) (<-chan time.Time, Timer) {
	return ts.base.NewTimer(d)
}

// advance increases the virtual time offset by d.
func (ts *TimeSkippingTimeSource) Advance(d time.Duration) {
	ts.offset += d
}

// ComputeTotalSkippedOffset sums all DurationToSkip values from persisted TimeSkippedDetails.
func ComputeTotalSkippedOffset(details []*persistencespb.TimeSkippedDetails) time.Duration {
	var total time.Duration
	for _, d := range details {
		total += TimeSkippedDurationFromTimestamp(d.GetDurationToSkip())
	}
	return total
}

// TimeSkippedDurationToTimestamp encodes a time.Duration into a *timestamppb.Timestamp
// by storing seconds and nanoseconds directly in the Timestamp fields.
// This is a convention for the DurationToSkip field in TimeSkippedDetails.
func TimeSkippedDurationToTimestamp(d time.Duration) *timestamppb.Timestamp {
	return &timestamppb.Timestamp{
		Seconds: int64(d / time.Second),
		Nanos:   int32(d % time.Second),
	}
}

// TimeSkippedDurationFromTimestamp reverses TimeSkippedDurationToTimestamp.
func TimeSkippedDurationFromTimestamp(ts *timestamppb.Timestamp) time.Duration {
	if ts == nil {
		return 0
	}
	return time.Duration(ts.GetSeconds())*time.Second + time.Duration(ts.GetNanos())
}
