package clock

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// TimeSkippingTimeSourceWrapper is a wrapper TimeSource that accepts a base TimeSource and an optional TimeSkippingInfo.
// If no valid TimeSkippingInfo is provided, the TimeSkippingTimeSourceWrapper will not apply time-skipping effects.
// If provided, the TimeSkippingTimeSourceWrapper will offset Now() and Since() by the accumulated skipped duration.
// Methods of AfterFunc and NewTimer will not be wrapped with time-skipping effects.
type TimeSkippingTimeSourceWrapper struct {
	base TimeSource
	// holding the timeSkippingInfo of mutableState to keep track of the accumulated skipped duration
	timeSkippingInfo *persistencespb.TimeSkippingInfo
}

func WrapTimeSourceWithTimeSkippingInfo(base TimeSource, info *persistencespb.TimeSkippingInfo) TimeSource {
	return &TimeSkippingTimeSourceWrapper{base: base, timeSkippingInfo: info}
}

// SetTimeSkippingInfo sets the TimeSkippingInfo to read accumulated skipped duration from.
// Pass nil to fall back to the base TimeSource.
func (ts *TimeSkippingTimeSourceWrapper) SetTimeSkippingInfo(info *persistencespb.TimeSkippingInfo) {
	ts.timeSkippingInfo = info
}

func (ts *TimeSkippingTimeSourceWrapper) Now() time.Time {
	t := ts.base.Now()
	if ts.timeSkippingInfo != nil && ts.timeSkippingInfo.AccumulatedSkippedDuration != nil {
		t = t.Add(ts.timeSkippingInfo.AccumulatedSkippedDuration.AsDuration())
	}
	return t
}

func (ts *TimeSkippingTimeSourceWrapper) Since(t time.Time) time.Duration {
	return ts.Now().Sub(t)
}

// AfterFunc leverages the base TimeSource's AfterFunc method, and
// time-skipping related features are not supported.
// TODO@time-skipping: examine if there is any need to skip time for this method
func (ts *TimeSkippingTimeSourceWrapper) AfterFunc(d time.Duration, f func()) Timer {
	return ts.base.AfterFunc(d, f)
}

// NewTimer leverages the base TimeSource's NewTimer method, and
// time-skipping related features are not supported.
// TODO@time-skipping: examine if there is any need to skip time for this method
func (ts *TimeSkippingTimeSourceWrapper) NewTimer(d time.Duration) (<-chan time.Time, Timer) {
	return ts.base.NewTimer(d)
}
