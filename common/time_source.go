package common

import "time"

type (
	// TimeSource is an interface for any
	// entity that provides the current
	// time. Its primarily used to mock
	// out timesources in unit test
	TimeSource interface {
		Now() time.Time
	}
	// realTimeSource serves real wall-clock time
	realTimeSource struct{}
)

// NewRealTimeSource returns a time source that servers
// real wall clock time using CLOCK_REALTIME
func NewRealTimeSource() TimeSource {
	return &realTimeSource{}
}

func (ts *realTimeSource) Now() time.Time {
	return time.Now()
}
