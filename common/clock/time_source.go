package clock

import (
	"time"

	// clockwork is not currently used but it is useful to have the option to use this in testing code
	// this comment is needed to stop lint from complaining about this _ import
	_ "github.com/jonboulle/clockwork"
)

type (
	// TimeSource is an interface for any
	// entity that provides the current
	// time. Its primarily used to mock
	// out timesources in unit test
	TimeSource interface {
		Now() time.Time
	}
	// RealTimeSource serves real wall-clock time
	RealTimeSource struct{}

	// EventTimeSource serves fake controlled time
	EventTimeSource struct {
		now time.Time
	}
)

// NewRealTimeSource returns a time source that servers
// real wall clock time
func NewRealTimeSource() *RealTimeSource {
	return &RealTimeSource{}
}

// Now return the real current time
func (ts *RealTimeSource) Now() time.Time {
	return time.Now()
}

// NewEventTimeSource returns a time source that servers
// fake controlled time
func NewEventTimeSource() *EventTimeSource {
	return &EventTimeSource{}
}

// Now return the fake current time
func (ts *EventTimeSource) Now() time.Time {
	return ts.now
}

// Update update the fake current time
func (ts *EventTimeSource) Update(now time.Time) *EventTimeSource {
	ts.now = now
	return ts
}
