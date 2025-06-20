// Package clock provides extensions to the [time] package.
package clock

import (
	"time"
)

type (
	// TimeSource is an interface to make it easier to test code that uses time.
	TimeSource interface {
		Now() time.Time
		Since(t time.Time) time.Duration
		AfterFunc(d time.Duration, f func()) Timer
		NewTimer(d time.Duration) (<-chan time.Time, Timer)
	}
	// Timer is a timer returned by TimeSource.AfterFunc. Unlike the timers returned by [time.NewTimer] or time.Ticker,
	// this timer does not have a channel. That is because the callback already reacts to the timer firing.
	Timer interface {
		// Reset changes the expiration time of the timer. It returns true if the timer had been active, false if the
		// timer had expired or been stopped.
		Reset(d time.Duration) bool
		// Stop prevents the Timer from firing. It returns true if the call stops the timer, false if the timer has
		// already expired or been stopped.
		Stop() bool
	}
	// RealTimeSource is a timeSource that uses the real wall timeSource time. The zero value is valid.
	RealTimeSource struct{}
)

var _ TimeSource = (*RealTimeSource)(nil)

// NewRealTimeSource returns a timeSource that uses the real wall timeSource time.
func NewRealTimeSource() RealTimeSource {
	return RealTimeSource{}
}

// Now returns the current time, with the location set to UTC.
func (ts RealTimeSource) Now() time.Time {
	return time.Now().UTC()
}

// Since returns the time elapsed since t
func (ts RealTimeSource) Since(t time.Time) time.Duration {
	return time.Since(t)
}

// AfterFunc is a pass-through to time.AfterFunc.
func (ts RealTimeSource) AfterFunc(d time.Duration, f func()) Timer {
	return time.AfterFunc(d, f)
}

// NewTimer is a pass-through to time.NewTimer.
func (ts RealTimeSource) NewTimer(d time.Duration) (<-chan time.Time, Timer) {
	t := time.NewTimer(d)
	return t.C, t
}
