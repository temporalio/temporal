// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package clock provides extensions to the [time] package.
package clock

import (
	"time"
)

type (
	// TimeSource is an interface to make it easier to test code that uses time.
	TimeSource interface {
		Now() time.Time
		AfterFunc(d time.Duration, f func()) Timer
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

// NewRealTimeSource returns a timeSource that uses the real wall timeSource time.
func NewRealTimeSource() RealTimeSource {
	return RealTimeSource{}
}

// Now returns the current time, with the location set to UTC.
func (ts RealTimeSource) Now() time.Time {
	return time.Now().UTC()
}

// AfterFunc is a pass-through to time.AfterFunc.
func (ts RealTimeSource) AfterFunc(d time.Duration, f func()) Timer {
	return time.AfterFunc(d, f)
}
