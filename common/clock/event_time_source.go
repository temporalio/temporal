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

package clock

import (
	"sync"
	"time"
)

type (
	// EventTimeSource is a fake TimeSource. Unlike other fake clock implementations, the methods are synchronous, so
	// when you call Advance or Update, all triggered timers from AfterFunc will fire before the method returns, in the
	// same goroutine.
	EventTimeSource struct {
		mu     sync.RWMutex
		now    time.Time
		timers []*fakeTimer
	}

	// fakeTimer is a fake implementation of [Timer].
	fakeTimer struct {
		// need a link to the parent timeSource for synchronization
		timeSource *EventTimeSource
		// deadline for when the timer should fire
		deadline time.Time
		// callback to call when the timer fires
		callback func()
		// done is true if the timer has fired or been stopped
		done bool
		// index of the timer in the parent timeSource
		index int
	}
)

// NewEventTimeSource returns a EventTimeSource with the current time set to Unix zero: 1970-01-01 00:00:00 +0000 UTC.
func NewEventTimeSource() *EventTimeSource {
	return &EventTimeSource{
		now: time.Unix(0, 0),
	}
}

// Now return the current time.
func (ts *EventTimeSource) Now() time.Time {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return ts.now
}

// AfterFunc return a timer that will fire after the specified duration. It is important to note that the timeSource is
// locked while the callback is called. This means that you must be cautious about calling any other mutating methods on
// the timeSource from within the callback. Doing so will probably result in a deadlock. To avoid this, you may want to
// wrap all such calls in a goroutine. If the duration is non-positive, the callback will fire immediately before
// AfterFunc returns.
func (ts *EventTimeSource) AfterFunc(d time.Duration, f func()) Timer {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if d < 0 {
		d = 0
	}
	t := &fakeTimer{timeSource: ts, deadline: ts.now.Add(d), callback: f}
	t.index = len(ts.timers)
	ts.timers = append(ts.timers, t)
	ts.fireTimers()

	return t
}

// Update the fake current time. It returns the timeSource so that you can chain calls like this:
// timeSource := NewEventTimeSource().Update(time.Now())
func (ts *EventTimeSource) Update(now time.Time) *EventTimeSource {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.now = now
	ts.fireTimers()
	return ts
}

// Advance the timer by the specified duration.
func (ts *EventTimeSource) Advance(d time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.now = ts.now.Add(d)
	ts.fireTimers()
}

// fireTimers fires all timers that are ready.
func (ts *EventTimeSource) fireTimers() {
	n := 0
	for _, t := range ts.timers {
		if t.deadline.After(ts.now) {
			ts.timers[n] = t
			t.index = n
			n++
		} else {
			t.callback()
			t.done = true
		}
	}
	ts.timers = ts.timers[:n]
}

// Reset the timer to fire after the specified duration. Returns true if the timer was active.
func (t *fakeTimer) Reset(d time.Duration) bool {
	t.timeSource.mu.Lock()
	defer t.timeSource.mu.Unlock()

	if d < 0 {
		d = 0
	}

	wasActive := !t.done
	t.deadline = t.timeSource.now.Add(d)
	if t.done {
		t.done = false
		t.index = len(t.timeSource.timers)
		t.timeSource.timers = append(t.timeSource.timers, t)
	}
	t.timeSource.fireTimers()
	return wasActive
}

// Stop the timer. Returns true if the timer was active.
func (t *fakeTimer) Stop() bool {
	t.timeSource.mu.Lock()
	defer t.timeSource.mu.Unlock()

	if t.done {
		return false
	}

	i := t.index
	timers := t.timeSource.timers

	timers[i] = timers[len(timers)-1] // swap with last timer
	timers[i].index = i               // update index of swapped timer
	timers = timers[:len(timers)-1]   // shrink list

	t.timeSource.timers = timers
	t.done = true // ensure that the timer is not reused

	return true
}
