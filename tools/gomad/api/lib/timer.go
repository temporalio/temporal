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

package lib

import (
	"time"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIM "go.temporal.io/server/tools/gomad/runtime"
)

// The Timer type represents a single event.
// When the Timer expires, the current time will be sent on C, unless the Timer was created by AfterFunc.
// A Timer must be created with NewTimer or AfterFunc.
type Timer struct {
	C       chan time.Time
	f       func()
	firesAt time.Time
	stopped bool
}

// NewTimer creates a new Timer that will send the current time on its channel after at least duration d.
func NewTimer(d time.Duration) *Timer {
	t := &Timer{C: make(chan time.Time, 1)}
	t.start(d)
	return t
}

func (t *Timer) start(d time.Duration) {
	t.firesAt = Now().Add(d)

	firesAt := t.firesAt // copy for the closure
	SIMLANG.InternalGo(func() {
		// the timer can be expired immediately if the duration is zero/negative
		if !t.expired() {
			SIM.Sleep(d) // wait until triggered

			if t.stopped {
				SIM.Dbg("⏱️❌", "timer was stopped")
				return
			}

			if firesAt != t.firesAt {
				SIM.Dbg("⏱️❌", "timer was reset")
				return
			}
		}

		SIM.Dbg("⏱️⚡", "timer fires")

		if t.f != nil {
			t.f()
		} else {
			SIMLANG.SndChan(t.C).Snd(Now())
		}
	})
}

// Stop prevents the Timer from firing.
// It returns true if the call stops the timer, false if the timer has already expired or been stopped.
// Stop does not close the channel, to prevent a read from the channel succeeding incorrectly.
// To ensure the channel is empty after a call to Stop, check the return value and drain the channel.
func (t *Timer) Stop() bool {
	SIM.Dbg("⏱️🛑", "timer stop")
	if t.stopped || t.expired() {
		return false
	}
	t.stopped = true
	return true
}

// Reset changes the timer to expire after duration d.
// It returns true if the timer had been active, false if the timer had expired or been stopped.
// For a Timer created with NewTimer, Reset should be invoked only on stopped or expired timers with drained channels.
//
// If a program has already received a value from t.C, the timer is known to have expired and the channel drained,
// so t.Reset can be used directly. If a program has not yet received a value from t.C, however,
// the timer must be stopped and—if Stop reports that the timer expired before being stopped—the channel explicitly drained.
//
// Note that it is not possible to use Reset's return value correctly, as there is a race condition
// between draining the channel and the new timer expiring. Reset should always be invoked
// on stopped or expired channels, as described above. The return value exists to preserve compatibility with existing programs.
//
// For a Timer created with AfterFunc(d, f), Reset either reschedules when f will run, in which case Reset returns true,
// or schedules f to run again, in which case it returns false. When Reset returns false,
// Reset neither waits for the prior f to complete before returning nor does it guarantee that the subsequent
// goroutine running f does not run concurrently with the prior one. If the caller needs to know whether the
// prior execution of f is completed, it must coordinate with f explicitly.
//
// TODO: introduce some jitter to simulate overlapping function calls
func (t *Timer) Reset(d time.Duration) bool {
	SIM.Dbg("⏱️🔃", "timer reset")

	if t.stopped || t.expired() {
		if t.f != nil {
			t.stopped = false
			t.start(d) // will trigger `f` *again*
		}
		return false
	}

	// create a new timer (previous one will abort once it wakes up)
	t.start(d)

	return true
}

func (t *Timer) expired() bool {
	return t.firesAt.Compare(Now()) != 1
}

// AfterFunc waits for the duration to elapse and then calls f in its own goroutine.
// It returns a Timer that can be used to cancel the call using its Stop method.
// The returned Timer's C field is not used and will be nil.
func AfterFunc(d time.Duration, f func()) *Timer {
	t := &Timer{f: f}
	t.start(d)
	return t
}
