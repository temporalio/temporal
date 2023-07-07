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

package matchingtest

import (
	"sync"
	"time"
)

type (
	// synchronousTimerFactory keeps a list of timers and fires them synchronously when Advance is called.
	synchronousTimerFactory struct {
		sync.Mutex
		// A list of timers which have been created by this clock and have not yet been fired or stopped.
		timers []*SynchronousTimer
	}
	// SynchronousTimer is a timer with a callback. It holds a reference to the clock that created it, so that it can
	// be stopped or reset.
	SynchronousTimer struct {
		// The remaining time on the timer. This is updated when the timer is reset.
		remaining time.Duration
		// The callback to be called when the timer fires.
		callback func()
		// A reference to the parent clock which created this timer.
		clock *synchronousTimerFactory
		// The index of this timer in the parent clock's list of timers.
		index int
		// A timer is orphaned if it has been stopped or fired. This is used to prevent Stop from panicking if it is
		// called multiple times.
		orphaned bool
	}
)

// NewSynchronousTimerFactory returns a synchronousTimerFactory, which is a fake clock that can be manually advanced.
// The advantage over clockwork.FakeClock is that its AfterFunc returns a timer which will be fired synchronously after
// Advance is called.
func NewSynchronousTimerFactory() *synchronousTimerFactory {
	return &synchronousTimerFactory{}
}

// AfterFunc returns a timer which will be fired synchronously after Advance is called. The callback runs while the lock
// is held, so it should not call any methods on the clock.
func (c *synchronousTimerFactory) AfterFunc(d time.Duration, f func()) *SynchronousTimer {
	c.Lock()
	defer c.Unlock()
	index := len(c.timers)
	t := &SynchronousTimer{
		remaining: d,
		callback:  f,
		clock:     c,
		index:     index,
	}
	c.timers = append(c.timers, t)

	return t
}

// remove removes the timer from the clock. It assumes the lock is already held.
func (c *synchronousTimerFactory) remove(t *SynchronousTimer) {
	c.timers[t.index] = c.timers[len(c.timers)-1]
	c.timers[t.index].index = t.index
	c.timers = c.timers[:len(c.timers)-1]
	t.orphaned = true
}

// Reset resets the timer to the given duration. If the timer was already fired or stopped, this has no effect.
func (t *SynchronousTimer) Reset(d time.Duration) {
	t.clock.Lock()
	defer t.clock.Unlock()

	t.remaining = d
}

// Stop stops the timer. If the timer was already fired or stopped, this has no effect.
func (t *SynchronousTimer) Stop() {
	t.clock.Lock()
	defer t.clock.Unlock()

	if t.orphaned {
		return
	}
	t.clock.remove(t)
}

// Advance advances the clock by the given duration. All timers whose duration has elapsed will have their callbacks
// fired synchronously and removed from the clock.
func (c *synchronousTimerFactory) Advance(d time.Duration) {
	c.Lock()
	defer c.Unlock()

	// Iterate backwards so that we can remove timers while iterating.
	for i := len(c.timers) - 1; i >= 0; i-- {
		t := c.timers[i]
		t.remaining -= d

		if t.remaining > 0 {
			continue
		}

		t.callback()
		c.remove(t)
	}
}
