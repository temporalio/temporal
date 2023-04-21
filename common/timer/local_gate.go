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

package timer

import (
	"time"

	"go.temporal.io/server/common/clock"
)

type (
	LocalGate interface {
		Gate
	}

	LocalGateImpl struct {
		// the channel which will be used to proxy the fired timer
		fireCh  chan struct{}
		closeCh chan struct{}

		timeSource clock.TimeSource

		// the actual timer which will fire
		timer *time.Timer
		// variable indicating when the above timer will fire
		nextWakeupTime time.Time
	}
)

// NewLocalGate create a new timer gate instance
func NewLocalGate(timeSource clock.TimeSource) LocalGate {
	lg := &LocalGateImpl{
		timer:          time.NewTimer(0),
		nextWakeupTime: time.Time{},
		fireCh:         make(chan struct{}, 1),
		closeCh:        make(chan struct{}),
		timeSource:     timeSource,
	}
	// the timer should be stopped when initialized
	if !lg.timer.Stop() {
		// drain the existing signal if exist
		<-lg.timer.C
	}

	go func() {
		defer close(lg.fireCh)
		defer lg.timer.Stop()
	loop:
		for {
			select {
			case <-lg.timer.C:
				select {
				// re-transmit on gateC
				case lg.fireCh <- struct{}{}:
				default:
				}

			case <-lg.closeCh:
				// closed; cleanup and quit
				break loop
			}
		}
	}()

	return lg
}

// FireCh return the channel which will be fired when time is up
func (lg *LocalGateImpl) FireCh() <-chan struct{} {
	return lg.fireCh
}

// FireAfter check will the timer get fired after a certain time
func (lg *LocalGateImpl) FireAfter(now time.Time) bool {
	return lg.nextWakeupTime.After(now)
}

// Update the timer gate, return true if update is a success.
// Success means timer is idle or timer is set with a sooner time to fire
func (lg *LocalGateImpl) Update(nextTime time.Time) bool {
	// NOTE: negative duration will make the timer fire immediately
	now := lg.timeSource.Now()

	if lg.timer.Stop() && lg.nextWakeupTime.Before(nextTime) {
		// this means the timer, before stopped, is active && next wake-up time do not have to be updated
		lg.timer.Reset(lg.nextWakeupTime.Sub(now))
		return false
	}

	// this means the timer, before stopped, is active && next wake-up time has to be updated
	// or this means the timer, before stopped, is already fired / never active
	lg.nextWakeupTime = nextTime
	lg.timer.Reset(nextTime.Sub(now))
	// Notifies caller that next notification is reset to fire at passed in 'next' visibility time
	return true
}

// Close shutdown the timer
func (lg *LocalGateImpl) Close() {
	close(lg.closeCh)
}
