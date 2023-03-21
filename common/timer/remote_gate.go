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
	"sync"
	"time"
)

type (
	RemoteGate interface {
		Gate
		// SetCurrentTime set the current time, and additionally fire the fire chan
		// if new "current" time is after the next wakeup time, return true if
		// "current" is actually updated
		SetCurrentTime(nextTime time.Time) bool
	}

	RemoteGateImpl struct {
		// the channel which will be used to proxy the fired timer
		fireCh chan struct{}

		// lock for timer and next wakeup time
		sync.Mutex
		// view of current time
		currentTime time.Time
		// time which timer will fire
		nextWakeupTime time.Time
	}
)

// NewRemoteGate create a new timer gate instance
func NewRemoteGate() RemoteGate {
	rg := &RemoteGateImpl{
		currentTime:    time.Time{},
		nextWakeupTime: time.Time{},
		fireCh:         make(chan struct{}, 1),
	}
	return rg
}

// FireCh return the channel which will be fired when time is up
func (rg *RemoteGateImpl) FireCh() <-chan struct{} {
	return rg.fireCh
}

// FireAfter check will the timer get fired after a certain time
func (rg *RemoteGateImpl) FireAfter(now time.Time) bool {
	rg.Lock()
	defer rg.Unlock()

	active := rg.currentTime.Before(rg.nextWakeupTime)
	return active && rg.nextWakeupTime.After(now)
}

// Update the timer gate, return true if update is a success.
// Success means timer is idle or timer is set with a sooner time to fire
func (rg *RemoteGateImpl) Update(nextTime time.Time) bool {
	rg.Lock()
	defer rg.Unlock()

	active := rg.currentTime.Before(rg.nextWakeupTime)
	if active {
		if rg.nextWakeupTime.Before(nextTime) {
			// current time < next wake up time < next time
			return false
		}

		if rg.currentTime.Before(nextTime) {
			// current time < next time <= next wake-up time
			rg.nextWakeupTime = nextTime
			return true
		}

		// next time <= current time < next wake-up time
		rg.nextWakeupTime = nextTime
		rg.fire()
		return true
	}

	// this means the timer, before stopped, has already fired / never active
	if !rg.currentTime.Before(nextTime) {
		// next time is <= current time, need to fire immediately
		// whether to update next wake-up time or not is irrelevant
		rg.fire()
	} else {
		// next time > current time
		rg.nextWakeupTime = nextTime
	}
	return true
}

// Close shutdown the timer
func (rg *RemoteGateImpl) Close() {
	// no op
}

// SetCurrentTime set the current time, and additionally fire the fire chan
// if new "current" time is after the next wake-up time, return true if
// "current" is actually updated
func (rg *RemoteGateImpl) SetCurrentTime(currentTime time.Time) bool {
	rg.Lock()
	defer rg.Unlock()

	if !rg.currentTime.Before(currentTime) {
		// new current time is <= current time
		return false
	}

	// NOTE: do not update the current time now
	if !rg.currentTime.Before(rg.nextWakeupTime) {
		// current time already >= next wakeup time
		// avoid duplicate fire
		rg.currentTime = currentTime
		return true
	}

	rg.currentTime = currentTime
	if !rg.currentTime.Before(rg.nextWakeupTime) {
		rg.fire()
	}
	return true
}

func (rg *RemoteGateImpl) fire() {
	select {
	case rg.fireCh <- struct{}{}:
		// timer successfully triggered
	default:
		// timer already triggered, pass
	}
}
