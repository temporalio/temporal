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
		fireChan chan struct{}

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
	timer := &RemoteGateImpl{
		currentTime:    time.Time{},
		nextWakeupTime: time.Time{},
		fireChan:       make(chan struct{}, 1),
	}
	return timer
}

// FireChan return the channel which will be fired when time is up
func (timerGate *RemoteGateImpl) FireChan() <-chan struct{} {
	return timerGate.fireChan
}

// FireAfter check will the timer get fired after a certain time
func (timerGate *RemoteGateImpl) FireAfter(now time.Time) bool {
	timerGate.Lock()
	defer timerGate.Unlock()

	active := timerGate.currentTime.Before(timerGate.nextWakeupTime)
	return active && timerGate.nextWakeupTime.After(now)
}

// Update update the timer gate, return true if update is a success
// success means timer is idle or timer is set with a sooner time to fire
func (timerGate *RemoteGateImpl) Update(nextTime time.Time) bool {
	timerGate.Lock()
	defer timerGate.Unlock()

	active := timerGate.currentTime.Before(timerGate.nextWakeupTime)
	if active {
		if timerGate.nextWakeupTime.Before(nextTime) {
			// current time < next wake up time < next time
			return false
		}

		if timerGate.currentTime.Before(nextTime) {
			// current time < next time <= next wake up time
			timerGate.nextWakeupTime = nextTime
			return true
		}

		// next time <= current time < next wake up time
		timerGate.nextWakeupTime = nextTime
		timerGate.fire()
		return true
	}

	// this means the timer, before stopped, has already fired / never active
	if !timerGate.currentTime.Before(nextTime) {
		// next time is <= current time, need to fire immediately
		// whether to update next wake up time or not is irrelevent
		timerGate.fire()
	} else {
		// next time > current time
		timerGate.nextWakeupTime = nextTime
	}
	return true
}

// Close shutdown the timer
func (timerGate *RemoteGateImpl) Close() {
	// no op
}

// SetCurrentTime set the current time, and additionally fire the fire chan
// if new "current" time is after the next wake up time, return true if
// "current" is actually updated
func (timerGate *RemoteGateImpl) SetCurrentTime(currentTime time.Time) bool {
	timerGate.Lock()
	defer timerGate.Unlock()

	if !timerGate.currentTime.Before(currentTime) {
		// new current time is <= current time
		return false
	}

	// NOTE: do not update the current time now
	if !timerGate.currentTime.Before(timerGate.nextWakeupTime) {
		// current time already >= next wakeup time
		// avoid duplicate fire
		timerGate.currentTime = currentTime
		return true
	}

	timerGate.currentTime = currentTime
	if !timerGate.currentTime.Before(timerGate.nextWakeupTime) {
		timerGate.fire()
	}
	return true
}

func (timerGate *RemoteGateImpl) fire() {
	select {
	case timerGate.fireChan <- struct{}{}:
		// timer successfully triggered
	default:
		// timer already triggered, pass
	}
}
