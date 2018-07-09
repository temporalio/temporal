// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	"sync"
	"time"
)

type (
	// TimerGate interface
	TimerGate interface {
		// FireChan return the channel which will be fired when time is up
		FireChan() <-chan struct{}
		// FireAfter check will the timer get fired after a certain time
		FireAfter(now time.Time) bool
		// Update update the timer gate, return true if update is a success
		// success means timer is idle or timer is set with a sooner time to fire
		Update(nextTime time.Time) bool
	}

	// LocalTimerGate interface
	LocalTimerGate interface {
		TimerGate
		// Close shutdown the timer
		Close()
	}

	// LocalTimerGateImpl is an timer implementation,
	// which basically is an wrapper of golang's timer and
	// additional feature
	LocalTimerGateImpl struct {
		// the channel which will be used to proxy the fired timer
		fireChan  chan struct{}
		closeChan chan struct{}

		// the actual timer which will fire
		timer *time.Timer
		// variable indicating when the above timer will fire
		nextWakeupTime time.Time
	}

	// RemoteTimerGate interface
	RemoteTimerGate interface {
		TimerGate
		// SetCurrentTime set the current time, and additionally fire the fire chan
		// if new "current" time is after the next wake up time, return true if
		// "current" is antually updated
		SetCurrentTime(nextTime time.Time) bool
	}

	// RemoteTimerGateImpl is an timer implementation,
	// which basically is an wrapper of golang's timer and
	// additional feature
	RemoteTimerGateImpl struct {
		// the channel which will be used to proxy the fired timer
		fireChan chan struct{}

		// lock for timer and next wake up time
		sync.Mutex
		// view of current time
		currentTime time.Time
		// time which timer will fire
		nextWakeupTime time.Time
	}
)

// NewLocalTimerGate create a new timer gate instance
func NewLocalTimerGate() LocalTimerGate {
	timer := &LocalTimerGateImpl{
		timer:          time.NewTimer(0),
		nextWakeupTime: time.Time{},
		fireChan:       make(chan struct{}),
		closeChan:      make(chan struct{}),
	}
	// the timer should be stopped when initialized
	if !timer.timer.Stop() {
		// drain the existing signal if exist
		<-timer.timer.C
	}

	go func() {
		defer close(timer.fireChan)
		defer timer.timer.Stop()
	loop:
		for {
			select {
			case <-timer.timer.C:
				// re-transmit on gateC
				timer.fireChan <- struct{}{}

			case <-timer.closeChan:
				// closed; cleanup and quit
				break loop
			}
		}
	}()

	return timer
}

// FireChan return the channel which will be fired when time is up
func (timerGate *LocalTimerGateImpl) FireChan() <-chan struct{} {
	return timerGate.fireChan
}

// FireAfter check will the timer get fired after a certain time
func (timerGate *LocalTimerGateImpl) FireAfter(now time.Time) bool {
	return timerGate.nextWakeupTime.After(now)
}

// Update update the timer gate, return true if update is a success
// success means timer is idle or timer is set with a sooner time to fire
func (timerGate *LocalTimerGateImpl) Update(nextTime time.Time) bool {
	// NOTE: negative duration will make the timer fire immediately
	now := time.Now()

	if timerGate.timer.Stop() && timerGate.nextWakeupTime.Before(nextTime) {
		// this means the timer, before stopped, is active && next wake up time do not have to be updated
		timerGate.timer.Reset(timerGate.nextWakeupTime.Sub(now))
		return false
	}

	// this means the timer, before stopped, is active && next wake up time has to be updated
	// or this means the timer, before stopped, is already fired / never active
	timerGate.nextWakeupTime = nextTime
	timerGate.timer.Reset(nextTime.Sub(now))
	// Notifies caller that next notification is reset to fire at passed in 'next' visibility time
	return true
}

// Close shutdown the timer
func (timerGate *LocalTimerGateImpl) Close() {
	close(timerGate.closeChan)
}

// NewRemoteTimerGate create a new timer gate instance
func NewRemoteTimerGate() RemoteTimerGate {
	timer := &RemoteTimerGateImpl{
		currentTime:    time.Time{},
		nextWakeupTime: time.Time{},
		fireChan:       make(chan struct{}, 1),
	}
	return timer
}

// FireChan return the channel which will be fired when time is up
func (timerGate *RemoteTimerGateImpl) FireChan() <-chan struct{} {
	return timerGate.fireChan
}

// FireAfter check will the timer get fired after a certain time
func (timerGate *RemoteTimerGateImpl) FireAfter(now time.Time) bool {
	timerGate.Lock()
	defer timerGate.Unlock()

	active := timerGate.currentTime.Before(timerGate.nextWakeupTime)
	return active && timerGate.nextWakeupTime.After(now)
}

// Update update the timer gate, return true if update is a success
// success means timer is idle or timer is set with a sooner time to fire
func (timerGate *RemoteTimerGateImpl) Update(nextTime time.Time) bool {
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

// SetCurrentTime set the current time, and additionally fire the fire chan
// if new "current" time is after the next wake up time, return true if
// "current" is actually updated
func (timerGate *RemoteTimerGateImpl) SetCurrentTime(currentTime time.Time) bool {
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

func (timerGate *RemoteTimerGateImpl) fire() {
	select {
	case timerGate.fireChan <- struct{}{}:
		// timer successfully triggered
	default:
		// timer already triggered, pass
	}
}
