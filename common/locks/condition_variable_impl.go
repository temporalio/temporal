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

package locks

import (
	"sync"
)

type (
	ConditionVariableImpl struct {
		lock Locker

		chanLock sync.Mutex
		channel  chan struct{}
	}
)

var _ ConditionVariable = (*ConditionVariableImpl)(nil)

func NewConditionVariable(
	lock Locker,
) *ConditionVariableImpl {
	return &ConditionVariableImpl{
		lock: lock,

		chanLock: sync.Mutex{},
		channel:  make(chan struct{}, 1),
	}
}

// Signal wakes one goroutine waiting on this condition variable, if there is any.
func (c *ConditionVariableImpl) Signal() {
	c.chanLock.Lock()
	defer c.chanLock.Unlock()

	select {
	case c.channel <- struct{}{}:
	default:
		//noop
	}
}

// Broadcast wakes all goroutines waiting on this condition variable.
func (c *ConditionVariableImpl) Broadcast() {
	newChannel := make(chan struct{})

	c.chanLock.Lock()
	defer c.chanLock.Unlock()

	close(c.channel)
	c.channel = newChannel
}

// Wait atomically unlocks user provided lock and suspends execution of the calling goroutine.
// After later resuming execution, Wait locks c.L before returning.
// Wait can be awoken by Broadcast, Signal or user provided interrupt channel.
func (c *ConditionVariableImpl) Wait(
	interrupt <-chan struct{},
) {

	c.chanLock.Lock()
	channel := c.channel
	c.chanLock.Unlock()

	// user provided lock must be released after getting the above channel
	// so channel to be waited on < release of user lock < acquire of user lock < signal / broadcast
	// ```
	// That is, if another thread is able to acquire the mutex after the about-to-block
	// thread has released it, then a subsequent call to pthread_cond_signal() or
	// pthread_cond_broadcast() in that thread behaves as if it were issued after the
	// about-to-block thread has blocked.
	// ref: https://pubs.opengroup.org/onlinepubs/007908799/xsh/pthread_cond_wait.html
	// ```

	c.lock.Unlock()
	defer c.lock.Lock()

	select {
	case <-channel:
		// received signal
	case <-interrupt:
		// interrupted
	}
}
