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
	"context"
	"fmt"
	"sync"
)

const (
	PriorityMutexStateUnlocked     PriorityMutexState = 0
	PriorityMutexStateLockedByHigh PriorityMutexState = 1
	PriorityMutexStateLockedByLow  PriorityMutexState = 2
)

type (
	PriorityMutexState int

	PriorityMutexImpl struct {
		locker   sync.Locker
		highCV   ConditionVariable
		lowCV    ConditionVariable
		highWait int
		lowWait  int

		lockState PriorityMutexState
	}
)

var _ PriorityMutex = (*PriorityMutexImpl)(nil)

func NewPriorityMutex() *PriorityMutexImpl {
	lock := &sync.Mutex{}
	syncCV := NewConditionVariable(lock)
	asyncCV := NewConditionVariable(lock)
	return &PriorityMutexImpl{
		locker:   lock,
		highCV:   syncCV,
		lowCV:    asyncCV,
		highWait: 0,
		lowWait:  0,

		lockState: PriorityMutexStateUnlocked,
	}
}

// LockHigh try to lock with high priority, use LockHigh / UnlockHigh pair to lock / unlock
func (c *PriorityMutexImpl) LockHigh(
	ctx context.Context,
) error {
	c.locker.Lock()
	defer c.locker.Unlock()

	c.highWait += 1
	defer func() { c.highWait -= 1 }()

	for c.lockState != PriorityMutexStateUnlocked && ctx.Err() == nil {
		c.highCV.Wait(ctx.Done())
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	c.lockState = PriorityMutexStateLockedByHigh
	return nil
}

// LockLow try to lock with low priority, use LockLow / UnlockLow pair to lock / unlock
func (c *PriorityMutexImpl) LockLow(
	ctx context.Context,
) error {
	c.locker.Lock()
	defer c.locker.Unlock()

	c.lowWait += 1
	defer func() { c.lowWait -= 1 }()

	for c.lockState != PriorityMutexStateUnlocked && ctx.Err() == nil {
		c.lowCV.Wait(ctx.Done())
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	c.lockState = PriorityMutexStateLockedByLow
	return nil
}

func (c *PriorityMutexImpl) TryLockHigh() bool {
	c.locker.Lock()
	defer c.locker.Unlock()

	if c.lockState != PriorityMutexStateUnlocked {
		return false
	}

	c.lockState = PriorityMutexStateLockedByHigh
	return true
}

func (c *PriorityMutexImpl) TryLockLow() bool {
	c.locker.Lock()
	defer c.locker.Unlock()

	if c.lockState != PriorityMutexStateUnlocked {
		return false
	}

	c.lockState = PriorityMutexStateLockedByLow
	return true
}

// UnlockHigh unlock with high priority, use LockHigh / UnlockHigh pair to lock / unlock
func (c *PriorityMutexImpl) UnlockHigh() {
	c.locker.Lock()
	defer c.locker.Unlock()

	if c.lockState != PriorityMutexStateLockedByHigh {
		panic(fmt.Sprintf("unable to unlock high priority, state: %v\n", c.lockState))
	}

	c.lockState = PriorityMutexStateUnlocked
	c.notify()
}

// UnlockLow unlock with low priority, use LockLow / UnlockLow pair to lock / unlock
func (c *PriorityMutexImpl) UnlockLow() {
	c.locker.Lock()
	defer c.locker.Unlock()

	if c.lockState != PriorityMutexStateLockedByLow {
		panic(fmt.Sprintf("unable to unlock high priority, state: %v\n", c.lockState))
	}

	c.lockState = PriorityMutexStateUnlocked
	c.notify()
}

func (c *PriorityMutexImpl) IsLocked() bool {
	c.locker.Lock()
	defer c.locker.Unlock()

	return c.lockState != PriorityMutexStateUnlocked
}

func (c *PriorityMutexImpl) notify() {
	if c.highWait > 0 {
		c.highCV.Signal()
	} else if c.lowWait > 0 {
		c.lowCV.Signal()
	}
}
