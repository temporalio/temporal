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
