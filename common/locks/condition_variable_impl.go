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
		channel:  newCVChannel(),
	}
}

// Signal wakes one goroutine waiting on this condition variable, if there is any.
func (c *ConditionVariableImpl) Signal() {
	c.chanLock.Lock()
	defer c.chanLock.Unlock()

	select {
	case c.channel <- struct{}{}:
	default:
		// noop
	}
}

// Broadcast wakes all goroutines waiting on this condition variable.
func (c *ConditionVariableImpl) Broadcast() {
	newChannel := newCVChannel()

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

func newCVChannel() chan struct{} {
	return make(chan struct{}, 1)
}
