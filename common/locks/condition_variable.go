package locks

type (
	// ConditionVariable is the interface for condition variable
	ConditionVariable interface {
		// Signal wakes one goroutine waiting on this condition variable, if there is any.
		Signal()
		// Broadcast wakes all goroutines waiting on this condition variable.
		Broadcast()
		// Wait atomically unlocks user provided lock and suspends execution of the calling goroutine.
		// After later resuming execution, Wait locks c.L before returning.
		// Wait can be awoken by Broadcast, Signal or user provided interrupt channel.
		Wait(interrupt <-chan struct{})
	}
)
