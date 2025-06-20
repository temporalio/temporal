package locks

import (
	"context"
)

type (
	PriorityMutex interface {
		// LockHigh try to lock with high priority, use LockHigh / UnlockHigh pair to lock / unlock
		LockHigh(context.Context) error
		// LockLow try to lock with low priority, use LockLow / UnlockLow pair to lock / unlock
		LockLow(context.Context) error
		// UnlockHigh unlock with high priority, use LockHigh / UnlockHigh pair to lock / unlock
		UnlockHigh()
		// UnlockLow unlock with low priority, use LockLow / UnlockLow pair to lock / unlock
		UnlockLow()
		// TryLockHigh try to lock with high priority, return true if lock acquired
		// use TryLockHigh / UnlockHigh pair to lock / unlock
		TryLockHigh() bool
		// TryLockLow try to lock with high priority, return true if lock acquired
		// use TryLockLow / UnlockLow pair to lock / unlock
		TryLockLow() bool
		// IsLocked returns true if already locked
		IsLocked() bool
	}
)
