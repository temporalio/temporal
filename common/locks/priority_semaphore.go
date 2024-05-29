package locks

import "context"

type (
	PrioritySemaphore interface {
		Acquire(ctx context.Context, priority Priority, n int) error
		TryAcquire(priority Priority, n int) bool
		Release(priority Priority, n int)
	}
)
