package locks

import "context"

type (
	PrioritySemaphore interface {
		Acquire(ctx context.Context, priority Priority, n int) error
		TryAcquire(n int) bool
		Release(n int)
	}
)
