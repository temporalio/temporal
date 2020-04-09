package locks

import (
	"context"
	"sync"
	"sync/atomic"
)

type (
	// Mutex accepts a context in its Lock method.
	// It blocks the goroutine until either the lock is acquired or the context
	// is closed.
	Mutex interface {
		Lock(context.Context) error
		Unlock()
	}

	mutexImpl struct {
		sync.Mutex
	}
)

const (
	acquiring = iota
	acquired
	bailed
)

// NewMutex creates a new RWMutex
func NewMutex() Mutex {
	return &mutexImpl{}
}

func (m *mutexImpl) Lock(ctx context.Context) error {
	return m.lockInternal(ctx)
}

func (m *mutexImpl) lockInternal(ctx context.Context) error {
	var state int32 = acquiring

	acquiredCh := make(chan struct{})
	go func() {
		m.Mutex.Lock()
		if !atomic.CompareAndSwapInt32(&state, acquiring, acquired) {
			// already bailed due to context closing
			m.Unlock()
		}

		close(acquiredCh)
	}()

	select {
	case <-acquiredCh:
		return nil
	case <-ctx.Done():
		{
			if !atomic.CompareAndSwapInt32(&state, acquiring, bailed) {
				return nil
			}
			return ctx.Err()
		}
	}
}
