package locks

import (
	"context"
	"sync"
)

type Priority int

const (
	PriorityHigh Priority = iota
	PriorityLow
)

type PrioritySemaphoreImpl struct {
	capacity     int
	highWaiting  int         // Current number of high priority requests waiting.
	highCount    int         // Current count of high-priority holds
	lowCount     int         // Current count of low-priority holds
	totalHeld    int         // Total number of holds (high + low)
	mu           *sync.Mutex // Mutex to protect conditional variable and changes to the counts
	highPriority *sync.Cond  // Condition variable for high-priority tasks
	lowPriority  *sync.Cond  // Condition variable for low-priority tasks
}

var _ PrioritySemaphore = (*PrioritySemaphoreImpl)(nil)

// NewPrioritySemaphore creates a new PrioritySemaphoreImpl with specified capacity
func NewPrioritySemaphore(capacity int) PrioritySemaphore {
	mu := sync.Mutex{}
	return &PrioritySemaphoreImpl{
		capacity:     capacity,
		highCount:    0,
		lowCount:     0,
		totalHeld:    0,
		mu:           &mu,
		highPriority: sync.NewCond(&mu),
		lowPriority:  sync.NewCond(&mu),
	}
}

func (s *PrioritySemaphoreImpl) Acquire(ctx context.Context, priority Priority, n int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.totalHeld+n > s.capacity {
		if priority == PriorityHigh {
			s.highWaiting++
			s.highPriority.Wait()
			s.highWaiting--
		} else {
			s.lowPriority.Wait()
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if priority == PriorityHigh {
		s.highCount += n
	} else {
		s.lowCount += n
	}
	s.totalHeld += n
	return nil
}

func (s *PrioritySemaphoreImpl) TryAcquire(priority Priority, n int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.totalHeld+n > s.capacity {
		return false
	}

	if priority == PriorityHigh {
		s.highCount += n
		s.totalHeld += n
		return true
	} else { // Low priority
		s.lowCount += n
		s.totalHeld += n
		return true
	}
}

func (s *PrioritySemaphoreImpl) Release(priority Priority, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if priority == PriorityHigh {
		if n > s.highCount {
			panic("trying to release more resources than acquired")
		}
		s.highCount -= n

	} else {
		if n > s.lowCount {
			panic("trying to release more resources than acquired")
		}
		s.lowCount -= n
	}
	s.totalHeld -= n

	if s.highWaiting != 0 {
		s.highPriority.Signal()
	} else {
		s.lowPriority.Signal()
	}
}
