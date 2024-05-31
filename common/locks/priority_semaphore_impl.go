// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	} else {
		s.lowCount += n
		s.totalHeld += n
	}
	return true
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
