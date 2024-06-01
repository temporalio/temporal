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
	capacity    int
	highWaiting int        // Current number of high priority requests waiting.
	highCount   int        // Current count of high-priority holds
	lowCount    int        // Current count of low-priority holds
	lock        sync.Mutex // Mutex to protect conditional variable and changes to the counts
	highCV      sync.Cond  // Condition variable for high-priority tasks
	lowCV       sync.Cond  // Condition variable for low-priority tasks
}

var _ PrioritySemaphore = (*PrioritySemaphoreImpl)(nil)

// NewPrioritySemaphore creates a new PrioritySemaphoreImpl with specified capacity
func NewPrioritySemaphore(capacity int) PrioritySemaphore {
	sem := &PrioritySemaphoreImpl{
		capacity:  capacity,
		highCount: 0,
		lowCount:  0,
	}
	sem.highCV.L = &sem.lock
	sem.lowCV.L = &sem.lock
	return sem
}

func (s *PrioritySemaphoreImpl) Acquire(ctx context.Context, priority Priority, n int) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	for s.lowCount+s.highCount+n > s.capacity {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if priority == PriorityHigh {
				s.highWaiting++
				s.highCV.Wait()
			} else {
				s.lowCV.Wait()
			}
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
	return nil
}

func (s *PrioritySemaphoreImpl) TryAcquire(priority Priority, n int) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.lowCount+s.highCount+n > s.capacity {
		return false
	}

	if priority == PriorityHigh {
		s.highCount += n
	} else {
		s.lowCount += n
	}
	return true
}

func (s *PrioritySemaphoreImpl) Release(priority Priority, n int) {
	s.lock.Lock()
	defer s.lock.Unlock()

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

	// Wake up threads in highCV. Here n is the number of resources getting released in this call. We might not need
	// all n threads to consume these resources.
	for ; s.highWaiting > 0 && n > 0; s.highWaiting, n = s.highWaiting-1, n-1 {
		s.highCV.Signal()
	}
	// Wake up threads in lowCV if resource is available.
	for ; n > 0; n-- {
		s.lowCV.Signal()
	}
}
