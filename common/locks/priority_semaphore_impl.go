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
	highWaiting int         // Current number of high priority requests waiting.
	highCount   int         // Current count of high-priority holds
	lowCount    int         // Current count of low-priority holds
	lock        sync.Locker // Mutex to protect conditional variable and changes to the counts
	highCV      *sync.Cond  // Condition variable for high-priority tasks
	lowCV       *sync.Cond  // Condition variable for low-priority tasks
}

var _ PrioritySemaphore = (*PrioritySemaphoreImpl)(nil)

// NewPrioritySemaphore creates a new PrioritySemaphoreImpl with specified capacity
func NewPrioritySemaphore(capacity int) PrioritySemaphore {
	mu := &sync.Mutex{}
	highCV := sync.NewCond(mu)
	lowCV := sync.NewCond(mu)
	return &PrioritySemaphoreImpl{
		capacity:  capacity,
		highCount: 0,
		lowCount:  0,
		lock:      mu,
		highCV:    highCV,
		lowCV:     lowCV,
	}
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
				s.highWaiting--
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

	if s.highWaiting != 0 {
		// If n > 1, more than one caller could potentially make progress.
		for ; n > 0; n-- {
			s.highCV.Signal()
		}
	} else {
		for ; n > 0; n-- {
			s.lowCV.Signal()
		}
	}
}
