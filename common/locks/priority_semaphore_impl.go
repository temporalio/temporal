// Copyright (c) 2009 The Go Authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
// * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package locks

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"go.temporal.io/api/serviceerror"
)

type (
	Priority uint32
	waiter   struct {
		n     int
		ready chan<- struct{} // Closed when semaphore acquired.
	}
	PrioritySemaphoreImpl struct {
		size      int
		cur       int
		mu        sync.Mutex
		waitLists []*list.List // Array of lists for managing different priority levels
	}
)

const (
	PriorityHigh Priority = iota
	PriorityLow
	NumPriorities
)

var (
	ErrRequestTooLarge = serviceerror.NewInternal("request is larger than the size of semaphore")
)

var _ PrioritySemaphore = (*PrioritySemaphoreImpl)(nil)

// NewPrioritySemaphore creates a new semaphore with the given
// maximum combined weight for concurrent access, capable of handling multiple priority levels.
// Most of the logic is taken directly from golang's semaphore.Weighted.
func NewPrioritySemaphore(n int) *PrioritySemaphoreImpl {
	waitLists := make([]*list.List, NumPriorities)
	for i := range waitLists {
		waitLists[i] = list.New()
	}
	return &PrioritySemaphoreImpl{
		size:      n,
		waitLists: waitLists,
	}
}

// Acquire acquires the semaphore with a weight of n and a priority, blocking until resources
// are available or ctx is done. On success, returns nil. On failure, returns
// ctx.Err() and leaves the semaphore unchanged.
func (s *PrioritySemaphoreImpl) Acquire(ctx context.Context, priority Priority, n int) error {
	if priority >= NumPriorities {
		panic(fmt.Sprintf("semaphore: invalid priority %v, priority must be less than %v", priority, NumPriorities))
	}

	done := ctx.Done()

	s.mu.Lock()
	select {
	case <-done:
		// ctx becoming done has "happened before" acquiring the semaphore,
		// whether it became done before the call began or while we were
		// waiting for the mutex. We prefer to fail even if we could acquire
		// the mutex without blocking.
		s.mu.Unlock()
		return ctx.Err()
	default:
	}
	// Check if acquisition can proceed without waiting
	if s.size-s.cur >= n && s.noWaiters() {
		// Since we hold s.mu and haven't synchronized since checking done, if
		// ctx becomes done before we return here, it becoming done must have
		// "happened concurrently" with this call - it cannot "happen before"
		// we return in this branch. So, we're ok to always acquire here.
		s.cur += n
		s.mu.Unlock()
		return nil
	}

	if n > s.size {
		s.mu.Unlock()
		return ErrRequestTooLarge
	}

	ready := make(chan struct{})
	w := waiter{n: n, ready: ready}
	elem := s.waitLists[priority].PushBack(w)
	s.mu.Unlock()

	select {
	case <-done:
		s.mu.Lock()
		select {
		case <-ready:
			// Acquired the semaphore after we were canceled.
			// Pretend we didn't and put the tokens back.
			s.cur -= n
			s.notifyWaiters()
		default:
			isFront := s.waitLists[priority].Front() == elem
			s.waitLists[priority].Remove(elem)
			// If we're at the front and there are extra tokens left, notify other waiters.
			if isFront && s.size > s.cur {
				s.notifyWaiters()
			}
		}
		s.mu.Unlock()
		return ctx.Err()

	case <-ready:
		// Acquired the semaphore. Check that ctx isn't already done.
		// We check the done channel instead of calling ctx.Err because we
		// already have the channel, and ctx.Err is O(n) with the nesting
		// depth of ctx.
		select {
		case <-done:
			s.Release(n)
			return ctx.Err()
		default:
		}
		return nil
	}
}

// TryAcquire acquires the semaphore with a weight of n without blocking.
// On success, returns true. On failure, returns false and leaves the semaphore unchanged.
func (s *PrioritySemaphoreImpl) TryAcquire(n int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.size-s.cur >= n && s.noWaiters() {
		s.cur += n
		return true
	}
	return false
}

func (s *PrioritySemaphoreImpl) Release(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur -= n
	if s.cur < 0 {
		s.mu.Unlock()
		panic("semaphore: released more than held")
	}
	s.notifyWaiters()
}

func (s *PrioritySemaphoreImpl) notifyWaiters() {
	for _, l := range s.waitLists {
		for {
			next := l.Front()
			if next == nil {
				break // No more waiters blocked.
			}

			w, ok := next.Value.(waiter)
			if !ok {
				panic("semaphore: failed to cast waiter")
			}
			if s.size-s.cur < w.n {
				// Not enough tokens for the next waiter.  We could keep going (to try to
				// find a waiter with a smaller request), but under load that could cause
				// starvation for large requests; instead, we leave all remaining waiters
				// blocked. For the same reason, we should not wake lower priority waiters.
				//
				// Consider a semaphore used as a read-write lock, with N tokens, N
				// readers, and one writer.  Each reader can Acquire(1) to obtain a read
				// lock.  The writer can Acquire(N) to obtain a write lock, excluding all
				// of the readers.  If we allow the readers to jump ahead in the queue,
				// the writer will starve — there is always one token available for every
				// reader.
				return
			}

			s.cur += w.n
			l.Remove(next)
			close(w.ready)
		}
	}
}

// noWaiters return true if all waitLists are empty, and false otherwise.
func (s *PrioritySemaphoreImpl) noWaiters() bool {
	for _, l := range s.waitLists {
		if l.Len() > 0 {
			return false
		}
	}
	return true
}
