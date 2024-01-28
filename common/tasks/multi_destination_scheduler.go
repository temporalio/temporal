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

package tasks

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
)

type RateLimiter interface {
	Wait(context.Context) error
}

// LimiterOptions are options for a single destination's limiter.
type LimiterOptions struct {
	// Concurrency returns the concurrency limit.
	Concurrency func() int
	RateLimiter RateLimiter
}

// limiter acts a concurrency and rate limiter for a single destination.
// All access to the limiter must be locked.
type limiter[T Task] struct {
	sync.Mutex
	options LimiterOptions
	// Number of reservations help by this limiter. Restricts concurrency.
	reservations int
	// Any tasks that exceed the concurrency limitation are buffered in this slice.
	buffer []T
}

func newLimiter[T Task](options LimiterOptions) *limiter[T] {
	return &limiter[T]{
		options: options,
	}
}

// enqueue a task in the limiter's buffer.
func (lim *limiter[T]) enqueue(task T) {
	lim.buffer = append(lim.buffer, task)
}

// dequeue a task from the limiter's buffer.
func (lim *limiter[T]) dequeue() (task T, ok bool) {
	if len(lim.buffer) > 0 {
		task, lim.buffer = lim.buffer[0], lim.buffer[1:]
		return task, true
	}
	return task, false
}

// reserve a concurrency slot. Returns true on success.
func (lim *limiter[T]) reserve() bool {
	if lim.reservations < lim.options.Concurrency() {
		lim.reservations++
		return true
	}
	return false
}

// release a reservation.
func (lim *limiter[T]) release() {
	lim.reservations--
}

// MultiDestinationSchedulerOptions are options for creating a [MultiDestinationScheduler].
type MultiDestinationSchedulerOptions[K comparable, T Task] struct {
	// A function to determine the destination of a task.
	KeyFn func(T) K
	// Limiter options for a given destination.
	LimiterOptionsFn func(K) LimiterOptions
	Logger           log.Logger
}

// MultDestinationScheduler for limiting concurrency and RPS (and later applying a circuit breaker) on a per destination
// basis. A task's destination is determined based on a provided key function.
// Accepts and buffers tasks that exceed the destination's concurrency limit to be picked up as soon as there's more
// availability.
type MultiDestinationScheduler[K comparable, T Task] struct {
	options      MultiDestinationSchedulerOptions[K, T]
	waitGroup    sync.WaitGroup
	stopCtx      context.Context
	stopFn       context.CancelFunc
	limitersLock sync.RWMutex
	limiters     map[K]*limiter[T]
}

func NewMultiDestinationScheduler[K comparable, T Task](options MultiDestinationSchedulerOptions[K, T]) *MultiDestinationScheduler[K, T] {
	stopCtx, stopFn := context.WithCancel(context.Background())
	return &MultiDestinationScheduler[K, T]{
		options:  options,
		stopCtx:  stopCtx,
		stopFn:   stopFn,
		limiters: make(map[K]*limiter[T]),
	}
}

func (*MultiDestinationScheduler[K, T]) Start() {
	// noop
}

// Stop signals running tasks to stop, aborts any pending tasks and waits up to a minute for all running tasks to
// complete.
func (s *MultiDestinationScheduler[K, T]) Stop() {
	s.stopFn()
	s.abortBufferedTasks()
	if success := common.AwaitWaitGroup(&s.waitGroup, time.Minute); !success {
		s.options.Logger.Warn("MultiDestinationScheduler timed out waiting for tasks to complete")
	} else {
		s.options.Logger.Debug("MultiDestinationScheduler drained")
	}
}

func (s *MultiDestinationScheduler[K, T]) abortBufferedTasks() {
	s.limitersLock.Lock()
	defer s.limitersLock.Unlock()

	for _, lim := range s.limiters {
		lim.Lock()
		for _, task := range lim.buffer {
			task.Abort()
		}
		lim.buffer = nil
		lim.Unlock()
	}
}

// Submit is not implemented as it is never invoked.
func (*MultiDestinationScheduler[K, T]) Submit(T) {
	panic("unimplemented")
}

// TrySubmit submits a task for processing unless the scheduler is shut down.
func (s *MultiDestinationScheduler[K, T]) TrySubmit(task T) bool {
	// Add to the wait group before checking if we're stopped to prevent Stop() from returning early if TrySubmit is
	// called concurrently.
	s.waitGroup.Add(1)
	if s.stopCtx.Err() != nil {
		s.waitGroup.Done()
		return false
	}

	if lim := s.limiterForTask(task); lim != nil {
		go func() {
			defer s.waitGroup.Done()
			s.executeUntilReleased(task, lim)
		}()
	} else {
		s.waitGroup.Done()
	}
	return true
}

// limiterForTask gets or creates a limiter for the task's destination.
// If the limiter has hit the concurrency limit, returns nil and buffers the task to be processed later.
func (s *MultiDestinationScheduler[K, T]) limiterForTask(task T) *limiter[T] {
	key := s.options.KeyFn(task)
	s.limitersLock.RLock()
	lim, ok := s.limiters[key]
	s.limitersLock.RUnlock()
	if !ok {
		s.limitersLock.Lock()
		// Check again in case the limiter map was populated between releasing and aquiring the lock.
		if lim, ok = s.limiters[key]; !ok {
			lim = newLimiter[T](s.options.LimiterOptionsFn(key))
			s.limiters[key] = lim
		}
		s.limitersLock.Unlock()
	}

	lim.Lock()
	defer lim.Unlock()
	if !lim.reserve() {
		lim.enqueue(task)
		return nil
	}
	return lim
}

// executeUntilReleased execute tasks starting from the given task using the provided limiter.
// Continues as long as it has tasks to dequeue.
func (s *MultiDestinationScheduler[K, T]) executeUntilReleased(task T, lim *limiter[T]) {
	for {
		if err := lim.options.RateLimiter.Wait(s.stopCtx); err != nil {
			// Abort because we're stopped.
			task.Abort()
			lim.Lock()
			lim.release()
			lim.Unlock()
			break
		}
		// TODO: add circuit breaker

		if err := task.HandleErr(task.Execute()); err != nil {
			if s.stopCtx.Err() != nil {
				// Abort because we're stopped.
				task.Abort()
			} else {
				task.Nack(err)
			}
		} else {
			task.Ack()
		}

		lim.Lock()
		nextTask, ok := lim.dequeue()
		if !ok {
			lim.release()
			lim.Unlock()
			break
		}
		task = nextTask
		lim.Unlock()
	}
}

var _ Scheduler[Task] = &MultiDestinationScheduler[string, Task]{}
