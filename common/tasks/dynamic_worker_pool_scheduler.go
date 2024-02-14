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
	"container/list"
	"context"
	"sync"
)

// DynamicWorkerPoolLimiter provides dynamic limiters for [DynamicWorkerPoolScheduler].
type DynamicWorkerPoolLimiter interface {
	// Dynamic concurrency limiter. Evaluated at submit time.
	Concurrency() int
	// Dynamic buffer size limiter. Evaluated at submit time.
	BufferSize() int
}

// DynamicWorkerPoolScheduler manages a pool of worker goroutines to execute [Runnable] instances.
// It limits the number of concurrently running workers and buffers tasks when that limit is reached.
// It limits the buffer size and rejects tasks when that limit is reached.
// New workers are created on-demand. Workers check for more tasks in the buffer after completing a task.
// If no tasks are available, the worker stops. The pool can be stopped, which aborts all buffered tasks.
type DynamicWorkerPoolScheduler struct {
	wg sync.WaitGroup

	stopFn  context.CancelFunc
	stopCtx context.Context

	limiter DynamicWorkerPoolLimiter

	// Protect access to fields below.
	mu sync.Mutex
	// Number of runningGoroutines held by this worker pool.
	runningGoroutines int
	// Tasks that exceed the concurrency limit are buffered here.
	buffer *list.List
}

// NewDynamicWorkerPoolScheduler creates a [DynamicWorkerPoolScheduler] with the given limiter.
func NewDynamicWorkerPoolScheduler(limiter DynamicWorkerPoolLimiter) *DynamicWorkerPoolScheduler {
	stopCtx, stopFn := context.WithCancel(context.Background())
	return &DynamicWorkerPoolScheduler{
		stopCtx: stopCtx,
		stopFn:  stopFn,
		limiter: limiter,
		buffer:  list.New(),
	}
}

// InitiateShutdown aborts all buffered tasks and empties the buffer.
func (pool *DynamicWorkerPoolScheduler) InitiateShutdown() {
	pool.stopFn()
	pool.mu.Lock()
	// Prevent any running goroutines from picking up already aborted runnables.
	buffer := pool.buffer
	pool.buffer = list.New()
	pool.mu.Unlock()
	for elem := buffer.Front(); elem != nil; elem = elem.Next() {
		elem.Value.(Runnable).Abort()
	}
}

// WaitShutdown waits for all worker goroutines to complete.
func (pool *DynamicWorkerPoolScheduler) WaitShutdown() {
	pool.wg.Wait()
}

func (pool *DynamicWorkerPoolScheduler) TrySubmit(task Runnable) bool {
	// First add to the waitgroup, then check stopCtx.Err to ensure InitiateShutdown() (which first cancels the stopCtx,
	// then waits for the waitgroup) always gets a chance to wait for submitted tasks, even when TrySubmit() and Stop()
	// are called concurrently.
	pool.wg.Add(1)
	if pool.stopCtx.Err() != nil {
		// No need to reschedule this task, just abort after we've shut down.
		pool.wg.Done()
		task.Abort()
		return true
	}
	pool.mu.Lock()
	if pool.runningGoroutines >= pool.limiter.Concurrency() {
		enqueued := pool.tryEnqueueLocked(task)
		pool.mu.Unlock()
		pool.wg.Done()
		return enqueued
	} else {
		pool.runningGoroutines++
		pool.mu.Unlock()
		go pool.executeUntilBufferEmpty(task)
	}
	return true
}

// executeUntilBufferEmpty execute tasks starting from the given task using the provided limiter.
// Continues as long as it has tasks to dequeue.
func (pool *DynamicWorkerPoolScheduler) executeUntilBufferEmpty(task Runnable) {
	defer pool.wg.Done()
	for {
		task.Run(pool.stopCtx)

		pool.mu.Lock()
		nextTask, ok := pool.dequeueLocked()
		if !ok {
			pool.runningGoroutines--
			pool.mu.Unlock()
			break
		}
		task = nextTask
		pool.mu.Unlock()
	}
}

// dequeueLocked dequeues a task from the pool's buffer. Must be called while holding the lock.
func (pool *DynamicWorkerPoolScheduler) dequeueLocked() (task Runnable, ok bool) {
	if elem := pool.buffer.Front(); elem != nil {
		task := elem.Value.(Runnable)
		pool.buffer.Remove(elem)
		return task, true
	}
	return task, false
}

// tryEnqueueLocked enqueues a task into the pool's buffer if it is under capacity.
// Returns true if the task was enqueued, otherwise returns false.
// Must be called while holding the lock.
func (pool *DynamicWorkerPoolScheduler) tryEnqueueLocked(task Runnable) bool {
	if pool.buffer.Len() < pool.limiter.BufferSize() {
		pool.buffer.PushBack(task)
		return true
	}
	return false
}
