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
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
)

// GroupBySchedulerOptions are options for creating a [GroupByScheduler].
type GroupBySchedulerOptions[K comparable, T Task] struct {
	Logger log.Logger
	// A function to determine the group of a task.
	KeyFn func(T) K
	// Factory for creating a runnable from a task.
	RunnableFactory func(T) Runnable
	// When a new group is encountered, use this function to create a scheduler for that group.
	SchedulerFactory func(K) RunnableScheduler
}

var _ Scheduler[Task] = &GroupByScheduler[string, Task]{}

// GroupByScheduler groups tasks based on a provided key function and submits that task for processing on a dedicated
// scheduler for that group.
type GroupByScheduler[K comparable, T Task] struct {
	stopped atomic.Bool
	options GroupBySchedulerOptions[K, T]
	// Synchronizes access to the schedulers map.
	mu         sync.RWMutex
	schedulers map[K]RunnableScheduler
}

// NewGroupByScheduler creates a new [GroupByScheduler] from given options.
func NewGroupByScheduler[K comparable, T Task](options GroupBySchedulerOptions[K, T]) *GroupByScheduler[K, T] {
	return &GroupByScheduler[K, T]{
		options:    options,
		schedulers: make(map[K]RunnableScheduler),
	}
}

func (*GroupByScheduler[K, T]) Start() {
	// noop
}

// Stop signals running tasks to stop, aborts any pending tasks and waits up to a minute for all running tasks to
// complete.
func (s *GroupByScheduler[K, T]) Stop() {
	if !s.stopped.CompareAndSwap(false, true) {
		return
	}
	s.mu.Lock()
	for _, lim := range s.schedulers {
		lim.InitiateShutdown()
	}
	s.mu.Unlock()

	if success := common.BlockWithTimeout(s.waitShutdown, time.Minute); !success {
		s.options.Logger.Warn("GroupByScheduler timed out waiting for groups to complete shutdown")
	} else {
		s.options.Logger.Debug("GroupByScheduler shutdown complete")
	}
}

func (s *GroupByScheduler[K, T]) waitShutdown() {
	for _, lim := range s.schedulers {
		lim.WaitShutdown()
	}
}

func (s *GroupByScheduler[K, T]) Submit(task T) {
	if !s.TrySubmit(task) {
		task.Reschedule()
	}
}

// TrySubmit submits a task for processing. If called after the scheduler is shut down, the task will be accepted and
// aborted.
func (s *GroupByScheduler[K, T]) TrySubmit(task T) bool {
	if s.stopped.Load() {
		// No need to reschedule this task, just abort after we've shut down.
		task.Abort()
		return true
	}
	key := s.options.KeyFn(task)
	sched := s.getOrCreateScheduler(key)
	return sched.TrySubmit(s.options.RunnableFactory(task))
}

// getOrCreateSchedulerForTask gets an existing scheduler for the given key or creates one if needed.
func (s *GroupByScheduler[K, T]) getOrCreateScheduler(key K) RunnableScheduler {
	s.mu.RLock()
	sched, ok := s.schedulers[key]
	s.mu.RUnlock()
	if !ok {
		s.mu.Lock()
		// Check again in case the map was populated between releasing and aquiring the lock.
		if sched, ok = s.schedulers[key]; !ok {
			sched = s.options.SchedulerFactory(key)
			s.schedulers[key] = sched
		}
		s.mu.Unlock()
	}
	return sched
}
