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
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

var _ Scheduler[Task] = (*ExecutionQueueScheduler[Task])(nil)

type (
	// ExecutionQueueSchedulerOptions contains configuration for the ExecutionQueueScheduler.
	// Both fields are functions so they respond to dynamic config changes at runtime.
	ExecutionQueueSchedulerOptions struct {
		// MaxQueues is the maximum number of concurrent workflow queues.
		// When this limit is reached, TrySubmit will reject new queues.
		MaxQueues func() int
		// QueueTTL is how long an idle queue stays in the map before being swept.
		QueueTTL func() time.Duration
		// QueueConcurrency is the max number of worker goroutines per queue.
		// Defaults to 1 (strictly sequential) if nil or returns <= 0.
		QueueConcurrency func() int
	}

	// QueueKeyFn extracts a workflow key from a task for queue routing.
	QueueKeyFn[T Task] func(T) any

	// taskEntry wraps a task with its submit timestamp for latency tracking.
	taskEntry[T Task] struct {
		task       T
		submitTime time.Time
	}

	// workflowQueue represents a single workflow's task queue.
	// Tasks are stored in a slice protected by the scheduler's mutex.
	// Worker goroutines pop tasks and execute them (up to QueueConcurrency workers).
	workflowQueue[T Task] struct {
		tasks         []taskEntry[T]
		activeWorkers int
		lastActive    time.Time
	}

	// ExecutionQueueScheduler is a scheduler that ensures sequential task execution
	// per workflow using a dedicated goroutine per queue.
	//
	// Key characteristics:
	// - Tasks for the same workflow are executed sequentially
	// - Each workflow gets its own goroutine that processes tasks from a slice
	// - Worker goroutines exit when the queue is empty; a background sweeper
	//   removes idle queue entries after QueueTTL
	// - Maximum number of concurrent queues is capped at MaxQueues
	ExecutionQueueScheduler[T Task] struct {
		status       atomic.Int32
		shutdownChan chan struct{}
		shutdownWG   sync.WaitGroup

		options        *ExecutionQueueSchedulerOptions
		queueKeyFn     QueueKeyFn[T]
		logger         log.Logger
		metricsHandler metrics.Handler
		timeSource     clock.TimeSource

		// mu protects the queues map, sweeperRunning, and all workflowQueue fields.
		// All task submissions and worker pops are serialized through this lock.
		mu             sync.Mutex
		queues         map[any]*workflowQueue[T]
		sweeperRunning bool
	}
)

// NewExecutionQueueScheduler creates a new ExecutionQueueScheduler.
func NewExecutionQueueScheduler[T Task](
	options *ExecutionQueueSchedulerOptions,
	queueKeyFn QueueKeyFn[T],
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
) *ExecutionQueueScheduler[T] {
	s := &ExecutionQueueScheduler[T]{
		shutdownChan:   make(chan struct{}),
		options:        options,
		queueKeyFn:     queueKeyFn,
		logger:         logger,
		metricsHandler: metricsHandler,
		timeSource:     timeSource,
		queues:         make(map[any]*workflowQueue[T]),
	}
	s.status.Store(common.DaemonStatusInitialized)
	return s
}

func (s *ExecutionQueueScheduler[T]) Start() {
	if !s.status.CompareAndSwap(common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	s.logger.Info("workflow queue scheduler started")
}

func (s *ExecutionQueueScheduler[T]) Stop() {
	if !s.status.CompareAndSwap(common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(s.shutdownChan)

	go func() {
		if success := common.AwaitWaitGroup(&s.shutdownWG, time.Minute); !success {
			s.logger.Warn("workflow queue scheduler timed out waiting for goroutines")
		}
	}()

	s.logger.Info("workflow queue scheduler stopped")
}

// HasQueue returns true if a queue exists for the given workflow key.
// This is used by other schedulers to check if they should route tasks here.
func (s *ExecutionQueueScheduler[T]) HasQueue(key any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.queues[key]
	return exists
}

// Submit adds a task to its workflow's queue. Creates a new queue if needed.
func (s *ExecutionQueueScheduler[T]) Submit(task T) {
	if s.isStopped() {
		task.Abort()
		metrics.ExecutionQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
		return
	}

	key := s.queueKeyFn(task)
	entry := taskEntry[T]{task: task, submitTime: s.timeSource.Now()}

	s.mu.Lock()
	if s.isStopped() {
		s.mu.Unlock()
		task.Abort()
		metrics.ExecutionQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
		return
	}
	s.appendTaskLocked(key, entry)
	s.mu.Unlock()
	metrics.ExecutionQueueSchedulerTasksSubmitted.With(s.metricsHandler).Record(1)
}

// TrySubmit adds a task to its workflow's queue without blocking.
// Returns true if the task was accepted, false if at max queue capacity for new queues.
func (s *ExecutionQueueScheduler[T]) TrySubmit(task T) bool {
	if s.isStopped() {
		task.Abort()
		metrics.ExecutionQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
		return true
	}

	key := s.queueKeyFn(task)
	entry := taskEntry[T]{task: task, submitTime: s.timeSource.Now()}

	s.mu.Lock()
	if s.isStopped() {
		s.mu.Unlock()
		task.Abort()
		metrics.ExecutionQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
		return true
	}

	if _, exists := s.queues[key]; !exists && len(s.queues) >= s.options.MaxQueues() {
		s.mu.Unlock()
		metrics.ExecutionQueueSchedulerSubmitRejected.With(s.metricsHandler).Record(1)
		return false
	}
	s.appendTaskLocked(key, entry)
	s.mu.Unlock()
	metrics.ExecutionQueueSchedulerTasksSubmitted.With(s.metricsHandler).Record(1)
	return true
}

// appendTaskLocked appends a task to the queue for the given key, creating the
// queue and spawning a worker if needed. Must be called with s.mu held.
func (s *ExecutionQueueScheduler[T]) appendTaskLocked(key any, entry taskEntry[T]) {
	q, exists := s.queues[key]
	if !exists {
		q = &workflowQueue[T]{}
		s.queues[key] = q
		metrics.ExecutionQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(len(s.queues)))
	}
	q.tasks = append(q.tasks, entry)
	if q.activeWorkers < s.queueConcurrency() {
		q.activeWorkers++
		s.shutdownWG.Add(1)
		go s.runWorker(q)
	}
	if !s.sweeperRunning {
		s.sweeperRunning = true
		s.shutdownWG.Add(1)
		go s.runSweeper()
	}
}

// runWorker is the goroutine that processes tasks for a single workflow queue.
// It pops tasks from the slice under the lock and executes them without the lock.
// When the queue is empty, the worker marks itself inactive and exits.
// A new worker is spawned by Submit/TrySubmit when tasks arrive for an inactive queue.
func (s *ExecutionQueueScheduler[T]) runWorker(q *workflowQueue[T]) {
	defer s.shutdownWG.Done()

	for {
		s.mu.Lock()
		if s.isStopped() {
			// Abort all remaining tasks.
			tasks := q.tasks
			q.tasks = nil
			q.activeWorkers--
			s.mu.Unlock()
			for _, entry := range tasks {
				entry.task.Abort()
			}
			if len(tasks) > 0 {
				metrics.ExecutionQueueSchedulerTasksAborted.With(s.metricsHandler).Record(int64(len(tasks)))
			}
			return
		}
		if len(q.tasks) == 0 {
			q.activeWorkers--
			if q.activeWorkers == 0 {
				q.lastActive = s.timeSource.Now()
			}
			s.mu.Unlock()
			return
		}
		entry := q.tasks[0]
		var zero taskEntry[T]
		q.tasks[0] = zero // clear reference for GC
		q.tasks = q.tasks[1:]
		s.mu.Unlock()

		queueWaitTime := s.timeSource.Now().Sub(entry.submitTime)
		metrics.ExecutionQueueSchedulerQueueWaitTime.With(s.metricsHandler).Record(queueWaitTime)
		s.executeTask(entry.task, entry.submitTime)
	}
}

// runSweeper periodically removes idle queue entries from the map.
func (s *ExecutionQueueScheduler[T]) runSweeper() {
	defer s.shutdownWG.Done()

	ticker := time.NewTicker(s.options.QueueTTL())
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if s.sweepIdleQueues() {
				return
			}
		case <-s.shutdownChan:
			return
		}
	}
}

// sweepIdleQueues removes queues that have been idle for longer than QueueTTL.
// Returns true if no queues remain (sweeper should exit).
func (s *ExecutionQueueScheduler[T]) sweepIdleQueues() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.timeSource.Now()
	swept := false
	for key, q := range s.queues {
		if q.activeWorkers == 0 && len(q.tasks) == 0 && now.Sub(q.lastActive) > s.options.QueueTTL() {
			delete(s.queues, key)
			swept = true
		}
	}
	if swept {
		metrics.ExecutionQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(len(s.queues)))
	}
	if len(s.queues) == 0 {
		s.sweeperRunning = false
		return true
	}
	return false
}

func (s *ExecutionQueueScheduler[T]) executeTask(task T, submitTime time.Time) {
	var panicErr error
	defer log.CapturePanic(s.logger, &panicErr)

	shouldRetry := true

	operation := func() (retErr error) {
		var executePanic error
		defer func() {
			if executePanic != nil {
				retErr = executePanic
				shouldRetry = false
			}
		}()
		defer log.CapturePanic(s.logger, &executePanic)

		if err := task.Execute(); err != nil {
			return task.HandleErr(err)
		}
		return nil
	}

	isRetryable := func(err error) bool {
		return !s.isStopped() && shouldRetry && task.IsRetryableError(err)
	}

	if err := backoff.ThrottleRetry(operation, task.RetryPolicy(), isRetryable); err != nil {
		if s.isStopped() {
			task.Abort()
			metrics.ExecutionQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
			return
		}

		task.Nack(err)
		metrics.ExecutionQueueSchedulerTasksFailed.With(s.metricsHandler).Record(1)
		return
	}

	task.Ack()
	metrics.ExecutionQueueSchedulerTasksCompleted.With(s.metricsHandler).Record(1)

	// Record total task latency (time from submit to completion)
	taskLatency := s.timeSource.Now().Sub(submitTime)
	metrics.ExecutionQueueSchedulerTaskLatency.With(s.metricsHandler).Record(taskLatency)
}

func (s *ExecutionQueueScheduler[T]) queueConcurrency() int {
	if s.options.QueueConcurrency == nil {
		return 1
	}
	if c := s.options.QueueConcurrency(); c > 0 {
		return c
	}
	return 1
}

func (s *ExecutionQueueScheduler[T]) isStopped() bool {
	return s.status.Load() == common.DaemonStatusStopped
}
