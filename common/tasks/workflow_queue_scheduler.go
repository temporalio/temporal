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

var _ Scheduler[Task] = (*WorkflowQueueScheduler[Task])(nil)

const (
	// defaultQueueTTL is how long a queue goroutine waits for new tasks before exiting.
	// This allows the queue to handle bursts of tasks without constantly creating/destroying goroutines.
	defaultQueueTTL = 5 * time.Second

	// defaultQueueBufferSize is the buffer size for each queue's task channel.
	defaultQueueBufferSize = 100

	// defaultMaxQueues is the maximum number of concurrent workflow queues.
	// This prevents unbounded goroutine growth under high cardinality workloads.
	defaultMaxQueues = 500
)

type (
	// WorkflowQueueSchedulerOptions contains configuration for the WorkflowQueueScheduler.
	WorkflowQueueSchedulerOptions struct {
		// QueueSize is the buffer size for each queue's task channel.
		QueueSize int
		// MaxQueues is the maximum number of concurrent workflow queues.
		// When this limit is reached, TrySubmit will reject new queues.
		MaxQueues int
		// QueueTTL is how long a queue goroutine waits idle before exiting.
		QueueTTL time.Duration
	}

	// QueueKeyFn extracts a workflow key from a task for queue routing.
	QueueKeyFn[T Task] func(T) any

	// taskEntry wraps a task with its submit timestamp for latency tracking.
	taskEntry[T Task] struct {
		task       T
		submitTime time.Time
	}

	// workflowQueue represents a single workflow's task queue with its dedicated goroutine.
	workflowQueue[T Task] struct {
		taskCh chan taskEntry[T]
	}

	// WorkflowQueueScheduler is a scheduler that ensures sequential task execution
	// per workflow using a dedicated goroutine per queue.
	//
	// Key characteristics:
	// - Tasks for the same workflow are executed sequentially
	// - Each workflow gets its own goroutine that processes tasks from a channel
	// - Goroutines self-terminate after being idle for QueueTTL
	// - Maximum number of concurrent queues is capped at MaxQueues
	WorkflowQueueScheduler[T Task] struct {
		status       atomic.Int32
		shutdownChan chan struct{}
		shutdownWG   sync.WaitGroup

		options    *WorkflowQueueSchedulerOptions
		queueKeyFn QueueKeyFn[T]
		logger     log.Logger
		metricsHandler metrics.Handler
		timeSource clock.TimeSource

		// mu protects the queues map
		mu     sync.RWMutex
		queues map[any]*workflowQueue[T]
	}
)

// NewWorkflowQueueScheduler creates a new WorkflowQueueScheduler.
func NewWorkflowQueueScheduler[T Task](
	options *WorkflowQueueSchedulerOptions,
	queueKeyFn QueueKeyFn[T],
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
) *WorkflowQueueScheduler[T] {
	opts := *options
	if opts.QueueSize <= 0 {
		opts.QueueSize = defaultQueueBufferSize
	}
	if opts.MaxQueues <= 0 {
		opts.MaxQueues = defaultMaxQueues
	}
	if opts.QueueTTL <= 0 {
		opts.QueueTTL = defaultQueueTTL
	}

	s := &WorkflowQueueScheduler[T]{
		shutdownChan:   make(chan struct{}),
		options:        &opts,
		queueKeyFn:     queueKeyFn,
		logger:         logger,
		metricsHandler: metricsHandler,
		timeSource:     timeSource,
		queues:         make(map[any]*workflowQueue[T]),
	}
	s.status.Store(common.DaemonStatusInitialized)
	return s
}

func (s *WorkflowQueueScheduler[T]) Start() {
	if !s.status.CompareAndSwap(common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	s.logger.Info("workflow queue scheduler started")
}

func (s *WorkflowQueueScheduler[T]) Stop() {
	if !s.status.CompareAndSwap(common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(s.shutdownChan)

	// Close all queue channels to signal goroutines to drain and exit
	s.mu.Lock()
	for _, q := range s.queues {
		close(q.taskCh)
	}
	s.mu.Unlock()

	// Wait for all queue goroutines to finish
	go func() {
		if success := common.AwaitWaitGroup(&s.shutdownWG, time.Minute); !success {
			s.logger.Warn("workflow queue scheduler timed out waiting for queue goroutines")
		}
	}()

	s.logger.Info("workflow queue scheduler stopped")
}

// HasQueue returns true if a queue exists for the given workflow key.
// This is used by other schedulers to check if they should route tasks here.
func (s *WorkflowQueueScheduler[T]) HasQueue(key any) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.queues[key]
	return exists
}

// Submit adds a task to its workflow's queue. This is a blocking call
// that blocks until the task is accepted or the scheduler stops.
func (s *WorkflowQueueScheduler[T]) Submit(task T) {
	if s.isStopped() {
		task.Abort()
		metrics.WorkflowQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
		return
	}

	key := s.queueKeyFn(task)
	entry := taskEntry[T]{task: task, submitTime: s.timeSource.Now()}

	// Try to get existing queue or create new one
	q, created := s.getOrCreateQueue(key)
	if q == nil {
		// Failed to create queue (at max capacity and TrySubmit semantics)
		// For Submit, we block until we can create a queue
		for q == nil {
			select {
			case <-s.shutdownChan:
				task.Abort()
				metrics.WorkflowQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
				return
			case <-time.After(10 * time.Millisecond):
				q, created = s.getOrCreateQueue(key)
			}
		}
	}

	if created {
		metrics.WorkflowQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(s.queueCount()))
	}

	// Send task to queue (blocking)
	select {
	case q.taskCh <- entry:
		metrics.WorkflowQueueSchedulerTasksSubmitted.With(s.metricsHandler).Record(1)
	case <-s.shutdownChan:
		task.Abort()
		metrics.WorkflowQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
	}
}

// TrySubmit adds a task to its workflow's queue without blocking.
// Returns true if the task was accepted, false if the queue is full or at max capacity.
func (s *WorkflowQueueScheduler[T]) TrySubmit(task T) bool {
	if s.isStopped() {
		task.Abort()
		metrics.WorkflowQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
		return true
	}

	key := s.queueKeyFn(task)
	entry := taskEntry[T]{task: task, submitTime: s.timeSource.Now()}

	q, created := s.getOrCreateQueue(key)
	if q == nil {
		// At max queue capacity
		metrics.WorkflowQueueSchedulerSubmitRejected.With(s.metricsHandler).Record(1)
		return false
	}

	if created {
		metrics.WorkflowQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(s.queueCount()))
	}

	// Try to send task without blocking
	select {
	case q.taskCh <- entry:
		metrics.WorkflowQueueSchedulerTasksSubmitted.With(s.metricsHandler).Record(1)
		return true
	default:
		// Queue channel is full
		metrics.WorkflowQueueSchedulerSubmitRejected.With(s.metricsHandler).Record(1)
		return false
	}
}

// getOrCreateQueue returns the queue for the given key, creating it if necessary.
// Returns (nil, false) if at max capacity and queue doesn't exist.
func (s *WorkflowQueueScheduler[T]) getOrCreateQueue(key any) (*workflowQueue[T], bool) {
	// Fast path: check if queue exists
	s.mu.RLock()
	if q, exists := s.queues[key]; exists {
		s.mu.RUnlock()
		return q, false
	}
	s.mu.RUnlock()

	// Slow path: need to create queue
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if q, exists := s.queues[key]; exists {
		return q, false
	}

	// Check if at max capacity
	if len(s.queues) >= s.options.MaxQueues {
		return nil, false
	}

	// Create new queue
	q := &workflowQueue[T]{
		taskCh: make(chan taskEntry[T], s.options.QueueSize),
	}
	s.queues[key] = q

	// Start the queue's goroutine
	s.shutdownWG.Add(1)
	go s.runQueueWorker(key, q)

	return q, true
}

// runQueueWorker is the goroutine that processes tasks for a single workflow queue.
// It exits when the queue is idle for longer than QueueTTL or when shutdown occurs.
func (s *WorkflowQueueScheduler[T]) runQueueWorker(key any, q *workflowQueue[T]) {
	defer s.shutdownWG.Done()
	defer s.removeQueue(key)

	idleTimer := time.NewTimer(s.options.QueueTTL)
	defer idleTimer.Stop()

	for {
		select {
		case entry, ok := <-q.taskCh:
			if !ok {
				// Channel closed - drain remaining tasks and exit
				s.drainQueue(q)
				return
			}

			// Reset idle timer since we got a task
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(s.options.QueueTTL)

			// Record queue wait time
			queueWaitTime := s.timeSource.Now().Sub(entry.submitTime)
			metrics.WorkflowQueueSchedulerQueueWaitTime.With(s.metricsHandler).Record(queueWaitTime)

			// Execute the task
			s.executeTask(entry.task, entry.submitTime)

		case <-idleTimer.C:
			// Queue has been idle for TTL - exit
			// But first check if there are pending tasks (race condition)
			select {
			case entry, ok := <-q.taskCh:
				if !ok {
					s.drainQueue(q)
					return
				}
				// Got a task, reset timer and process it
				idleTimer.Reset(s.options.QueueTTL)
				queueWaitTime := s.timeSource.Now().Sub(entry.submitTime)
				metrics.WorkflowQueueSchedulerQueueWaitTime.With(s.metricsHandler).Record(queueWaitTime)
				s.executeTask(entry.task, entry.submitTime)
			default:
				// Queue is truly idle - exit
				return
			}
		}
	}
}

// removeQueue removes a queue from the map.
func (s *WorkflowQueueScheduler[T]) removeQueue(key any) {
	s.mu.Lock()
	delete(s.queues, key)
	queueCount := len(s.queues)
	s.mu.Unlock()

	metrics.WorkflowQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(queueCount))
}

// drainQueue aborts all remaining tasks in a queue's channel.
func (s *WorkflowQueueScheduler[T]) drainQueue(q *workflowQueue[T]) {
	abortedCount := 0
	for entry := range q.taskCh {
		entry.task.Abort()
		abortedCount++
	}
	if abortedCount > 0 {
		metrics.WorkflowQueueSchedulerTasksAborted.With(s.metricsHandler).Record(int64(abortedCount))
	}
}

func (s *WorkflowQueueScheduler[T]) executeTask(task T, submitTime time.Time) {
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
			metrics.WorkflowQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
			return
		}

		task.Nack(err)
		metrics.WorkflowQueueSchedulerTasksFailed.With(s.metricsHandler).Record(1)
		return
	}

	task.Ack()
	metrics.WorkflowQueueSchedulerTasksCompleted.With(s.metricsHandler).Record(1)

	// Record total task latency (time from submit to completion)
	taskLatency := s.timeSource.Now().Sub(submitTime)
	metrics.WorkflowQueueSchedulerTaskLatency.With(s.metricsHandler).Record(taskLatency)
}

func (s *WorkflowQueueScheduler[T]) isStopped() bool {
	return s.status.Load() == common.DaemonStatusStopped
}

func (s *WorkflowQueueScheduler[T]) queueCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.queues)
}
