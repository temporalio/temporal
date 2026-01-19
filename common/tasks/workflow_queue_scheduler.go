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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var _ Scheduler[Task] = (*WorkflowQueueScheduler[Task])(nil)

const (
	// defaultTasksPerBatch is the default number of tasks to process from a queue
	// before yielding to allow other queues to be processed. This prevents starvation
	// when one workflow has many tasks while others are waiting.
	defaultTasksPerBatch = 10
)

type (
	// WorkflowQueueSchedulerOptions contains configuration for the WorkflowQueueScheduler.
	WorkflowQueueSchedulerOptions struct {
		// QueueSize is the buffer size for the dispatch channel.
		QueueSize int
		// WorkerCount is the number of worker goroutines.
		WorkerCount dynamicconfig.TypedSubscribable[int]
		// TasksPerBatch is the maximum number of tasks to process from a single queue
		// before re-dispatching to allow other queues to be processed. This prevents
		// starvation when one workflow has many pending tasks. Default is 10.
		TasksPerBatch int
	}

	// QueueKeyFn extracts a workflow key from a task for queue routing.
	QueueKeyFn[T Task] func(T) any

	// taskEntry wraps a task with its submit timestamp for latency tracking.
	taskEntry[T Task] struct {
		task       T
		submitTime time.Time
	}

	// WorkflowQueueScheduler is a scheduler that ensures sequential task execution
	// per workflow. It creates queues only for workflows that are routed to it
	// (typically those experiencing contention).
	//
	// Key characteristics:
	// - Tasks for the same workflow are executed sequentially
	// - Each workflow queue is processed by at most one worker at a time
	// - Queues are created on-demand and cleaned up when empty
	// - Queue existence in the map indicates it's being processed
	WorkflowQueueScheduler[T Task] struct {
		status       atomic.Int32
		shutdownChan chan struct{}
		shutdownWG   sync.WaitGroup

		options        *WorkflowQueueSchedulerOptions
		queueKeyFn     QueueKeyFn[T]
		logger         log.Logger
		metricsHandler metrics.Handler
		timeSource     clock.TimeSource

		// mu protects queues map and queue contents
		mu     sync.RWMutex
		queues map[any][]taskEntry[T]

		// pendingTaskCount tracks total pending tasks across all queues
		pendingTaskCount atomic.Int64

		// dispatchChan carries workflow keys for workers to process
		dispatchChan chan any

		// Worker management
		workerLock                      sync.Mutex
		workerShutdownCh                []chan struct{}
		workerCountSubscriptionCancelFn func()
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
	tasksPerBatch := options.TasksPerBatch
	if tasksPerBatch <= 0 {
		tasksPerBatch = defaultTasksPerBatch
	}
	opts := *options
	opts.TasksPerBatch = tasksPerBatch

	s := &WorkflowQueueScheduler[T]{
		shutdownChan:   make(chan struct{}),
		options:        &opts,
		queueKeyFn:     queueKeyFn,
		logger:         logger,
		metricsHandler: metricsHandler,
		timeSource:     timeSource,

		queues:       make(map[any][]taskEntry[T]),
		dispatchChan: make(chan any, options.QueueSize),
	}
	s.status.Store(common.DaemonStatusInitialized)
	return s
}

func (s *WorkflowQueueScheduler[T]) Start() {
	if !s.status.CompareAndSwap(common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	initialWorkerCount, workerCountSubscriptionCancelFn := s.options.WorkerCount(s.updateWorkerCount)
	s.workerCountSubscriptionCancelFn = workerCountSubscriptionCancelFn
	s.updateWorkerCount(initialWorkerCount)

	s.logger.Info("workflow queue scheduler started")
}

func (s *WorkflowQueueScheduler[T]) Stop() {
	if !s.status.CompareAndSwap(common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(s.shutdownChan)
	s.workerCountSubscriptionCancelFn()
	s.updateWorkerCount(0)
	s.drainTasks()

	go func() {
		if success := common.AwaitWaitGroup(&s.shutdownWG, time.Minute); !success {
			s.logger.Warn("workflow queue scheduler timed out waiting for workers")
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
// that blocks until the task is successfully submitted or the scheduler stops.
func (s *WorkflowQueueScheduler[T]) Submit(task T) {
	// Fast path: try non-blocking submit first
	if s.TrySubmit(task) {
		return
	}

	// Slow path: need to block until dispatch channel has space
	key := s.queueKeyFn(task)
	submitTime := s.timeSource.Now()

	s.mu.Lock()

	// Check stopped state while holding lock
	if s.isStopped() {
		s.mu.Unlock()
		task.Abort()
		metrics.WorkflowQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
		return
	}

	// Re-check if queue was created while we were waiting for the lock
	if entries, exists := s.queues[key]; exists {
		s.queues[key] = append(entries, taskEntry[T]{task: task, submitTime: submitTime})
		s.mu.Unlock()
		metrics.WorkflowQueueSchedulerTasksSubmitted.With(s.metricsHandler).Record(1)
		metrics.WorkflowQueueSchedulerPendingTasks.With(s.metricsHandler).Record(float64(s.pendingTaskCount.Add(1)))
		return
	}

	// Create queue while holding lock to ensure other submitters for the same
	// workflow will see it and append to it rather than trying to dispatch
	s.queues[key] = []taskEntry[T]{{task: task, submitTime: submitTime}}
	queueCount := len(s.queues)
	s.mu.Unlock()

	// Block on dispatch channel outside the lock.
	// Other goroutines submitting tasks for the same workflow will see the queue
	// exists and append to it. drainTasks will clean up if shutdown occurs.
	select {
	case s.dispatchChan <- key:
		// Successfully dispatched - worker will process the queue
		metrics.WorkflowQueueSchedulerTasksSubmitted.With(s.metricsHandler).Record(1)
		metrics.WorkflowQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(queueCount))
		metrics.WorkflowQueueSchedulerPendingTasks.With(s.metricsHandler).Record(float64(s.pendingTaskCount.Add(1)))
	case <-s.shutdownChan:
		// Scheduler stopped while waiting - drainTasks will abort queued tasks
	}
}

// TrySubmit adds a task to its workflow's queue and attempts non-blocking dispatch.
// Returns true if the task was added successfully, false if the dispatch channel is full
// when trying to dispatch a new queue.
func (s *WorkflowQueueScheduler[T]) TrySubmit(task T) bool {
	// Fast path: check if stopped before acquiring lock
	if s.isStopped() {
		task.Abort()
		metrics.WorkflowQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
		return true
	}

	key := s.queueKeyFn(task)
	submitTime := s.timeSource.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check stopped state while holding lock to prevent race with drainTasks.
	// This ensures that if drainTasks has completed, we don't add tasks that will never be processed.
	if s.isStopped() {
		task.Abort()
		metrics.WorkflowQueueSchedulerTasksAborted.With(s.metricsHandler).Record(1)
		return true
	}

	// If queue exists, a worker is processing it - just add task
	if entries, exists := s.queues[key]; exists {
		s.queues[key] = append(entries, taskEntry[T]{task: task, submitTime: submitTime})
		metrics.WorkflowQueueSchedulerTasksSubmitted.With(s.metricsHandler).Record(1)
		metrics.WorkflowQueueSchedulerPendingTasks.With(s.metricsHandler).Record(float64(s.pendingTaskCount.Add(1)))
		return true
	}

	// New queue - try to dispatch first, then create
	select {
	case s.dispatchChan <- key:
		s.queues[key] = []taskEntry[T]{{task: task, submitTime: submitTime}}
		metrics.WorkflowQueueSchedulerTasksSubmitted.With(s.metricsHandler).Record(1)
		metrics.WorkflowQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(len(s.queues)))
		metrics.WorkflowQueueSchedulerPendingTasks.With(s.metricsHandler).Record(float64(s.pendingTaskCount.Add(1)))
		return true
	default:
		// Channel full - don't create queue, return false
		metrics.WorkflowQueueSchedulerSubmitRejected.With(s.metricsHandler).Record(1)
		return false
	}
}

func (s *WorkflowQueueScheduler[T]) updateWorkerCount(targetWorkerNum int) {
	s.workerLock.Lock()
	defer s.workerLock.Unlock()

	if s.isStopped() {
		targetWorkerNum = 0
	}

	if targetWorkerNum < 0 {
		s.logger.Error("Target worker pool size is negative. Please fix the dynamic config.",
			tag.Key("worker-pool-size"), tag.Value(targetWorkerNum))
		return
	}

	currentWorkerNum := len(s.workerShutdownCh)
	if targetWorkerNum == currentWorkerNum {
		return
	}

	if targetWorkerNum > currentWorkerNum {
		s.startWorkers(targetWorkerNum - currentWorkerNum)
	} else {
		s.stopWorkers(currentWorkerNum - targetWorkerNum)
	}

	// Emit active workers metric on change
	metrics.WorkflowQueueSchedulerActiveWorkers.With(s.metricsHandler).Record(float64(targetWorkerNum))

	s.logger.Info("Update worker pool size", tag.Key("worker-pool-size"), tag.Value(targetWorkerNum))
}

func (s *WorkflowQueueScheduler[T]) startWorkers(count int) {
	for range count {
		shutdownCh := make(chan struct{})
		s.workerShutdownCh = append(s.workerShutdownCh, shutdownCh)

		s.shutdownWG.Add(1)
		go s.worker(shutdownCh)
	}
}

func (s *WorkflowQueueScheduler[T]) stopWorkers(count int) {
	shutdownChToClose := s.workerShutdownCh[:count]
	s.workerShutdownCh = s.workerShutdownCh[count:]

	for _, shutdownCh := range shutdownChToClose {
		close(shutdownCh)
	}
}

func (s *WorkflowQueueScheduler[T]) worker(workerShutdownCh <-chan struct{}) {
	defer s.shutdownWG.Done()

	for {
		select {
		case <-s.shutdownChan:
			return
		case <-workerShutdownCh:
			return
		case key := <-s.dispatchChan:
			s.processQueue(key)
		}
	}
}

// processQueue processes tasks from the queue for the given workflow.
// To prevent starvation of other workflows, it processes at most TasksPerBatch
// tasks before re-dispatching the queue if more tasks remain.
func (s *WorkflowQueueScheduler[T]) processQueue(key any) {
	tasksProcessed := 0

	for {
		if s.isStopped() {
			return
		}

		entry, queueDeleted, ok := s.popTask(key)
		if !ok {
			return
		}

		// Emit metrics for task removal
		metrics.WorkflowQueueSchedulerPendingTasks.With(s.metricsHandler).Record(float64(s.pendingTaskCount.Add(-1)))
		if queueDeleted {
			s.mu.RLock()
			queueCount := len(s.queues)
			s.mu.RUnlock()
			metrics.WorkflowQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(queueCount))
		}

		// Record queue wait time (time from submit to start of execution)
		queueWaitTime := s.timeSource.Now().Sub(entry.submitTime)
		metrics.WorkflowQueueSchedulerQueueWaitTime.With(s.metricsHandler).Record(queueWaitTime)

		s.executeTask(entry.task, entry.submitTime)
		tasksProcessed++

		// If we've processed a batch and the queue still has tasks,
		// re-dispatch to allow other workflows to be processed (prevent starvation)
		if !queueDeleted && tasksProcessed >= s.options.TasksPerBatch {
			select {
			case s.dispatchChan <- key:
				// Successfully re-dispatched, let other queues get a turn
				return
			default:
				// Channel full, continue processing this queue
				tasksProcessed = 0
			}
		}
	}
}

// popTask removes and returns the next task from the queue.
// If the queue becomes empty, it's deleted from the map.
// Returns (entry, queueDeleted, ok) - queueDeleted indicates if the queue was removed.
func (s *WorkflowQueueScheduler[T]) popTask(key any) (taskEntry[T], bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var zero taskEntry[T]
	entries, exists := s.queues[key]
	if !exists || len(entries) == 0 {
		delete(s.queues, key)
		return zero, true, false
	}

	entry := entries[0]
	// Nil out the element to allow GC (prevents memory leak)
	entries[0] = zero

	if len(entries) == 1 {
		// Last task in queue - delete the queue
		delete(s.queues, key)
		return entry, true, true
	}

	s.queues[key] = entries[1:]
	return entry, false, true
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

func (s *WorkflowQueueScheduler[T]) drainTasks() {
	// Drain the dispatch channel
LoopDrainChannel:
	for {
		select {
		case <-s.dispatchChan:
		default:
			break LoopDrainChannel
		}
	}

	// Drain all queues
	s.mu.Lock()
	defer s.mu.Unlock()

	abortedCount := 0
	for key, entries := range s.queues {
		for _, entry := range entries {
			entry.task.Abort()
			abortedCount++
		}
		delete(s.queues, key)
	}

	if abortedCount > 0 {
		metrics.WorkflowQueueSchedulerTasksAborted.With(s.metricsHandler).Record(int64(abortedCount))
		// Emit final metrics after drain
		metrics.WorkflowQueueSchedulerPendingTasks.With(s.metricsHandler).Record(float64(s.pendingTaskCount.Add(-int64(abortedCount))))
		metrics.WorkflowQueueSchedulerQueueCount.With(s.metricsHandler).Record(0)
	}
}

func (s *WorkflowQueueScheduler[T]) isStopped() bool {
	return s.status.Load() == common.DaemonStatusStopped
}
