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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var _ Scheduler[Task] = (*WorkflowQueueScheduler[Task])(nil)

type (
	// WorkflowQueueSchedulerOptions contains configuration for the WorkflowQueueScheduler.
	WorkflowQueueSchedulerOptions struct {
		// QueueSize is the buffer size for the dispatch channel.
		QueueSize int
		// WorkerCount is the number of worker goroutines.
		WorkerCount dynamicconfig.TypedSubscribable[int]
	}

	// QueueKeyFn extracts a workflow key from a task for queue routing.
	QueueKeyFn[T Task] func(T) any

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
		status       int32
		shutdownChan chan struct{}
		shutdownWG   sync.WaitGroup

		options    *WorkflowQueueSchedulerOptions
		queueKeyFn QueueKeyFn[T]
		logger     log.Logger

		// mu protects queues map and queue contents
		mu     sync.RWMutex
		queues map[any][]T

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
) *WorkflowQueueScheduler[T] {
	return &WorkflowQueueScheduler[T]{
		status:       common.DaemonStatusInitialized,
		shutdownChan: make(chan struct{}),
		options:      options,
		queueKeyFn:   queueKeyFn,
		logger:       logger,

		queues:       make(map[any][]T),
		dispatchChan: make(chan any, options.QueueSize),
	}
}

func (s *WorkflowQueueScheduler[T]) Start() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	initialWorkerCount, workerCountSubscriptionCancelFn := s.options.WorkerCount(s.updateWorkerCount)
	s.workerCountSubscriptionCancelFn = workerCountSubscriptionCancelFn
	s.updateWorkerCount(initialWorkerCount)

	s.logger.Info("workflow queue scheduler started")
}

func (s *WorkflowQueueScheduler[T]) Stop() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
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
// that retries until the task is successfully submitted.
func (s *WorkflowQueueScheduler[T]) Submit(task T) {
	for !s.TrySubmit(task) {
		if s.isStopped() {
			task.Abort()
			return
		}
		runtime.Gosched() // Yield to let workers make progress
	}
}

// TrySubmit adds a task to its workflow's queue and attempts non-blocking dispatch.
// Returns true if the task was added successfully, false if the dispatch channel is full
// when trying to dispatch a new queue.
func (s *WorkflowQueueScheduler[T]) TrySubmit(task T) bool {
	// Fast path: check if stopped before acquiring lock
	if s.isStopped() {
		task.Abort()
		return true
	}

	key := s.queueKeyFn(task)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check stopped state while holding lock to prevent race with drainTasks.
	// This ensures that if drainTasks has completed, we don't add tasks that will never be processed.
	if s.isStopped() {
		task.Abort()
		return true
	}

	// If queue exists, a worker is processing it - just add task
	if tasks, exists := s.queues[key]; exists {
		s.queues[key] = append(tasks, task)
		return true
	}

	// New queue - try to dispatch first, then create
	select {
	case s.dispatchChan <- key:
		s.queues[key] = []T{task}
		return true
	default:
		// Channel full - don't create queue, return false
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

// processQueue processes all tasks in the queue for the given workflow.
// The worker owns the queue until it's empty.
func (s *WorkflowQueueScheduler[T]) processQueue(key any) {
	for {
		if s.isStopped() {
			return
		}

		task, ok := s.popTask(key)
		if !ok {
			return
		}

		s.executeTask(task)
	}
}

// popTask removes and returns the next task from the queue.
// If the queue becomes empty, it's deleted from the map.
// Returns false if there are no more tasks.
func (s *WorkflowQueueScheduler[T]) popTask(key any) (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var zero T
	tasks, exists := s.queues[key]
	if !exists || len(tasks) == 0 {
		delete(s.queues, key)
		return zero, false
	}

	task := tasks[0]
	s.queues[key] = tasks[1:]
	return task, true
}

func (s *WorkflowQueueScheduler[T]) executeTask(task T) {
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
			return
		}

		task.Nack(err)
		return
	}

	task.Ack()
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

	for key, tasks := range s.queues {
		for _, task := range tasks {
			task.Abort()
		}
		delete(s.queues, key)
	}
}

func (s *WorkflowQueueScheduler[T]) isStopped() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStopped
}
