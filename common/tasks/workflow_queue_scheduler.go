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
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var _ Scheduler[Task] = (*WorkflowQueueScheduler[Task])(nil)

type (
	// WorkflowQueueSchedulerOptions contains configuration for the WorkflowQueueScheduler.
	WorkflowQueueSchedulerOptions struct {
		// QueueSize is the buffer size for the dispatch channel.
		// Should be large enough to handle normal load without blocking.
		QueueSize int
		// WorkerCount is the number of worker goroutines.
		WorkerCount dynamicconfig.TypedSubscribable[int]
	}

	// WorkflowQueueScheduler is a scheduler that ensures sequential task execution
	// per workflow. It creates queues only for workflows that are routed to it
	// (typically those experiencing contention).
	//
	// Key characteristics:
	// - Tasks for the same workflow are executed sequentially
	// - Each workflow queue is processed by at most one worker at a time
	// - Queues are created on-demand and cleaned up when empty
	// - TrySubmit is non-blocking for integration with other schedulers
	// - Only dispatches to queueChan when a NEW queue is created (like SequentialScheduler)
	WorkflowQueueScheduler[T Task] struct {
		status       int32
		shutdownChan chan struct{}
		shutdownWG   sync.WaitGroup

		workerLock       sync.Mutex
		workerShutdownCh []chan struct{}

		workerCountSubscriptionCancelFn func()

		options      *WorkflowQueueSchedulerOptions
		queueFactory SequentialTaskQueueFactory[T]
		logger       log.Logger

		// queues maps workflow key -> task queue
		queues collection.ConcurrentTxMap
		// queueChan carries queue IDs for dispatch (only sent once per queue lifecycle)
		queueChan chan interface{}
	}
)

// NewWorkflowQueueScheduler creates a new WorkflowQueueScheduler.
func NewWorkflowQueueScheduler[T Task](
	options *WorkflowQueueSchedulerOptions,
	taskQueueHashFn collection.HashFunc,
	taskQueueFactory SequentialTaskQueueFactory[T],
	logger log.Logger,
) *WorkflowQueueScheduler[T] {
	return &WorkflowQueueScheduler[T]{
		status:       common.DaemonStatusInitialized,
		shutdownChan: make(chan struct{}),
		options:      options,
		queueFactory: taskQueueFactory,
		logger:       logger,

		queues:    collection.NewShardedConcurrentTxMap(1024, taskQueueHashFn),
		queueChan: make(chan interface{}, options.QueueSize),
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
	// must be called after the close of the shutdownChan
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
func (s *WorkflowQueueScheduler[T]) HasQueue(key interface{}) bool {
	return s.queues.Contains(key)
}

// Submit adds a task to its workflow's queue and dispatches for processing.
// This is a blocking call if the dispatch channel is full.
// Only dispatches to queueChan when a NEW queue is created.
func (s *WorkflowQueueScheduler[T]) Submit(task T) {
	queueID, isNewQueue := s.addTaskToQueue(task)

	// Only dispatch if this is a new queue (queue was just created)
	// If queue already existed, it's already been dispatched and a worker is processing it
	if !isNewQueue {
		if s.isStopped() {
			s.drainTasks()
		}
		return
	}

	// Need to dispatch this new queue
	select {
	case <-s.shutdownChan:
		// Task is already in the queue - drainTasks will abort it.
		// Don't call task.Abort() here to avoid double abort.
		s.drainTasks()
	case s.queueChan <- queueID:
		if s.isStopped() {
			s.drainTasks()
		}
	}
}

// TrySubmit adds a task to its workflow's queue and attempts non-blocking dispatch.
// Returns true if the task was added successfully, false if the channel is full
// when trying to dispatch a new queue.
// This is used for integration with other schedulers that may want to fall back
// to rescheduling if this scheduler is overloaded.
func (s *WorkflowQueueScheduler[T]) TrySubmit(task T) bool {
	queueID, isNewQueue := s.addTaskToQueue(task)

	// If queue already existed, task was added to it - success
	// The queue is already dispatched and being processed
	if !isNewQueue {
		if s.isStopped() {
			s.drainTasks()
		}
		return true
	}

	// New queue - need to dispatch it
	select {
	case s.queueChan <- queueID:
		if s.isStopped() {
			s.drainTasks()
		}
		return true
	default:
		// Channel is full. Check if we can safely remove our queue.
		// Only remove if the queue contains just our task (Len <= 1).
		// If other tasks were added concurrently, we must dispatch the queue.
		deleted := s.queues.RemoveIf(queueID, func(k, v interface{}) bool {
			return v.(SequentialTaskQueue[T]).Len() <= 1
		})
		if deleted {
			// Only our task was in the queue, removed successfully.
			// Return false so the caller can handle the task (e.g., reschedule).
			return false
		}
		// Other tasks were added concurrently - we must dispatch to process them.
		// Fall back to blocking dispatch. This only happens in the rare case
		// where: (1) channel is full, AND (2) another task was added
		// concurrently. In this edge case, blocking is acceptable to ensure
		// no tasks are orphaned.
		select {
		case <-s.shutdownChan:
			// Scheduler is stopping - drainTasks will abort all tasks including ours.
			// Return true since task is being handled (aborted is valid handling during shutdown).
			s.drainTasks()
			return true
		case s.queueChan <- queueID:
			if s.isStopped() {
				s.drainTasks()
			}
			return true
		}
	}
}

// addTaskToQueue adds a task to its workflow's queue.
// Returns the queue ID and whether the queue was newly created.
//
// Implementation note: We pre-add the task to the new queue before calling PutOrDo.
// This ensures atomicity - if we added after PutOrDo, a worker could pop from
// the empty queue and remove it before we add the task.
//
// When the key already exists, the pre-added queue is discarded (a minor allocation
// cost) and the task is added to the existing queue via the callback. This tradeoff
// is acceptable to maintain the atomic guarantee.
func (s *WorkflowQueueScheduler[T]) addTaskToQueue(task T) (interface{}, bool) {
	queue := s.queueFactory(task)
	queue.Add(task) // Pre-add for atomicity (see comment above)

	_, existed, err := s.queues.PutOrDo(
		queue.ID(),
		queue,
		func(key interface{}, value interface{}) error {
			// Key exists: add task to existing queue (pre-added queue will be discarded)
			value.(SequentialTaskQueue[T]).Add(task)
			return nil
		},
	)
	if err != nil {
		panic("Error is not expected as the evaluation function returns nil")
	}

	return queue.ID(), !existed
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
	for i := 0; i < count; i++ {
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
			s.drainTasks()
			return
		case <-workerShutdownCh:
			return
		case queueID := <-s.queueChan:
			s.processQueue(queueID, workerShutdownCh)
		}
	}
}

// processQueue processes all tasks in the queue for the given workflow.
// The worker owns the queue until it's empty or the worker shuts down.
func (s *WorkflowQueueScheduler[T]) processQueue(queueID interface{}, workerShutdownCh <-chan struct{}) {
	// Process all tasks in the queue
	for {
		select {
		case <-s.shutdownChan:
			s.drainTasks()
			return
		case <-workerShutdownCh:
			// Worker is shutting down (scale-down), re-dispatch the queue
			// so another worker can pick it up.
			// Check if scheduler is already stopped first.
			if s.isStopped() {
				s.drainTasks()
				return
			}
			// Use select with shutdownChan to avoid deadlock if scheduler stops
			// while we're waiting for channel space.
			select {
			case <-s.shutdownChan:
				// Scheduler is stopping - don't re-dispatch, drainTasks will handle it
				s.drainTasks()
			case s.queueChan <- queueID:
				// Successfully re-dispatched to another worker
			}
			return
		default:
			task := s.popNextTask(queueID)
			// Check for zero value since T is a generic type
			var zero T
			if any(task) == any(zero) {
				// Queue is empty and removed, we're done
				return
			}
			s.executeTask(task)
		}
	}
}

// popNextTask atomically removes and returns the next task from the queue.
// If the queue becomes empty after removing the task, the queue is also removed from the map.
// Returns the zero value of T if the queue doesn't exist or is empty.
func (s *WorkflowQueueScheduler[T]) popNextTask(queueID interface{}) T {
	var task T
	s.queues.RemoveIf(queueID, func(key interface{}, value interface{}) bool {
		queue := value.(SequentialTaskQueue[T])
		if !queue.IsEmpty() {
			task = queue.Remove()
			return queue.IsEmpty() // Remove from map only if now empty
		}
		return true // Queue is empty, remove it
	})
	return task
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
				shouldRetry = false // do not retry if panic
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
		case queueID := <-s.queueChan:
			// Drain the queue with retry loop to handle concurrent additions
		LoopDrainSingleQueue:
			for {
				queueVal, ok := s.queues.Get(queueID)
				if !ok {
					break LoopDrainSingleQueue
				}
				queue := queueVal.(SequentialTaskQueue[T])
				for !queue.IsEmpty() {
					task := queue.Remove()
					// Check for zero value since T is a generic type
					var zero T
					if any(task) != any(zero) {
						task.Abort()
					}
				}
				// Try to remove queue only if still empty
				deleted := s.queues.RemoveIf(queueID, func(key interface{}, value interface{}) bool {
					return value.(SequentialTaskQueue[T]).IsEmpty()
				})
				if deleted {
					break LoopDrainSingleQueue
				}
				// If not deleted, new tasks were added - continue draining
			}
		default:
			break LoopDrainChannel
		}
	}

	// Drain any remaining queues not in the channel
	// Use a loop to handle concurrent additions during iteration
	for {
		var remainingQueueIDs []interface{}
		iter := s.queues.Iter()
		for entry := range iter.Entries() {
			remainingQueueIDs = append(remainingQueueIDs, entry.Key)
		}
		iter.Close()

		if len(remainingQueueIDs) == 0 {
			break
		}

		for _, queueID := range remainingQueueIDs {
			// Drain each queue with the same retry pattern
			for {
				queueVal, ok := s.queues.Get(queueID)
				if !ok {
					break
				}
				queue := queueVal.(SequentialTaskQueue[T])
				for !queue.IsEmpty() {
					task := queue.Remove()
					var zero T
					if any(task) != any(zero) {
						task.Abort()
					}
				}
				deleted := s.queues.RemoveIf(queueID, func(key interface{}, value interface{}) bool {
					return value.(SequentialTaskQueue[T]).IsEmpty()
				})
				if deleted {
					break
				}
			}
		}
	}
}

func (s *WorkflowQueueScheduler[T]) isStopped() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStopped
}
