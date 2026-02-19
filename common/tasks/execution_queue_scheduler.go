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

type (
	// QueueKeyFn extracts a queue key from a task for queue routing.
	QueueKeyFn[T Task] func(T) any

	// taskEntry wraps a task with its submit timestamp for latency tracking.
	taskEntry[T Task] struct {
		task       T
		submitTime time.Time
	}

	// executionQueue represents a single execution's task queue.
	// Tasks are stored in a slice protected by the scheduler's mutex.
	// Worker goroutines pop tasks and execute them (up to QueueConcurrency workers).
	executionQueue[T Task] struct {
		tasks         []taskEntry[T]
		activeWorkers int
		lastActive    time.Time
	}

	// executionQueueScheduler is a scheduler that ensures sequential task execution
	// per execution using dedicated goroutines per queue.
	//
	// Key characteristics:
	// - Tasks for the same execution are executed sequentially
	// - Each execution gets its own goroutines that processes tasks from a slice
	// - Worker goroutines exit when the queue is empty
	// - A background sweeper removes idle queue entries after QueueTTL
	// - Maximum number of concurrent queues is capped at MaxQueues
	executionQueueScheduler[T Task] struct {
		status       atomic.Int32
		shutdownChan chan struct{}
		shutdownWG   sync.WaitGroup

		maxQueues        func() int
		queueTTL         func() time.Duration
		queueConcurrency func() int
		queueKeyFn       QueueKeyFn[T]
		logger           log.Logger
		metricsHandler   metrics.Handler
		timeSource       clock.TimeSource

		// mu protects the queues map, sweeperRunning, and all executionQueue fields.
		// All task submissions and worker pops are serialized through this lock.
		mu             sync.Mutex
		queues         map[any]*executionQueue[T]
		sweeperRunning bool
	}
)

// newExecutionQueueScheduler creates a new executionQueueScheduler.
func newExecutionQueueScheduler[T Task](
	maxQueues func() int,
	queueTTL func() time.Duration,
	queueConcurrency func() int,
	queueKeyFn QueueKeyFn[T],
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
) *executionQueueScheduler[T] {
	s := &executionQueueScheduler[T]{
		shutdownChan:     make(chan struct{}),
		maxQueues:        maxQueues,
		queueTTL:         queueTTL,
		queueConcurrency: queueConcurrency,
		queueKeyFn:       queueKeyFn,
		logger:           logger,
		metricsHandler:   metricsHandler,
		timeSource:       timeSource,
		queues:           make(map[any]*executionQueue[T]),
	}
	s.status.Store(common.DaemonStatusInitialized)
	return s
}

func (s *executionQueueScheduler[T]) Start() {
	if !s.status.CompareAndSwap(common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	s.logger.Info("execution queue scheduler started")
}

func (s *executionQueueScheduler[T]) Stop() {
	if !s.status.CompareAndSwap(common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(s.shutdownChan)

	go func() {
		if success := common.AwaitWaitGroup(&s.shutdownWG, time.Minute); !success {
			s.logger.Warn("execution queue scheduler timed out waiting for goroutines")
		}
	}()

	s.logger.Info("execution queue scheduler stopped")
}

// HasQueue returns true if a queue exists for the given execution key.
// This is used by other schedulers to check if they should route tasks here.
func (s *executionQueueScheduler[T]) HasQueue(key any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.queues[key]
	return exists
}

// TrySubmit adds a task to its execution's queue without blocking.
// Returns true if the task was accepted, false if at max queue capacity for new queues.
func (s *executionQueueScheduler[T]) TrySubmit(task T) bool {
	if s.isStopped() {
		task.Abort()

		return true
	}

	key := s.queueKeyFn(task)
	entry := taskEntry[T]{task: task, submitTime: s.timeSource.Now()}

	s.mu.Lock()
	if s.isStopped() {
		s.mu.Unlock()
		task.Abort()

		return true
	}

	q, exists := s.queues[key]
	if !exists && len(s.queues) >= s.maxQueues() {
		s.mu.Unlock()
		metrics.ExecutionQueueSchedulerSubmitRejected.With(s.metricsHandler).Record(1)
		return false
	}
	s.appendTaskLocked(key, q, entry)
	s.mu.Unlock()
	metrics.ExecutionQueueSchedulerTasksSubmitted.With(s.metricsHandler).Record(1)
	return true
}

// appendTaskLocked appends a task to the queue for the given key, creating the
// queue and spawning a worker if needed. Must be called with s.mu held.
// q is the existing queue for the key, or nil if one needs to be created.
func (s *executionQueueScheduler[T]) appendTaskLocked(key any, q *executionQueue[T], entry taskEntry[T]) {
	if q == nil {
		q = &executionQueue[T]{}
		s.queues[key] = q
		metrics.ExecutionQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(len(s.queues)))
	}
	q.tasks = append(q.tasks, entry)
	if q.activeWorkers < max(1, s.queueConcurrency()) {
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

// runWorker is the goroutine that processes tasks for a single execution queue.
// It pops tasks from the slice under the lock and executes them without the lock.
// When the queue is empty, the worker marks itself inactive and exits.
// A new worker is spawned by Submit/TrySubmit when tasks arrive for an inactive queue.
func (s *executionQueueScheduler[T]) runWorker(q *executionQueue[T]) {
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
func (s *executionQueueScheduler[T]) runSweeper() {
	defer s.shutdownWG.Done()

	ch, t := s.timeSource.NewTimer(s.queueTTL())
	defer t.Stop()

	for {
		select {
		case <-ch:
			if s.sweepIdleQueues() {
				return
			}
			t.Reset(s.queueTTL())
		case <-s.shutdownChan:
			return
		}
	}
}

// sweepIdleQueues removes queues that have been idle for longer than QueueTTL.
// Returns true if no queues remain (sweeper should exit).
func (s *executionQueueScheduler[T]) sweepIdleQueues() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.timeSource.Now()
	for key, q := range s.queues {
		if q.activeWorkers == 0 && len(q.tasks) == 0 && now.Sub(q.lastActive) > s.queueTTL() {
			delete(s.queues, key)
		}
	}

	metrics.ExecutionQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(len(s.queues)))
	s.sweeperRunning = len(s.queues) != 0
	return !s.sweeperRunning
}

func (s *executionQueueScheduler[T]) executeTask(task T, submitTime time.Time) {
	operation := func() error {
		if err := task.Execute(); err != nil {
			return task.HandleErr(err)
		}
		return nil
	}

	isRetryable := func(err error) bool {
		return !s.isStopped() && task.IsRetryableError(err)
	}

	if err := backoff.ThrottleRetry(operation, task.RetryPolicy(), isRetryable); err != nil {
		if s.isStopped() {
			task.Abort()
			return
		}

		metrics.ExecutionQueueSchedulerTasksFailed.With(s.metricsHandler).Record(1)
		task.Nack(err)
		return
	}

	metrics.ExecutionQueueSchedulerTasksCompleted.With(s.metricsHandler).Record(1)
	task.Ack()

	taskLatency := s.timeSource.Now().Sub(submitTime)
	metrics.ExecutionQueueSchedulerTaskLatency.With(s.metricsHandler).Record(taskLatency)
}

func (s *executionQueueScheduler[T]) isStopped() bool {
	return s.status.Load() == common.DaemonStatusStopped
}
