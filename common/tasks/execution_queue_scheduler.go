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
	ExecutionQueueSchedulerOptions struct {
		// MaxQueues is the maximum number of concurrent execution queues.
		// When this limit is reached, TrySubmit will reject new queues.
		MaxQueues func() int
		// QueueTTL is how long an idle queue stays in the map before being swept.
		QueueTTL func() time.Duration
		// QueueConcurrency is the max number of worker goroutines per queue.
		// Values <= 0 are capped to 1 (strictly sequential).
		QueueConcurrency func() int
	}

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

	// ExecutionQueueScheduler is a scheduler that ensures sequential task execution
	// per execution using dedicated goroutines per queue.
	//
	// Key characteristics:
	// - Tasks for the same execution are executed sequentially
	// - Each execution gets its own goroutines that processes tasks from a slice
	// - Worker goroutines exit when the queue is empty
	// - A background sweeper removes idle queue entries after QueueTTL
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

		// mu protects the queues map, sweeperRunning, and all executionQueue fields.
		// All task submissions and worker pops are serialized through this lock.
		mu             sync.Mutex
		queues         map[any]*executionQueue[T]
		sweeperRunning bool
		// queueAvailable is signaled when queues are removed by the sweeper,
		// unblocking Submit callers waiting for queue capacity.
		queueAvailable *sync.Cond
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
		queues:         make(map[any]*executionQueue[T]),
	}
	s.queueAvailable = sync.NewCond(&s.mu)
	s.status.Store(common.DaemonStatusInitialized)
	return s
}

func (s *ExecutionQueueScheduler[T]) Start() {
	if !s.status.CompareAndSwap(common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	s.logger.Info("execution queue scheduler started")
}

func (s *ExecutionQueueScheduler[T]) Stop() {
	if !s.status.CompareAndSwap(common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(s.shutdownChan)

	// Wake up any Submit callers blocked waiting for queue capacity.
	s.mu.Lock()
	s.queueAvailable.Broadcast()
	s.mu.Unlock()

	go func() {
		if success := common.AwaitWaitGroup(&s.shutdownWG, time.Minute); !success {
			s.logger.Warn("execution queue scheduler timed out waiting for goroutines")
		}
	}()

	s.logger.Info("execution queue scheduler stopped")
}

// HasQueue returns true if a queue exists for the given execution key.
// This is used by other schedulers to check if they should route tasks here.
func (s *ExecutionQueueScheduler[T]) HasQueue(key any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.queues[key]
	return exists
}

// Submit adds a task to its execution's queue. Creates a new queue if needed.
// If MaxQueues is reached and the task needs a new queue, Submit blocks until
// the sweeper frees a slot or the scheduler is stopped.
func (s *ExecutionQueueScheduler[T]) Submit(task T) {
	if s.isStopped() {
		task.Abort()

		return
	}

	key := s.queueKeyFn(task)
	entry := taskEntry[T]{task: task, submitTime: s.timeSource.Now()}

	s.mu.Lock()
	var q *executionQueue[T]
	for {
		if s.isStopped() {
			s.mu.Unlock()
			task.Abort()
	
			return
		}
		var exists bool
		q, exists = s.queues[key]
		if exists || len(s.queues) < s.options.MaxQueues() {
			break
		}
		s.queueAvailable.Wait()
	}
	s.appendTaskLocked(key, q, entry)
	s.mu.Unlock()
	metrics.ExecutionQueueSchedulerTasksSubmitted.With(s.metricsHandler).Record(1)
}

// TrySubmit adds a task to its execution's queue without blocking.
// Returns true if the task was accepted, false if at max queue capacity for new queues.
func (s *ExecutionQueueScheduler[T]) TrySubmit(task T) bool {
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
	if !exists && len(s.queues) >= s.options.MaxQueues() {
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
func (s *ExecutionQueueScheduler[T]) appendTaskLocked(key any, q *executionQueue[T], entry taskEntry[T]) {
	if q == nil {
		q = &executionQueue[T]{}
		s.queues[key] = q
		metrics.ExecutionQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(len(s.queues)))
		// Wake all blocked Submit callers — those waiting for this same key
		// can now proceed since the queue exists. Different-key waiters will
		// re-check the condition and go back to sleep.
		s.queueAvailable.Broadcast()
	}
	q.tasks = append(q.tasks, entry)
	if q.activeWorkers < max(1, s.options.QueueConcurrency()) {
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
func (s *ExecutionQueueScheduler[T]) runWorker(q *executionQueue[T]) {
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
func (s *ExecutionQueueScheduler[T]) runSweeper() {
	defer s.shutdownWG.Done()

	ch, t := s.timeSource.NewTimer(s.options.QueueTTL())
	defer t.Stop()

	for {
		select {
		case <-ch:
			if s.sweepIdleQueues() {
				return
			}
			t.Reset(s.options.QueueTTL())
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
	sweptCount := 0
	for key, q := range s.queues {
		if q.activeWorkers == 0 && len(q.tasks) == 0 && now.Sub(q.lastActive) > s.options.QueueTTL() {
			delete(s.queues, key)
			sweptCount++
		}
	}

	metrics.ExecutionQueueSchedulerQueueCount.With(s.metricsHandler).Record(float64(len(s.queues)))
	for range sweptCount {
		s.queueAvailable.Signal()
	}

	if len(s.queues) == 0 {
		s.sweeperRunning = false
		return true
	}
	return false
}

func (s *ExecutionQueueScheduler[T]) executeTask(task T, submitTime time.Time) {
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

func (s *ExecutionQueueScheduler[T]) isStopped() bool {
	return s.status.Load() == common.DaemonStatusStopped
}
