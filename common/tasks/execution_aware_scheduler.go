package tasks

import (
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

var _ Scheduler[Task] = (*ExecutionAwareScheduler[Task])(nil)

type (
	// ExecutionAwareSchedulerOptions contains configuration for the ExecutionAwareScheduler.
	ExecutionAwareSchedulerOptions struct {
		// Enabled controls whether the executionQueueScheduler is active.
		Enabled func() bool
		// MaxQueues is the maximum number of concurrent execution queues.
		// When this limit is reached, new queues are rejected and tasks fall back to the base scheduler.
		MaxQueues func() int
		// QueueTTL is how long an idle queue stays in the map before being swept.
		QueueTTL func() time.Duration
		// QueueConcurrency is the max number of worker goroutines per queue.
		// Values <= 0 are capped to 1 (strictly sequential).
		QueueConcurrency func() int
	}

	// ExecutionAwareScheduler is a scheduler that wraps a base scheduler and adds
	// an executionQueueScheduler for handling execution contention.
	//
	// By default, tasks are processed by the base scheduler. When an execution experiences
	// contention (e.g., busy workflow error), it gets routed to the executionQueueScheduler
	// which ensures tasks are processed sequentially per execution.
	ExecutionAwareScheduler[T Task] struct {
		baseScheduler           Scheduler[T]
		executionQueueScheduler *executionQueueScheduler[T]

		queueKeyFn QueueKeyFn[T]
		options    ExecutionAwareSchedulerOptions
		logger     log.Logger
	}
)

// NewExecutionAwareScheduler creates a new ExecutionAwareScheduler.
func NewExecutionAwareScheduler[T Task](
	baseScheduler Scheduler[T],
	options ExecutionAwareSchedulerOptions,
	queueKeyFn QueueKeyFn[T],
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
) *ExecutionAwareScheduler[T] {
	return &ExecutionAwareScheduler[T]{
		baseScheduler: baseScheduler,
		executionQueueScheduler: newExecutionQueueScheduler(
			options.MaxQueues,
			options.QueueTTL,
			options.QueueConcurrency,
			queueKeyFn,
			logger,
			metricsHandler,
			timeSource,
		),
		queueKeyFn: queueKeyFn,
		options:    options,
		logger:     logger,
	}
}

func (s *ExecutionAwareScheduler[T]) Start() {
	s.baseScheduler.Start()
	// Always start the executionQueueScheduler regardless of current config.
	// The Enabled check gates task routing, so an idle scheduler has minimal
	// overhead. This ensures if the config changes from disabled to enabled,
	// tasks will be processed correctly.
	s.executionQueueScheduler.Start()
}

func (s *ExecutionAwareScheduler[T]) Stop() {
	s.baseScheduler.Stop()
	s.executionQueueScheduler.Stop()
}

func (s *ExecutionAwareScheduler[T]) Submit(task T) {
	if s.shouldRouteToExecutionQueueScheduler(task) {
		if s.executionQueueScheduler.TrySubmit(task) {
			return
		}
		// executionQueueScheduler is full, fall through to base scheduler.
	}
	s.baseScheduler.Submit(task)
}

func (s *ExecutionAwareScheduler[T]) TrySubmit(task T) bool {
	if s.shouldRouteToExecutionQueueScheduler(task) {
		if s.executionQueueScheduler.TrySubmit(task) {
			return true
		}
		// executionQueueScheduler is full, fall through to base scheduler.
	}
	return s.baseScheduler.TrySubmit(task)
}

// HandleBusyWorkflow routes a task to the executionQueueScheduler when it
// encounters a contention error. Returns true if the task was handled
// (submitted to EQS), false if the caller should handle it (e.g., feature
// disabled or EQS at max capacity).
func (s *ExecutionAwareScheduler[T]) HandleBusyWorkflow(task T) bool {
	if !s.options.Enabled() {
		return false
	}
	return s.executionQueueScheduler.TrySubmit(task)
}

// HasExecutionQueue returns true if the task's execution has an active queue
// in the executionQueueScheduler.
func (s *ExecutionAwareScheduler[T]) HasExecutionQueue(task T) bool {
	if !s.options.Enabled() {
		return false
	}
	return s.executionQueueScheduler.HasQueue(s.queueKeyFn(task))
}

func (s *ExecutionAwareScheduler[T]) shouldRouteToExecutionQueueScheduler(task T) bool {
	if !s.options.Enabled() {
		return false
	}
	return s.executionQueueScheduler.HasQueue(s.queueKeyFn(task))
}
