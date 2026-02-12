package queues

import (
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tasks"
)

var _ tasks.Scheduler[Executable] = (*ExecutionAwareScheduler)(nil)

type (
	// BusyWorkflowHandler is an interface for schedulers that can handle busy workflow errors
	// by routing tasks to a ExecutionQueueScheduler.
	BusyWorkflowHandler interface {
		// HandleBusyWorkflow is called when a task encounters a busy workflow error.
		// It routes the task to the ExecutionQueueScheduler for sequential processing.
		// Returns true if the task was handled, false if the caller should handle it.
		HandleBusyWorkflow(Executable) bool
	}

	// ExecutionAwareSchedulerOptions contains configuration for the ExecutionAwareScheduler.
	ExecutionAwareSchedulerOptions struct {
		// EnableExecutionQueueScheduler controls whether the ExecutionQueueScheduler is enabled.
		EnableExecutionQueueScheduler dynamicconfig.BoolPropertyFn
		// ExecutionQueueSchedulerMaxQueues is the maximum number of concurrent per-workflow queues.
		ExecutionQueueSchedulerMaxQueues dynamicconfig.IntPropertyFn
		// ExecutionQueueSchedulerQueueTTL is how long a queue goroutine waits idle before exiting.
		ExecutionQueueSchedulerQueueTTL dynamicconfig.DurationPropertyFn
		// ExecutionQueueSchedulerQueueConcurrency is the max workers per queue.
		// Defaults to 1 (sequential) if nil.
		ExecutionQueueSchedulerQueueConcurrency dynamicconfig.IntPropertyFn
	}

	// ExecutionAwareScheduler is a scheduler that wraps a base FIFO scheduler and adds
	// a ExecutionQueueScheduler for handling workflow contention.
	//
	// This scheduler implements tasks.Scheduler[Executable] and is designed to be
	// passed to the InterleavedWeightedRoundRobinScheduler as the underlying processor.
	//
	// By default, tasks are processed by the base FIFO scheduler. When a workflow experiences
	// contention (busy workflow error), it gets routed to the ExecutionQueueScheduler which
	// ensures tasks are processed one at a time per workflow.
	ExecutionAwareScheduler struct {
		baseScheduler          tasks.Scheduler[Executable]
		executionQueueScheduler *tasks.ExecutionQueueScheduler[Executable]

		options ExecutionAwareSchedulerOptions
		logger  log.Logger
	}
)

// NewExecutionAwareScheduler creates a new ExecutionAwareScheduler.
func NewExecutionAwareScheduler(
	baseScheduler tasks.Scheduler[Executable],
	options ExecutionAwareSchedulerOptions,
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
) *ExecutionAwareScheduler {
	return &ExecutionAwareScheduler{
		baseScheduler: baseScheduler,
		executionQueueScheduler: tasks.NewExecutionQueueScheduler(
			&tasks.ExecutionQueueSchedulerOptions{
				MaxQueues:        options.ExecutionQueueSchedulerMaxQueues,
				QueueTTL:         options.ExecutionQueueSchedulerQueueTTL,
				QueueConcurrency: options.ExecutionQueueSchedulerQueueConcurrency,
			},
			executableQueueKeyFn,
			logger,
			metricsHandler,
			timeSource,
		),
		options: options,
		logger:  logger,
	}
}

func (s *ExecutionAwareScheduler) Start() {
	s.baseScheduler.Start()
	// Always start the ExecutionQueueScheduler regardless of current config.
	// The EnableExecutionQueueScheduler check gates task routing, so an idle
	// scheduler has minimal overhead. This ensures if the dynamic config changes
	// from disabled to enabled, tasks will be processed correctly.
	s.executionQueueScheduler.Start()
}

func (s *ExecutionAwareScheduler) Stop() {
	s.baseScheduler.Stop()
	s.executionQueueScheduler.Stop()
}

func (s *ExecutionAwareScheduler) Submit(executable Executable) {
	if s.shouldRouteToExecutionQueueScheduler(executable) {
		s.executionQueueScheduler.Submit(executable)
		return
	}
	s.baseScheduler.Submit(executable)
}

func (s *ExecutionAwareScheduler) TrySubmit(executable Executable) bool {
	if s.shouldRouteToExecutionQueueScheduler(executable) {
		if s.executionQueueScheduler.TrySubmit(executable) {
			return true
		}
		// ExecutionQueueScheduler is full, fall through to base scheduler.
	}
	return s.baseScheduler.TrySubmit(executable)
}

// HandleBusyWorkflow implements BusyWorkflowHandler.
// It routes a task to the ExecutionQueueScheduler when it encounters a busy workflow error.
// Returns true if the task was handled (submitted to EQS), false if caller should handle it
// (e.g., feature disabled or EQS at max capacity).
func (s *ExecutionAwareScheduler) HandleBusyWorkflow(executable Executable) bool {
	if !s.options.EnableExecutionQueueScheduler() {
		return false
	}
	return s.executionQueueScheduler.TrySubmit(executable)
}

// HasExecutionQueue returns true if the workflow has an active queue in the ExecutionQueueScheduler.
func (s *ExecutionAwareScheduler) HasExecutionQueue(executable Executable) bool {
	if !s.options.EnableExecutionQueueScheduler() {
		return false
	}
	return s.executionQueueScheduler.HasQueue(executableQueueKeyFn(executable))
}

func (s *ExecutionAwareScheduler) shouldRouteToExecutionQueueScheduler(executable Executable) bool {
	if !s.options.EnableExecutionQueueScheduler() {
		return false
	}
	return s.executionQueueScheduler.HasQueue(executableQueueKeyFn(executable))
}

// executableQueueKeyFn extracts the workflow key from an Executable for queue routing.
func executableQueueKeyFn(e Executable) any {
	return definition.NewWorkflowKey(
		e.GetNamespaceID(),
		e.GetWorkflowID(),
		e.GetRunID(),
	)
}
