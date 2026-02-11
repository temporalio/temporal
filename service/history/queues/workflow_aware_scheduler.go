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

package queues

import (
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tasks"
)

var _ tasks.Scheduler[Executable] = (*WorkflowAwareScheduler)(nil)

type (
	// BusyWorkflowHandler is an interface for schedulers that can handle busy workflow errors
	// by routing tasks to a WorkflowQueueScheduler.
	BusyWorkflowHandler interface {
		// HandleBusyWorkflow is called when a task encounters a busy workflow error.
		// It routes the task to the WorkflowQueueScheduler for sequential processing.
		// Returns true if the task was handled, false if the caller should handle it.
		HandleBusyWorkflow(Executable) bool
	}

	// WorkflowAwareSchedulerOptions contains configuration for the WorkflowAwareScheduler.
	WorkflowAwareSchedulerOptions struct {
		// EnableWorkflowQueueScheduler controls whether the WorkflowQueueScheduler is enabled.
		EnableWorkflowQueueScheduler dynamicconfig.BoolPropertyFn
		// WorkflowQueueSchedulerMaxQueues is the maximum number of concurrent per-workflow queues.
		WorkflowQueueSchedulerMaxQueues dynamicconfig.IntPropertyFn
		// WorkflowQueueSchedulerQueueTTL is how long a queue goroutine waits idle before exiting.
		WorkflowQueueSchedulerQueueTTL dynamicconfig.DurationPropertyFn
		// WorkflowQueueSchedulerQueueConcurrency is the max workers per queue.
		// Defaults to 1 (sequential) if nil.
		WorkflowQueueSchedulerQueueConcurrency dynamicconfig.IntPropertyFn
	}

	// WorkflowAwareScheduler is a scheduler that wraps a base FIFO scheduler and adds
	// a WorkflowQueueScheduler for handling workflow contention.
	//
	// This scheduler implements tasks.Scheduler[Executable] and is designed to be
	// passed to the InterleavedWeightedRoundRobinScheduler as the underlying processor.
	//
	// By default, tasks are processed by the base FIFO scheduler. When a workflow experiences
	// contention (busy workflow error), it gets routed to the WorkflowQueueScheduler which
	// ensures tasks are processed one at a time per workflow.
	WorkflowAwareScheduler struct {
		baseScheduler          tasks.Scheduler[Executable]
		workflowQueueScheduler *tasks.WorkflowQueueScheduler[Executable]

		options WorkflowAwareSchedulerOptions
		logger  log.Logger
	}
)

// NewWorkflowAwareScheduler creates a new WorkflowAwareScheduler that wraps a base FIFO scheduler
// and adds a WorkflowQueueScheduler for handling workflow contention.
//
// This returns a tasks.Scheduler[Executable] that can be passed to
// InterleavedWeightedRoundRobinScheduler.
func NewWorkflowAwareScheduler(
	baseScheduler tasks.Scheduler[Executable],
	options WorkflowAwareSchedulerOptions,
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
) *WorkflowAwareScheduler {
	workflowQueueScheduler := tasks.NewWorkflowQueueScheduler(
		&tasks.WorkflowQueueSchedulerOptions{
			MaxQueues:        options.WorkflowQueueSchedulerMaxQueues,
			QueueTTL:         options.WorkflowQueueSchedulerQueueTTL,
			QueueConcurrency: options.WorkflowQueueSchedulerQueueConcurrency,
		},
		executableQueueKeyFn,
		logger,
		metricsHandler,
		timeSource,
	)

	s := &WorkflowAwareScheduler{
		baseScheduler:          baseScheduler,
		workflowQueueScheduler: workflowQueueScheduler,
		options:                options,
		logger:                 logger,
	}

	return s
}

func (s *WorkflowAwareScheduler) Start() {
	s.baseScheduler.Start()
	// Always start the WorkflowQueueScheduler regardless of current config.
	// The EnableWorkflowQueueScheduler check gates task routing, so an idle
	// scheduler has minimal overhead. This ensures if the dynamic config changes
	// from disabled to enabled, tasks will be processed correctly.
	s.workflowQueueScheduler.Start()
}

func (s *WorkflowAwareScheduler) Stop() {
	s.baseScheduler.Stop()
	s.workflowQueueScheduler.Stop()
}

func (s *WorkflowAwareScheduler) Submit(executable Executable) {
	if s.shouldRouteToWorkflowQueueScheduler(executable) {
		s.workflowQueueScheduler.Submit(executable)
		return
	}
	s.baseScheduler.Submit(executable)
}

func (s *WorkflowAwareScheduler) TrySubmit(executable Executable) bool {
	if s.shouldRouteToWorkflowQueueScheduler(executable) {
		if s.workflowQueueScheduler.TrySubmit(executable) {
			return true
		}
		// WorkflowQueueScheduler is full, fall through to base scheduler.
	}
	return s.baseScheduler.TrySubmit(executable)
}

// HandleBusyWorkflow implements BusyWorkflowHandler.
// It routes a task to the WorkflowQueueScheduler when it encounters a busy workflow error.
// Returns true if the task was handled (submitted to a scheduler), false if caller should handle it.
func (s *WorkflowAwareScheduler) HandleBusyWorkflow(executable Executable) bool {
	if !s.options.EnableWorkflowQueueScheduler() {
		// WorkflowQueueScheduler not enabled, let caller handle it
		return false
	}

	if s.workflowQueueScheduler.TrySubmit(executable) {
		return true
	}
	// WorkflowQueueScheduler is full, fall back to base scheduler.
	return s.baseScheduler.TrySubmit(executable)
}

// HasWorkflowQueue returns true if the workflow has an active queue in the WorkflowQueueScheduler.
func (s *WorkflowAwareScheduler) HasWorkflowQueue(executable Executable) bool {
	if !s.options.EnableWorkflowQueueScheduler() {
		return false
	}
	key := getWorkflowKey(executable)
	return s.workflowQueueScheduler.HasQueue(key)
}

func (s *WorkflowAwareScheduler) shouldRouteToWorkflowQueueScheduler(executable Executable) bool {
	if !s.options.EnableWorkflowQueueScheduler() {
		return false
	}
	key := getWorkflowKey(executable)
	return s.workflowQueueScheduler.HasQueue(key)
}

// getWorkflowKey extracts the workflow key from an executable for queue lookups.
func getWorkflowKey(e Executable) definition.WorkflowKey {
	return definition.NewWorkflowKey(
		e.GetNamespaceID(),
		e.GetWorkflowID(),
		e.GetRunID(),
	)
}

// executableQueueKeyFn extracts the workflow key from an Executable for queue routing.
func executableQueueKeyFn(e Executable) any {
	return getWorkflowKey(e)
}
