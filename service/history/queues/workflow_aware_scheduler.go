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
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
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
		// WorkflowQueueSchedulerQueueSize is the buffer size for the WorkflowQueueScheduler's dispatch channel.
		WorkflowQueueSchedulerQueueSize dynamicconfig.IntPropertyFn
		// WorkflowQueueSchedulerWorkerCount is the number of workers for the WorkflowQueueScheduler.
		WorkflowQueueSchedulerWorkerCount dynamicconfig.TypedSubscribable[int]
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
) *WorkflowAwareScheduler {
	queueSize := 1000 // default
	if options.WorkflowQueueSchedulerQueueSize != nil {
		queueSize = options.WorkflowQueueSchedulerQueueSize()
	}

	workflowQueueScheduler := tasks.NewWorkflowQueueScheduler[Executable](
		&tasks.WorkflowQueueSchedulerOptions{
			QueueSize:   queueSize,
			WorkerCount: options.WorkflowQueueSchedulerWorkerCount,
		},
		executableQueueKeyFn,
		logger,
	)

	return &WorkflowAwareScheduler{
		baseScheduler:       baseScheduler,
		workflowQueueScheduler: workflowQueueScheduler,
		options:             options,
		logger:              logger,
	}
}

func (s *WorkflowAwareScheduler) Start() {
	s.baseScheduler.Start()
	// Always start the WorkflowQueueScheduler regardless of current config.
	// The isWorkflowQueueSchedulerEnabled() check gates task routing, so an idle
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
		// WorkflowQueueScheduler is full, reschedule the task
		executable.Reschedule()
		return true // Return true because we've handled the task (via reschedule)
	}
	return s.baseScheduler.TrySubmit(executable)
}

// HandleBusyWorkflow implements BusyWorkflowHandler.
// It routes a task to the WorkflowQueueScheduler when it encounters a busy workflow error.
// Returns true if the task was handled (either submitted to WorkflowQueueScheduler or rescheduled).
func (s *WorkflowAwareScheduler) HandleBusyWorkflow(executable Executable) bool {
	if !s.isWorkflowQueueSchedulerEnabled() {
		// WorkflowQueueScheduler not enabled, let caller handle it
		return false
	}

	if s.workflowQueueScheduler.TrySubmit(executable) {
		return true
	}
	// WorkflowQueueScheduler is full, reschedule the task
	executable.Reschedule()
	return true
}

// HasWorkflowQueue returns true if the workflow has an active queue in the WorkflowQueueScheduler.
func (s *WorkflowAwareScheduler) HasWorkflowQueue(executable Executable) bool {
	if !s.isWorkflowQueueSchedulerEnabled() {
		return false
	}
	key := getWorkflowKey(executable)
	return s.workflowQueueScheduler.HasQueue(key)
}

func (s *WorkflowAwareScheduler) shouldRouteToWorkflowQueueScheduler(executable Executable) bool {
	if !s.isWorkflowQueueSchedulerEnabled() {
		return false
	}
	key := getWorkflowKey(executable)
	return s.workflowQueueScheduler.HasQueue(key)
}

func (s *WorkflowAwareScheduler) isWorkflowQueueSchedulerEnabled() bool {
	return s.options.EnableWorkflowQueueScheduler != nil && s.options.EnableWorkflowQueueScheduler()
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
