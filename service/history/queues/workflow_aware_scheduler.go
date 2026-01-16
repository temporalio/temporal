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
	"hash/fnv"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/tasks"
)

var (
	_ Scheduler            = (*WorkflowAwareScheduler)(nil)
	_ BusyWorkflowHandler  = (*WorkflowAwareScheduler)(nil)
)

type (
	// BusyWorkflowHandler is an interface for schedulers that can handle busy workflow errors
	// by routing tasks to a sequential scheduler.
	BusyWorkflowHandler interface {
		// HandleBusyWorkflow is called when a task encounters a busy workflow error.
		// It routes the task to the sequential scheduler for sequential processing.
		// Returns true if the task was handled, false if the caller should handle it.
		HandleBusyWorkflow(Executable) bool
	}

	// WorkflowAwareSchedulerOptions contains configuration for the WorkflowAwareScheduler.
	WorkflowAwareSchedulerOptions struct {
		// EnableSequentialScheduler controls whether the sequential scheduler is enabled.
		EnableSequentialScheduler dynamicconfig.BoolPropertyFn
		// SequentialSchedulerQueueSize is the buffer size for the sequential scheduler's dispatch channel.
		SequentialSchedulerQueueSize dynamicconfig.IntPropertyFn
		// SequentialSchedulerWorkerCount is the number of workers for the sequential scheduler.
		SequentialSchedulerWorkerCount dynamicconfig.TypedSubscribable[int]
	}

	// WorkflowAwareScheduler is a scheduler that routes tasks to either a regular scheduler
	// or a sequential per-workflow scheduler based on whether the workflow has an active queue.
	//
	// When a workflow experiences contention (busy workflow error), it gets routed to the
	// sequential scheduler which ensures tasks are processed one at a time per workflow.
	WorkflowAwareScheduler struct {
		baseScheduler       Scheduler
		sequentialScheduler *tasks.WorkflowQueueScheduler[Executable]

		options WorkflowAwareSchedulerOptions
		logger  log.Logger
	}
)

// NewWorkflowAwareScheduler creates a new WorkflowAwareScheduler that wraps the base scheduler
// and adds workflow-aware sequential scheduling for contended workflows.
func NewWorkflowAwareScheduler(
	baseScheduler Scheduler,
	options WorkflowAwareSchedulerOptions,
	logger log.Logger,
) *WorkflowAwareScheduler {
	queueSize := 1000 // default
	if options.SequentialSchedulerQueueSize != nil {
		queueSize = options.SequentialSchedulerQueueSize()
	}

	sequentialScheduler := tasks.NewWorkflowQueueScheduler[Executable](
		&tasks.WorkflowQueueSchedulerOptions{
			QueueSize:   queueSize,
			WorkerCount: options.SequentialSchedulerWorkerCount,
		},
		workflowKeyHashFn,
		workflowTaskQueueFactory,
		logger,
	)

	return &WorkflowAwareScheduler{
		baseScheduler:       baseScheduler,
		sequentialScheduler: sequentialScheduler,
		options:             options,
		logger:              logger,
	}
}

func (s *WorkflowAwareScheduler) Start() {
	s.baseScheduler.Start()
	// Always start the sequential scheduler regardless of current config.
	// The isSequentialSchedulerEnabled() check gates task routing, so an idle
	// scheduler has minimal overhead. This ensures if the dynamic config changes
	// from disabled to enabled, tasks will be processed correctly.
	s.sequentialScheduler.Start()
}

func (s *WorkflowAwareScheduler) Stop() {
	s.baseScheduler.Stop()
	s.sequentialScheduler.Stop()
}

func (s *WorkflowAwareScheduler) Submit(executable Executable) {
	if s.shouldRouteToSequential(executable) {
		s.sequentialScheduler.Submit(executable)
		return
	}
	s.baseScheduler.Submit(executable)
}

func (s *WorkflowAwareScheduler) TrySubmit(executable Executable) bool {
	if s.shouldRouteToSequential(executable) {
		if s.sequentialScheduler.TrySubmit(executable) {
			return true
		}
		// Sequential scheduler is full, reschedule the task
		executable.Reschedule()
		return true // Return true because we've handled the task (via reschedule)
	}
	return s.baseScheduler.TrySubmit(executable)
}

func (s *WorkflowAwareScheduler) TaskChannelKeyFn() TaskChannelKeyFn {
	return s.baseScheduler.TaskChannelKeyFn()
}

// HandleBusyWorkflow implements BusyWorkflowHandler.
// It routes a task to the sequential scheduler when it encounters a busy workflow error.
// Returns true if the task was handled (either submitted to sequential scheduler or rescheduled).
func (s *WorkflowAwareScheduler) HandleBusyWorkflow(executable Executable) bool {
	if !s.isSequentialSchedulerEnabled() {
		// Sequential scheduler not enabled, let caller handle it
		return false
	}

	if s.sequentialScheduler.TrySubmit(executable) {
		return true
	}
	// Sequential scheduler is full, reschedule the task
	executable.Reschedule()
	return true
}

// HasWorkflowQueue returns true if the workflow has an active queue in the sequential scheduler.
func (s *WorkflowAwareScheduler) HasWorkflowQueue(executable Executable) bool {
	if !s.isSequentialSchedulerEnabled() {
		return false
	}
	key := getWorkflowKey(executable)
	return s.sequentialScheduler.HasQueue(key)
}

func (s *WorkflowAwareScheduler) shouldRouteToSequential(executable Executable) bool {
	if !s.isSequentialSchedulerEnabled() {
		return false
	}
	key := getWorkflowKey(executable)
	return s.sequentialScheduler.HasQueue(key)
}

func (s *WorkflowAwareScheduler) isSequentialSchedulerEnabled() bool {
	return s.options.EnableSequentialScheduler != nil && s.options.EnableSequentialScheduler()
}

// getWorkflowKey extracts the workflow key from an executable for queue lookups.
func getWorkflowKey(e Executable) definition.WorkflowKey {
	return definition.NewWorkflowKey(
		e.GetNamespaceID(),
		e.GetWorkflowID(),
		e.GetRunID(),
	)
}

// workflowKeyHashFn is a hash function for workflow keys.
func workflowKeyHashFn(key interface{}) uint32 {
	wfKey, ok := key.(definition.WorkflowKey)
	if !ok {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(wfKey.NamespaceID))
	h.Write([]byte(wfKey.WorkflowID))
	h.Write([]byte(wfKey.RunID))
	return h.Sum32()
}

// workflowTaskQueueFactory creates a task queue for grouping tasks by workflow key.
func workflowTaskQueueFactory(e Executable) tasks.SequentialTaskQueue[Executable] {
	key := getWorkflowKey(e)
	return tasks.NewSequentialTaskQueueImpl[Executable, definition.WorkflowKey](key)
}
