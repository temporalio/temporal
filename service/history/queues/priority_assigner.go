// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"fmt"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasks"
)

type (
	// PriorityAssigner assigns priority to task executables
	PriorityAssigner interface {
		Assign(Executable) tasks.Priority
	}

	PriorityAssignerOptions struct {
		CriticalRetryAttempts int
	}

	priorityAssignerImpl struct {
		currentClusterName string
		namespaceRegistry  namespace.Registry
		options            PriorityAssignerOptions
	}

	// noopPriorityAssigner always assign high priority to tasks
	// it should only be used in tests
	noopPriorityAssigner struct{}
)

func NewPriorityAssigner(
	currentClusterName string,
	namespaceRegistry namespace.Registry,
	options PriorityAssignerOptions,
) PriorityAssigner {
	return &priorityAssignerImpl{
		currentClusterName: currentClusterName,
		namespaceRegistry:  namespaceRegistry,
		options:            options,
	}
}

func (a *priorityAssignerImpl) Assign(executable Executable) tasks.Priority {
	/*
		Summary:
		- High priority: active tasks from active queue processor and no-op tasks (currently ignoring overrides)
		- Default priority: selected task types (e.g. delete history events)
		- Low priority: standby tasks and tasks keep retrying
	*/

	if executable.Attempt() > a.options.CriticalRetryAttempts {
		return tasks.PriorityLow
	}

	namespaceEntry, err := a.namespaceRegistry.GetNamespaceByID(namespace.ID(executable.GetNamespaceID()))
	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceNotFound); ok {
			return tasks.PriorityLow
		}

		panic(fmt.Sprintf("unexpected error from GetNamespaceByID, namespaceID: %v", executable.GetNamespaceID()))
	}

	namespaceActive := namespaceEntry.ActiveInCluster(a.currentClusterName)
	// TODO: remove QueueType() and the special logic for assgining high priority to no-op tasks
	// after merging active/standby queue processor or performing task filtering before submitting
	// tasks to worker pool
	var taskActive bool
	switch executable.QueueType() {
	case QueueTypeActiveTransfer, QueueTypeActiveTimer:
		taskActive = true
	case QueueTypeStandbyTransfer, QueueTypeStandbyTimer:
		taskActive = false
	default:
		taskActive = namespaceActive
	}

	if !taskActive && !namespaceActive {
		// standby tasks
		return tasks.PriorityLow
	}

	if (taskActive && !namespaceActive) || (!taskActive && namespaceActive) {
		// no-op tasks, set to high priority to ack them as soon as possible
		// don't consume rps limit
		// ignoring overrides for some no-op standby tasks for now
		return tasks.PriorityHigh
	}

	// active tasks for active namespaces
	switch executable.GetType() {
	case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
		// add more task types here if we believe it's ok to delay those tasks
		// and assign them the same priority as throttled tasks
		return tasks.PriorityMedium
	}

	return tasks.PriorityHigh
}

func NewNoopPriorityAssigner() PriorityAssigner {
	return &noopPriorityAssigner{}
}

func (a *noopPriorityAssigner) Assign(_ Executable) tasks.Priority {
	return tasks.PriorityHigh
}
