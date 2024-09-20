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
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/tasks"
)

type (
	// PriorityAssigner assigns priority to task executables
	PriorityAssigner interface {
		Assign(Executable) tasks.Priority
	}

	priorityAssignerImpl struct{}

	staticPriorityAssigner struct {
		priority tasks.Priority
	}
)

func NewPriorityAssigner() PriorityAssigner {
	return &priorityAssignerImpl{}
}

func (a *priorityAssignerImpl) Assign(executable Executable) tasks.Priority {
	taskType := executable.GetType()
	switch taskType {
	case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
		enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION,
		enumsspb.TASK_TYPE_UNSPECIFIED:
		// add more task types here if we believe it's ok to delay those tasks
		// and assign them the same priority as throttled tasks
		return tasks.PriorityLow
	}

	if _, ok := enumsspb.TaskType_name[int32(taskType)]; !ok {
		// low priority for unknown task types
		return tasks.PriorityLow
	}

	return tasks.PriorityHigh
}

func NewNoopPriorityAssigner() PriorityAssigner {
	return NewStaticPriorityAssigner(tasks.PriorityHigh)
}

func NewStaticPriorityAssigner(priority tasks.Priority) PriorityAssigner {
	return staticPriorityAssigner{priority: priority}
}

func (a staticPriorityAssigner) Assign(_ Executable) tasks.Priority {
	return a.priority
}
