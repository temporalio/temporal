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

package tasks

import (
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
	ctasks "go.temporal.io/server/common/tasks"
)

const (
	// SpeculativeWorkflowTaskScheduleToStartTimeout is the timeout for a speculative workflow task on a normal task queue.
	// Default ScheduleToStart timeout for a sticky task queue is 5 seconds.
	// Setting this value also to 5 seconds to match the sticky queue timeout.
	SpeculativeWorkflowTaskScheduleToStartTimeout = 5 * time.Second
)

var _ Task = (*WorkflowTaskTimeoutTask)(nil)

type (
	WorkflowTaskTimeoutTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
		ScheduleAttempt     int32
		TimeoutType         enumspb.TimeoutType
		Version             int64

		// state is used by speculative WT only.
		state atomic.Uint32 // of type ctasks.State
	}
)

func (d *WorkflowTaskTimeoutTask) GetKey() Key {
	return NewKey(d.VisibilityTimestamp, d.TaskID)
}

func (d *WorkflowTaskTimeoutTask) GetVersion() int64 {
	return d.Version
}

func (d *WorkflowTaskTimeoutTask) SetVersion(version int64) {
	d.Version = version
}

func (d *WorkflowTaskTimeoutTask) GetTaskID() int64 {
	return d.TaskID
}

func (d *WorkflowTaskTimeoutTask) SetTaskID(id int64) {
	d.TaskID = id
}

func (d *WorkflowTaskTimeoutTask) GetVisibilityTime() time.Time {
	return d.VisibilityTimestamp
}

func (d *WorkflowTaskTimeoutTask) SetVisibilityTime(t time.Time) {
	d.VisibilityTimestamp = t
}

func (d *WorkflowTaskTimeoutTask) GetCategory() Category {
	return CategoryTimer
}

func (d *WorkflowTaskTimeoutTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT
}

// Cancel and State are used by in-memory WorkflowTaskTimeoutTask (for speculative WT) only.
// TODO (alex): They need to be moved to speculativeWorkflowTaskTimeoutExecutable
// and workflowTaskStateMachine should somehow signal that executable directly.
// Major refactoring needs to be done to achieve that.
func (d *WorkflowTaskTimeoutTask) Cancel() {
	d.state.Store(uint32(ctasks.TaskStateCancelled))
}
func (d *WorkflowTaskTimeoutTask) State() ctasks.State {
	return ctasks.State(d.state.Load())
}
