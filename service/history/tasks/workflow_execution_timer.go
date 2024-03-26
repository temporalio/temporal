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
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
)

var _ Task = (*WorkflowRunTimeoutTask)(nil)

type (
	WorkflowExecutionTimeoutTask struct {
		NamespaceID string
		WorkflowID  string
		FirstRunID  string

		VisibilityTimestamp time.Time
		TaskID              int64

		// Check the comment in timerQueueTaskExecutorBase.isValidExecutionTimeoutTask()
		// for why version is not needed here
	}
)

func (t *WorkflowExecutionTimeoutTask) GetNamespaceID() string {
	return t.NamespaceID
}

func (t *WorkflowExecutionTimeoutTask) GetWorkflowID() string {
	return t.WorkflowID
}

func (t *WorkflowExecutionTimeoutTask) GetRunID() string {
	// RunID is empty as the task is not for a specific run but a workflow chain
	return ""
}

func (t *WorkflowExecutionTimeoutTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *WorkflowExecutionTimeoutTask) GetVersion() int64 {
	return common.EmptyVersion
}

func (t *WorkflowExecutionTimeoutTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *WorkflowExecutionTimeoutTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *WorkflowExecutionTimeoutTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *WorkflowExecutionTimeoutTask) SetVisibilityTime(visibilityTime time.Time) {
	t.VisibilityTimestamp = visibilityTime
}

func (t *WorkflowExecutionTimeoutTask) GetCategory() Category {
	return CategoryTimer
}

func (t *WorkflowExecutionTimeoutTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_WORKFLOW_EXECUTION_TIMEOUT
}
