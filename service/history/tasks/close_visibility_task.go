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
	"go.temporal.io/server/common/definition"
)

var _ Task = (*CloseExecutionVisibilityTask)(nil)

type (
	CloseExecutionVisibilityTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}
)

func (t *CloseExecutionVisibilityTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *CloseExecutionVisibilityTask) GetVersion() int64 {
	return t.Version
}

func (t *CloseExecutionVisibilityTask) SetVersion(version int64) {
	t.Version = version
}

func (t *CloseExecutionVisibilityTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *CloseExecutionVisibilityTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *CloseExecutionVisibilityTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *CloseExecutionVisibilityTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *CloseExecutionVisibilityTask) GetCategory() Category {
	return CategoryVisibility
}

func (t *CloseExecutionVisibilityTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION
}
