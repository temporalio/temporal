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

var _ Task = (*ArchiveExecutionTask)(nil)

type (
	// ArchiveExecutionTask is the task which archives both the history and visibility of a workflow execution and then
	// produces a retention timer task to delete the data.
	ArchiveExecutionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}
)

func (a *ArchiveExecutionTask) GetKey() Key {
	return NewKey(a.VisibilityTimestamp, a.TaskID)
}

func (a *ArchiveExecutionTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *ArchiveExecutionTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *ArchiveExecutionTask) GetVersion() int64 {
	return a.Version
}

func (a *ArchiveExecutionTask) GetCategory() Category {
	return CategoryArchival
}

func (a *ArchiveExecutionTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION
}

func (a *ArchiveExecutionTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *ArchiveExecutionTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}
