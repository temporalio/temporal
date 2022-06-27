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
	"go.temporal.io/server/common/definition"
)

var _ Task = (*DeleteExecutionTask)(nil)

type (
	DeleteExecutionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
	}
)

func (a *DeleteExecutionTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *DeleteExecutionTask) GetVersion() int64 {
	// Version is not used for DeleteExecutionTask transfer task because it is created only for
	// explicit API call, and in this case execution needs to be deleted regardless of the version.
	return common.EmptyVersion
}

func (a *DeleteExecutionTask) SetVersion(_ int64) {
}

func (a *DeleteExecutionTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *DeleteExecutionTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *DeleteExecutionTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *DeleteExecutionTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *DeleteExecutionTask) GetCategory() Category {
	return CategoryTransfer
}

func (a *DeleteExecutionTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION
}
