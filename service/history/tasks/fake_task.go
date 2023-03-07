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

type (
	FakeTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
		Category            Category
	}
)

func NewFakeTask(
	workflowKey definition.WorkflowKey,
	category Category,
	visibilityTimestamp time.Time,
) Task {
	return &FakeTask{
		WorkflowKey:         workflowKey,
		TaskID:              common.EmptyEventTaskID,
		Version:             common.EmptyVersion,
		VisibilityTimestamp: visibilityTimestamp,
		Category:            category,
	}
}

func (f *FakeTask) GetKey() Key {
	if f.Category.Type() == CategoryTypeImmediate {
		return NewImmediateKey(f.TaskID)
	}
	return NewKey(f.VisibilityTimestamp, f.TaskID)
}

func (f *FakeTask) GetVersion() int64 {
	return f.Version
}

func (f *FakeTask) SetVersion(version int64) {
	f.Version = version
}

func (f *FakeTask) GetTaskID() int64 {
	return f.TaskID
}

func (f *FakeTask) SetTaskID(id int64) {
	f.TaskID = id
}

func (f *FakeTask) GetVisibilityTime() time.Time {
	return f.VisibilityTimestamp
}

func (f *FakeTask) SetVisibilityTime(t time.Time) {
	f.VisibilityTimestamp = t
}

func (f *FakeTask) GetCategory() Category {
	return f.Category
}

func (f *FakeTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_UNSPECIFIED
}
