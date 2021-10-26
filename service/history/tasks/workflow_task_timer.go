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

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/definition"
)

type (
	WorkflowTaskTimeoutTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
		ScheduleAttempt     int32
		TimeoutType         enumspb.TimeoutType
		Version             int64
	}
)

func (d *WorkflowTaskTimeoutTask) GetKey() Key {
	return Key{
		FireTime: d.VisibilityTimestamp,
		TaskID:   d.TaskID,
	}
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
