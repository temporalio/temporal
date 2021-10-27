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

	"go.temporal.io/server/common/definition"
)

type (
	WorkflowTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		TaskQueue           string
		ScheduleID          int64
		Version             int64
	}
)

func (d *WorkflowTask) GetKey() Key {
	return Key{
		FireTime: time.Unix(0, 0),
		TaskID:   d.TaskID,
	}
}

func (d *WorkflowTask) GetVersion() int64 {
	return d.Version
}

func (d *WorkflowTask) SetVersion(version int64) {
	d.Version = version
}

func (d *WorkflowTask) GetTaskID() int64 {
	return d.TaskID
}

func (d *WorkflowTask) SetTaskID(id int64) {
	d.TaskID = id
}

func (d *WorkflowTask) GetVisibilityTime() time.Time {
	return d.VisibilityTimestamp
}

func (d *WorkflowTask) SetVisibilityTime(timestamp time.Time) {
	d.VisibilityTimestamp = timestamp
}
