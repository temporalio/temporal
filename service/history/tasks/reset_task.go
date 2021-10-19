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
	ResetWorkflowTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}
)

func (a *ResetWorkflowTask) GetKey() Key {
	return Key{
		FireTime: time.Unix(0, 0),
		TaskID:   a.TaskID,
	}
}

func (a *ResetWorkflowTask) GetVersion() int64 {
	return a.Version
}

func (a *ResetWorkflowTask) SetVersion(version int64) {
	a.Version = version
}

func (a *ResetWorkflowTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *ResetWorkflowTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *ResetWorkflowTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *ResetWorkflowTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}
