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

var _ Task = (*ActivityRetryTimerTask)(nil)

type (
	ActivityRetryTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
		Version             int64
		Attempt             int32
	}
)

func (r *ActivityRetryTimerTask) GetKey() Key {
	return NewKey(r.VisibilityTimestamp, r.TaskID)
}

func (r *ActivityRetryTimerTask) GetVersion() int64 {
	return r.Version
}

func (r *ActivityRetryTimerTask) SetVersion(version int64) {
	r.Version = version
}

func (r *ActivityRetryTimerTask) GetTaskID() int64 {
	return r.TaskID
}

func (r *ActivityRetryTimerTask) SetTaskID(id int64) {
	r.TaskID = id
}

func (r *ActivityRetryTimerTask) GetVisibilityTime() time.Time {
	return r.VisibilityTimestamp
}

func (r *ActivityRetryTimerTask) SetVisibilityTime(t time.Time) {
	r.VisibilityTimestamp = t
}

func (r *ActivityRetryTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (r *ActivityRetryTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER
}
