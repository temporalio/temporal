// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

// CallbackTask is an immediate task scheduled on the callback task queue to deliver a callback to a given destination.
type CallbackTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	Version             int64
	// This destination address for the callback.
	// Used to associate the callback with an executor.
	DestinationAddress string
	// Key in mutable state's callback map.
	CallbackID string
	// The number of callback transitions - should match the mutable state callback info.
	// Used as an indicator for stale mutable state cache or task.
	TransitionCount int32
}

var _ HasDestination = (*CallbackTask)(nil)
var _ Task = (*CallbackTask)(nil)

func (t *CallbackTask) SetWorkflowKey(key definition.WorkflowKey) {
	t.WorkflowKey = key
}

func (t *CallbackTask) GetDestination() string {
	return t.DestinationAddress
}

func (t *CallbackTask) GetTransitionCount() int32 {
	return t.TransitionCount
}

func (t *CallbackTask) GetCallbackID() string {
	return t.CallbackID
}

func (t *CallbackTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *CallbackTask) GetVersion() int64 {
	return t.Version
}

func (t *CallbackTask) SetVersion(version int64) {
	t.Version = version
}

func (t *CallbackTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *CallbackTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *CallbackTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *CallbackTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *CallbackTask) GetCategory() Category {
	return CategoryCallback
}

func (t *CallbackTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_CALLBACK
}
