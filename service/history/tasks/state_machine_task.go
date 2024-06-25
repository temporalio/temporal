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

	enums "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
)

// StateMachineTask is a base for all tasks emitted by hierarchical state machines.
type StateMachineTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	Info                *persistence.StateMachineTaskInfo
}

var _ HasStateMachineTaskType = &StateMachineTask{}

func (t *StateMachineTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *StateMachineTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *StateMachineTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *StateMachineTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *StateMachineTask) StateMachineTaskType() string {
	return t.Info.Type
}

// StateMachineOutboundTask is a task on the outbound queue.
type StateMachineOutboundTask struct {
	StateMachineTask
	Destination string
}

// GetDestination is used for grouping outbound tasks into a per source namespace and destination scheduler and in multi-cursor predicates.
func (t *StateMachineOutboundTask) GetDestination() string {
	return t.Destination
}

func (*StateMachineOutboundTask) GetCategory() Category {
	return CategoryOutbound
}

func (*StateMachineOutboundTask) GetType() enums.TaskType {
	return enums.TASK_TYPE_STATE_MACHINE_OUTBOUND
}

func (t *StateMachineOutboundTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

var _ Task = &StateMachineOutboundTask{}
var _ HasDestination = &StateMachineOutboundTask{}

// StateMachineCallbackTask is a generic timer task that can be emitted by any hierarchical state machine.
type StateMachineTimerTask struct {
	definition.WorkflowKey
	VisibilityTimestamp time.Time
	TaskID              int64
	Version             int64
}

func (*StateMachineTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (*StateMachineTimerTask) GetType() enums.TaskType {
	return enums.TASK_TYPE_STATE_MACHINE_TIMER
}

func (t *StateMachineTimerTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *StateMachineTimerTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *StateMachineTimerTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *StateMachineTimerTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *StateMachineTimerTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

var _ Task = &StateMachineTimerTask{}
