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

package scheduler

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeSchedulerWait    = "scheduler.SchedulerWait"
	TaskTypeSchedulerExecute = "scheduler.SchedulerExecute"
)

type SchedulerWaitTask struct {
	deadline time.Time
}

var _ hsm.Task = SchedulerWaitTask{}

func (SchedulerWaitTask) Type() string {
	return TaskTypeSchedulerWait
}

func (t SchedulerWaitTask) Deadline() time.Time {
	return t.deadline
}

func (SchedulerWaitTask) Destination() string {
	return ""
}

func (SchedulerWaitTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	// TODO: We should allow an RPC to transition the machine.
	if err := hsm.ValidateNotTransitioned(ref, node); err != nil {
		return err
	}
	return node.CheckRunning()
}

type ScheduleWaitTaskSerializer struct{}

func (ScheduleWaitTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return SchedulerWaitTask{deadline: attrs.Deadline}, nil
}

func (ScheduleWaitTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type SchedulerActivateTask struct{}

var _ hsm.Task = SchedulerActivateTask{}

func (SchedulerActivateTask) Type() string {
	return TaskTypeSchedulerExecute
}

func (t SchedulerActivateTask) Deadline() time.Time {
	return hsm.Immediate
}

func (t SchedulerActivateTask) Destination() string {
	return "TODO(bergundy): make this an empty string when we support transfer tasks"
}

func (SchedulerActivateTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	// TODO: We should allow an RPC to transition the machine.
	if err := hsm.ValidateNotTransitioned(ref, node); err != nil {
		return err
	}
	return node.CheckRunning()
}

type ScheduleExecuteTaskSerializer struct{}

func (ScheduleExecuteTaskSerializer) Deserialize(data []byte, kind hsm.TaskAttributes) (hsm.Task, error) {
	return SchedulerActivateTask{}, nil
}

func (ScheduleExecuteTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

func RegisterTaskSerializers(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(TaskTypeSchedulerWait, ScheduleWaitTaskSerializer{}); err != nil {
		return err
	}
	return reg.RegisterTaskSerializer(TaskTypeSchedulerExecute, ScheduleExecuteTaskSerializer{})
}
