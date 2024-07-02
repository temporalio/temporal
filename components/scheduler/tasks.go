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
	"fmt"
	"time"

	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeSchedulerWait    = "scheduler.SchedulerWait"
	TaskTypeSchedulerExecute = "scheduler.SchedulerExecute"
)

type SchedulerWaitTask struct {
	Deadline time.Time
}

func (SchedulerWaitTask) Type() string {
	return TaskTypeSchedulerWait
}

func (t SchedulerWaitTask) Kind() hsm.TaskKind {
	return hsm.TaskKindTimer{Deadline: t.Deadline}
}

func (SchedulerWaitTask) Concurrent() bool {
	return false
}

type ScheduleWaitTaskSerializer struct{}

func (ScheduleWaitTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindTimer); ok {
		return SchedulerWaitTask{Deadline: kind.Deadline}, nil
	}
	return nil, fmt.Errorf("%w: expected timer", hsm.ErrInvalidTaskKind)
}

func (ScheduleWaitTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type SchedulerActivateTask struct {
	Destination string
}

var _ hsm.Task = SchedulerActivateTask{}

func (SchedulerActivateTask) Type() string {
	return TaskTypeSchedulerExecute
}

func (t SchedulerActivateTask) Kind() hsm.TaskKind {
	return hsm.TaskKindOutbound{Destination: t.Destination}
}

func (SchedulerActivateTask) Concurrent() bool {
	return false
}

type ScheduleExecuteTaskSerializer struct{}

func (ScheduleExecuteTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindOutbound); ok {
		return SchedulerActivateTask{Destination: kind.Destination}, nil
	}
	return nil, fmt.Errorf("%w: expected outbound", hsm.ErrInvalidTaskKind)
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
