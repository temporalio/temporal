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

package dummy

import (
	"fmt"
	"time"

	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeTimer     = "dummy.Immediate"
	TaskTypeImmediate = "dummy.Timer"
)

type ImmediateTask struct {
	Destination string
}

var _ hsm.Task = ImmediateTask{}

func (ImmediateTask) Type() string {
	return TaskTypeImmediate
}

func (t ImmediateTask) Kind() hsm.TaskKind {
	return hsm.TaskKindOutbound{Destination: t.Destination}
}

func (ImmediateTask) Concurrent() bool {
	return false
}

type ImmediateTaskSerializer struct{}

func (ImmediateTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindOutbound); ok {
		return ImmediateTask{Destination: kind.Destination}, nil
	}
	return nil, fmt.Errorf("%w: expected outbound", hsm.ErrInvalidTaskKind)
}

func (ImmediateTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type TimerTask struct {
	Deadline time.Time
}

var _ hsm.Task = TimerTask{}

func (TimerTask) Type() string {
	return TaskTypeTimer
}

func (t TimerTask) Kind() hsm.TaskKind {
	return hsm.TaskKindTimer{Deadline: t.Deadline}
}

func (TimerTask) Concurrent() bool {
	return false
}

type TimerTaskSerializer struct{}

func (TimerTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindTimer); ok {
		return TimerTask{Deadline: kind.Deadline}, nil
	}
	return nil, fmt.Errorf("%w: expected timer", hsm.ErrInvalidTaskKind)
}

func (TimerTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

func RegisterTaskSerializers(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(
		TaskTypeImmediate,
		ImmediateTaskSerializer{},
	); err != nil {
		return err
	}
	return reg.RegisterTaskSerializer(TaskTypeTimer, TimerTaskSerializer{})
}
