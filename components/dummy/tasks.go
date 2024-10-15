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

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeTimer     = "dummy.Timer"
	TaskTypeImmediate = "dummy.Immediate"
)

type ImmediateTask struct {
	destination string
}

var _ hsm.Task = ImmediateTask{}

func (ImmediateTask) Type() string {
	return TaskTypeImmediate
}

func (ImmediateTask) Deadline() time.Time {
	return hsm.Immediate
}

func (t ImmediateTask) Destination() string {
	return t.destination
}

func (ImmediateTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	return hsm.ValidateNotTransitioned(ref, node)
}

type ImmediateTaskSerializer struct{}

func (ImmediateTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return ImmediateTask{destination: attrs.Destination}, nil
}

func (ImmediateTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type TimerTask struct {
	deadline   time.Time
	concurrent bool
}

var _ hsm.Task = TimerTask{}

func (TimerTask) Type() string {
	return TaskTypeTimer
}

func (t TimerTask) Deadline() time.Time {
	return t.deadline
}

func (TimerTask) Destination() string {
	return ""
}

func (t TimerTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	if !t.concurrent {
		return hsm.ValidateNotTransitioned(ref, node)
	}
	return nil
}

type TimerTaskSerializer struct{}

func (TimerTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return TimerTask{deadline: attrs.Deadline, concurrent: len(data) > 0}, nil
}

func (s TimerTaskSerializer) Serialize(task hsm.Task) ([]byte, error) {
	if tt, ok := task.(TimerTask); ok {
		if tt.concurrent {
			// Non empty data marks the task as concurrent.
			return []byte{1}, nil
		}
		// No-op.
		return nil, nil
	}
	return nil, fmt.Errorf("incompatible task: %v", task)
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
