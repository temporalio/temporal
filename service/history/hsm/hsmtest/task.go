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

package hsmtest

import (
	"encoding/json"
	"fmt"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

const TaskType = "test-task-type-name"

var (
	errInvalidTaskType = fmt.Errorf("invalid task type")
)

type Task struct {
	attrs        hsm.TaskAttributes
	IsConcurrent bool
}

func NewTask(
	attrs hsm.TaskAttributes,
	concurrent bool,
) *Task {
	return &Task{
		attrs:        attrs,
		IsConcurrent: concurrent,
	}
}

func (t *Task) Type() string {
	return TaskType
}

func (t *Task) Deadline() time.Time {
	return t.attrs.Deadline
}

func (t *Task) Destination() string {
	return t.attrs.Destination
}

func (t *Task) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	if t.IsConcurrent {
		return hsm.ValidateNotTransitioned(ref, node)
	}
	return nil
}

type TaskSerializer struct{}

func (s TaskSerializer) Serialize(t hsm.Task) ([]byte, error) {
	if t.Type() != TaskType {
		return nil, errInvalidTaskType
	}
	return json.Marshal(t)
}

func (s TaskSerializer) Deserialize(b []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	var t Task
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	t.attrs = attrs
	return &t, nil
}
