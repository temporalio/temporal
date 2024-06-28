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

	"go.temporal.io/server/service/history/hsm"
)

const TaskType = "test-task-type-name"

var (
	errInvalidTaskType = fmt.Errorf("invalid task type")
)

type Task struct {
	kind         hsm.TaskKind
	IsConcurrent bool
}

func NewTask(
	kind hsm.TaskKind,
	concurrent bool,
) *Task {
	return &Task{
		kind:         kind,
		IsConcurrent: concurrent,
	}
}

func (t *Task) Type() string {
	return TaskType
}

func (t *Task) Kind() hsm.TaskKind {
	return t.kind
}

func (t *Task) Concurrent() bool {
	return t.IsConcurrent
}

type TaskSerializer struct{}

func (s TaskSerializer) Serialize(t hsm.Task) ([]byte, error) {
	if t.Type() != TaskType {
		return nil, errInvalidTaskType
	}
	return json.Marshal(t)
}

func (s TaskSerializer) Deserialize(b []byte, kind hsm.TaskKind) (hsm.Task, error) {
	var t Task
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	t.kind = kind
	return &t, nil
}
