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

package queuestest

import (
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

// FakeExecutable is a fake queues.Executable that returns the given task and error upon GetTask and Execute,
// respectively, and it also records the number of calls.
type FakeExecutable struct {
	queues.Executable
	err   error
	task  tasks.Task
	calls int
}

func NewFakeExecutable(task tasks.Task, err error) *FakeExecutable {
	return &FakeExecutable{task: task, err: err}
}

func (e *FakeExecutable) Execute() error {
	e.calls++
	return e.err
}

func (e *FakeExecutable) GetTask() tasks.Task {
	return e.task
}

func (e *FakeExecutable) GetCalls() int {
	return e.calls
}
