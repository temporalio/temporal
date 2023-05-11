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

package queues

import (
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

var _ Executable = (*speculativeWorkflowTaskTimeoutExecutable)(nil)

type (
	speculativeWorkflowTaskTimeoutExecutable struct {
		Executable
		workflowTaskTimeoutTask *tasks.WorkflowTaskTimeoutTask
	}
)

func newSpeculativeWorkflowTaskTimeoutExecutable(
	executable Executable,
	workflowTaskTimeoutTask *tasks.WorkflowTaskTimeoutTask,
) *speculativeWorkflowTaskTimeoutExecutable {

	return &speculativeWorkflowTaskTimeoutExecutable{
		Executable:              executable,
		workflowTaskTimeoutTask: workflowTaskTimeoutTask,
	}
}

// ctasks.Task interface overrides.

func (e *speculativeWorkflowTaskTimeoutExecutable) IsRetryableError(_ error) bool {
	// Speculative WT lives in memory representation of mutable state.
	// Almost any error cause workflow context (and mutable state) to be reloaded from database.
	// This clears speculative WT information and retrying corresponding timeout tasks doesn't make sense.
	return false
}

func (e *speculativeWorkflowTaskTimeoutExecutable) Abort() {
}

func (e *speculativeWorkflowTaskTimeoutExecutable) Cancel() {
	e.workflowTaskTimeoutTask.Cancel()
}

func (e *speculativeWorkflowTaskTimeoutExecutable) Ack() {
}

func (e *speculativeWorkflowTaskTimeoutExecutable) Nack(err error) {
}

func (e *speculativeWorkflowTaskTimeoutExecutable) Reschedule() {
}

func (e *speculativeWorkflowTaskTimeoutExecutable) State() ctasks.State {
	return e.workflowTaskTimeoutTask.State()
}
