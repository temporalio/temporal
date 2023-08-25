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

package replication

import (
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableUnknownTask struct {
		ProcessToolBox

		ExecutableTask
		task any
	}
)

const (
	unknownTaskID = "unknown-task-id"
)

var _ ctasks.Task = (*ExecutableUnknownTask)(nil)
var _ TrackableExecutableTask = (*ExecutableUnknownTask)(nil)

func NewExecutableUnknownTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task any,
) *ExecutableUnknownTask {
	return &ExecutableUnknownTask{
		ProcessToolBox: processToolBox,

		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.UnknownTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			"sourceCluster",
		),
		task: task,
	}
}

func (e *ExecutableUnknownTask) QueueID() interface{} {
	return unknownTaskID
}

func (e *ExecutableUnknownTask) Execute() error {
	return serviceerror.NewInvalidArgument(
		fmt.Sprintf("unknown task, ID: %v, task: %v", e.TaskID(), e.task),
	)
}

func (e *ExecutableUnknownTask) HandleErr(err error) error {
	return err
}

func (e *ExecutableUnknownTask) IsRetryableError(err error) bool {
	return false
}

func (e *ExecutableUnknownTask) MarkPoisonPill() error {
	e.Logger.Error("unable to enqueue unknown replication task to DLQ",
		tag.Task(e.task),
		tag.TaskID(e.ExecutableTask.TaskID()),
	)
	return nil
}
