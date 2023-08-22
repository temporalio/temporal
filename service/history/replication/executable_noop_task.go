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
	"time"

	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableNoopTask struct {
		ExecutableTask
	}
)

const (
	noopTaskID = "noop-task-id"
)

var _ ctasks.Task = (*ExecutableNoopTask)(nil)
var _ TrackableExecutableTask = (*ExecutableNoopTask)(nil)

func NewExecutableNoopTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	sourceClusterName string,
) *ExecutableNoopTask {
	return &ExecutableNoopTask{
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.NoopTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
		),
	}
}

func (e *ExecutableNoopTask) QueueID() interface{} {
	return noopTaskID
}

func (e *ExecutableNoopTask) Execute() error {
	return nil
}

func (e *ExecutableNoopTask) HandleErr(err error) error {
	return err
}

func (e *ExecutableNoopTask) MarkPoisonPill() error {
	return nil
}
