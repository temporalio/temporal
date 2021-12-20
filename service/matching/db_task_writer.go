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

package matching

import (
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

const (
	dbTaskFlusherBatchSize  = 16
	dbTaskFlusherBufferSize = dbTaskFlusherBatchSize * 4
)

var (
	errDBTaskWriterBufferFull = serviceerror.NewUnavailable("dbTaskWriter encountered task buffer full")
)

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination db_task_writer_mock.go

type (
	dbTaskWriter interface {
		appendTask(task *persistencespb.TaskInfo) future.Future
		flushTasks()
		notifyFlushChan() <-chan struct{}
	}

	dbTaskInfo struct {
		task   *persistencespb.TaskInfo
		future *future.FutureImpl // nil, error
	}

	dbTaskWriterImpl struct {
		taskQueueKey persistence.TaskQueueKey
		ownership    dbTaskQueueOwnership
		logger       log.Logger

		flushSignalChan chan struct{}
		taskBuffer      chan dbTaskInfo
	}
)

func newDBTaskWriter(
	taskQueueKey persistence.TaskQueueKey,
	ownership dbTaskQueueOwnership,
	logger log.Logger,
) *dbTaskWriterImpl {
	return &dbTaskWriterImpl{
		taskQueueKey: taskQueueKey,
		ownership:    ownership,
		logger:       logger,

		flushSignalChan: make(chan struct{}, 1),
		taskBuffer:      make(chan dbTaskInfo, dbTaskFlusherBufferSize),
	}
}

func (f *dbTaskWriterImpl) appendTask(
	task *persistencespb.TaskInfo,
) future.Future {
	if len(f.taskBuffer) >= dbTaskFlusherBatchSize {
		f.notifyFlush()
	}

	fut := future.NewFuture()
	select {
	case f.taskBuffer <- dbTaskInfo{
		task:   task,
		future: fut,
	}:
		// noop
	default:
		// busy
		fut.Set(nil, errDBTaskWriterBufferFull)
	}
	return fut
}

func (f *dbTaskWriterImpl) flushTasks() {
	for len(f.taskBuffer) > 0 {
		f.flushTasksOnce()
	}
}

func (f *dbTaskWriterImpl) flushTasksOnce() {
	tasks := make([]*persistencespb.TaskInfo, 0, dbTaskFlusherBatchSize)
	futures := make([]*future.FutureImpl, 0, len(tasks))

FlushLoop:
	for i := 0; i < dbTaskFlusherBatchSize; i++ {
		select {
		case task := <-f.taskBuffer:
			tasks = append(tasks, task.task)
			futures = append(futures, task.future)
		default:
			break FlushLoop
		}
	}

	if len(tasks) == 0 {
		return
	}
	err := f.ownership.flushTasks(tasks...)
	for _, fut := range futures {
		fut.Set(nil, err)
	}
}

func (f *dbTaskWriterImpl) notifyFlush() {
	select {
	case f.flushSignalChan <- struct{}{}:
	default:
		// noop, already notified
	}
}

func (f *dbTaskWriterImpl) notifyFlushChan() <-chan struct{} {
	return f.flushSignalChan
}
