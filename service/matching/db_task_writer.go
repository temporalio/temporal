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
	"context"

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
	dbTaskWriterFuture = future.Future[struct{}]
	dbTaskWriter       interface {
		appendTask(task *persistencespb.TaskInfo) dbTaskWriterFuture
		flushTasks(ctx context.Context)
		notifyFlushChan() <-chan struct{}
	}

	dbTaskInfo struct {
		task   *persistencespb.TaskInfo
		future *future.FutureImpl[struct{}] // nil, error
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
) dbTaskWriterFuture {
	if len(f.taskBuffer) >= dbTaskFlusherBatchSize {
		f.notifyFlush()
	}

	fut := future.NewFuture[struct{}]()
	select {
	case f.taskBuffer <- dbTaskInfo{
		task:   task,
		future: fut,
	}:
		// noop
	default:
		// busy
		fut.Set(struct{}{}, errDBTaskWriterBufferFull)
	}
	return fut
}

func (f *dbTaskWriterImpl) flushTasks(
	ctx context.Context,
) {
	for len(f.taskBuffer) > 0 {
		f.flushTasksOnce(ctx)
	}
}

func (f *dbTaskWriterImpl) flushTasksOnce(
	ctx context.Context,
) {
	tasks := make([]*persistencespb.TaskInfo, 0, dbTaskFlusherBatchSize)
	futures := make([]*future.FutureImpl[struct{}], 0, len(tasks))

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
	err := f.ownership.flushTasks(ctx, tasks...)
	for _, fut := range futures {
		fut.Set(struct{}{}, err)
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
