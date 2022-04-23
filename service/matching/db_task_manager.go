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
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/worker/scanner/taskqueue"
)

const (
	dbTaskInitialRangeID     = 1
	dbTaskStickyTaskQueueTTL = 24 * time.Hour

	dbTaskFlushInterval = 24 * time.Millisecond

	dbTaskDeletionInterval = 10 * time.Second

	dbTaskUpdateAckInterval   = time.Minute
	dbTaskUpdateQueueInterval = time.Minute
)

var (
	errDBTaskManagerNotReady = serviceerror.NewUnavailable("dbTaskManager is not ready")
)

type (
	taskQueueOwnershipProviderFn func() dbTaskQueueOwnership
	taskReaderProviderFn         func(ownership dbTaskQueueOwnership) dbTaskReader
	taskWriterProviderFn         func(ownership dbTaskQueueOwnership) dbTaskWriter

	dbTaskManager struct {
		status                     int32
		taskQueueKey               persistence.TaskQueueKey
		taskQueueKind              enumspb.TaskQueueKind
		taskIDRangeSize            int64
		taskQueueOwnershipProvider taskQueueOwnershipProviderFn
		taskReaderProvider         taskReaderProviderFn
		taskWriterProvider         taskWriterProviderFn
		dispatchTaskFn             func(context.Context, *internalTask) error
		store                      persistence.TaskManager
		logger                     log.Logger

		dispatchChan chan struct{}
		startupChan  chan struct{}
		shutdownChan chan struct{}

		taskQueueOwnership        dbTaskQueueOwnership
		taskReader                dbTaskReader
		taskWriter                dbTaskWriter
		maxDeletedTaskIDInclusive int64 // in mem only
	}
)

func newDBTaskManager(
	taskQueueKey persistence.TaskQueueKey,
	taskQueueKind enumspb.TaskQueueKind,
	taskIDRangeSize int64,
	dispatchTaskFn func(context.Context, *internalTask) error,
	store persistence.TaskManager,
	logger log.Logger,
) *dbTaskManager {
	return &dbTaskManager{
		status:          common.DaemonStatusInitialized,
		taskQueueKey:    taskQueueKey,
		taskQueueKind:   taskQueueKind,
		taskIDRangeSize: taskIDRangeSize,
		taskQueueOwnershipProvider: func() dbTaskQueueOwnership {
			return newDBTaskQueueOwnership(
				taskQueueKey,
				taskQueueKind,
				taskIDRangeSize,
				store,
				logger,
			)
		},
		taskReaderProvider: func(taskQueueOwnership dbTaskQueueOwnership) dbTaskReader {
			return newDBTaskReader(
				taskQueueKey,
				store,
				taskQueueOwnership.getAckedTaskID(),
				logger,
			)
		},
		taskWriterProvider: func(taskQueueOwnership dbTaskQueueOwnership) dbTaskWriter {
			return newDBTaskWriter(
				taskQueueKey,
				taskQueueOwnership,
				logger,
			)
		},
		dispatchTaskFn: dispatchTaskFn,
		store:          store,
		logger:         logger,

		dispatchChan: make(chan struct{}, 1),
		startupChan:  make(chan struct{}),
		shutdownChan: make(chan struct{}),

		taskQueueOwnership:        nil,
		taskWriter:                nil,
		taskReader:                nil,
		maxDeletedTaskIDInclusive: 0,
	}
}

func (d *dbTaskManager) Start() {
	if !atomic.CompareAndSwapInt32(
		&d.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	d.signalDispatch()
	go d.acquireLoop(context.TODO())
	go d.writerEventLoop(context.TODO())
	go d.readerEventLoop(context.TODO())
}

func (d *dbTaskManager) Stop() {
	if !atomic.CompareAndSwapInt32(
		&d.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(d.shutdownChan)
}

func (d *dbTaskManager) isStopped() bool {
	return atomic.LoadInt32(&d.status) == common.DaemonStatusStopped
}

func (d *dbTaskManager) acquireLoop(
	ctx context.Context,
) {
	defer close(d.startupChan)

AcquireLoop:
	for !d.isStopped() {
		err := d.acquireOwnership(ctx)
		if err == nil {
			break AcquireLoop
		}
		if !common.IsPersistenceTransientError(err) {
			d.Stop()
			break AcquireLoop
		}
		time.Sleep(2 * time.Second)
	}
}

func (d *dbTaskManager) writerEventLoop(
	ctx context.Context,
) {
	<-d.startupChan

	updateQueueTicker := time.NewTicker(dbTaskUpdateQueueInterval)
	defer updateQueueTicker.Stop()
	// TODO we should impl a more efficient method to
	//  buffer & wait for max duration
	//  right now simply just flush every dbTaskFlushInterval
	flushTicker := time.NewTicker(dbTaskFlushInterval)
	defer flushTicker.Stop()

	for {
		if d.isStopped() {
			return
		}

		select {
		case <-d.shutdownChan:
			return
		case <-d.taskQueueOwnership.getShutdownChan():
			d.Stop()

		case <-updateQueueTicker.C:
			d.persistTaskQueue(ctx)
		case <-flushTicker.C:
			d.taskWriter.flushTasks(ctx)
			d.signalDispatch()
		case <-d.taskWriter.notifyFlushChan():
			d.taskWriter.flushTasks(ctx)
			d.signalDispatch()
		}
	}
}

func (d *dbTaskManager) readerEventLoop(
	ctx context.Context,
) {
	<-d.startupChan

	updateAckTicker := time.NewTicker(dbTaskUpdateAckInterval)
	defer updateAckTicker.Stop()

	dbTaskAckTicker := time.NewTicker(dbTaskDeletionInterval)
	defer dbTaskAckTicker.Stop()

	for {
		if d.isStopped() {
			return
		}

		select {
		case <-d.shutdownChan:
			return
		case <-d.taskQueueOwnership.getShutdownChan():
			d.Stop()

		case <-updateAckTicker.C:
			d.updateAckTaskID()
		case <-dbTaskAckTicker.C:
			d.deleteAckedTasks(ctx)
		case <-d.dispatchChan:
			d.readAndDispatchTasks(ctx)
		}
	}
}

func (d *dbTaskManager) acquireOwnership(
	ctx context.Context,
) error {
	taskQueueOwnership := d.taskQueueOwnershipProvider()
	if err := taskQueueOwnership.takeTaskQueueOwnership(ctx); err != nil {
		return err
	}
	d.taskReader = d.taskReaderProvider(taskQueueOwnership)
	d.taskWriter = d.taskWriterProvider(taskQueueOwnership)
	d.maxDeletedTaskIDInclusive = taskQueueOwnership.getAckedTaskID()
	d.taskQueueOwnership = taskQueueOwnership
	return nil
}

func (d *dbTaskManager) signalDispatch() {
	select {
	case d.dispatchChan <- struct{}{}:
	default: // channel already has an event, don't block
	}
}

func (d *dbTaskManager) BufferAndWriteTask(
	task *persistencespb.TaskInfo,
) dbTaskWriterFuture {
	select {
	case <-d.startupChan:
		if d.isStopped() {
			return future.NewReadyFuture[struct{}](struct{}{}, errDBTaskManagerNotReady)
		}
		return d.taskWriter.appendTask(task)
	default:
		return future.NewReadyFuture[struct{}](struct{}{}, errDBTaskManagerNotReady)
	}
}

func (d *dbTaskManager) readAndDispatchTasks(
	ctx context.Context,
) {
	iter := d.taskReader.taskIterator(ctx, d.taskQueueOwnership.getLastAllocatedTaskID())
	for iter.HasNext() {
		task, err := iter.Next()
		if err != nil {
			d.logger.Error("dbTaskManager encountered error when fetching tasks", tag.Error(err))
			d.signalDispatch()
			return
		}

		d.mustDispatch(task)
	}
}

func (d *dbTaskManager) mustDispatch(
	task *persistencespb.AllocatedTaskInfo,
) {
	for !d.isStopped() {
		if taskqueue.IsTaskExpired(task) {
			d.taskReader.ackTask(task.TaskId)
			return
		}

		err := d.dispatchTaskFn(context.Background(), newInternalTask(
			task,
			d.finishTask,
			enumsspb.TASK_SOURCE_DB_BACKLOG,
			"",
			false,
		))
		if err == nil {
			return
		}
		d.logger.Error("dbTaskManager unable to dispatch task", tag.Task(task), tag.Error(err))
	}
}

func (d *dbTaskManager) updateAckTaskID() {
	ackedTaskID := d.taskReader.moveAckedTaskID()
	d.taskQueueOwnership.updateAckedTaskID(ackedTaskID)
}

func (d *dbTaskManager) deleteAckedTasks(
	ctx context.Context,
) {
	ackedTaskID := d.taskQueueOwnership.getAckedTaskID()
	if ackedTaskID <= d.maxDeletedTaskIDInclusive {
		return
	}
	_, err := d.store.CompleteTasksLessThan(ctx, &persistence.CompleteTasksLessThanRequest{
		NamespaceID:        d.taskQueueKey.NamespaceID,
		TaskQueueName:      d.taskQueueKey.TaskQueueName,
		TaskType:           d.taskQueueKey.TaskQueueType,
		ExclusiveMaxTaskID: ackedTaskID + 1,
		Limit:              100000, // TODO @wxing1292 why delete with limit? history service is not doing similar thing
	})
	if err != nil {
		d.logger.Error("dbTaskManager encountered task deletion error", tag.Error(err))
		return
	}
	d.maxDeletedTaskIDInclusive = ackedTaskID
}

func (d *dbTaskManager) persistTaskQueue(
	ctx context.Context,
) {
	err := d.taskQueueOwnership.persistTaskQueue(ctx)
	if err != nil {
		d.logger.Error("dbTaskManager encountered unknown error", tag.Error(err))
	}
}

func (d *dbTaskManager) finishTask(
	info *persistencespb.AllocatedTaskInfo,
	err error,
) {
	if err == nil {
		d.taskReader.ackTask(info.TaskId)
		return
	}

	// TODO @wxing1292 logic below is subject to discussion
	//  NOTE: logic below is legacy logic, which will move task with error
	//  to the end of the queue for later retry
	//
	// failed to start the task.
	// We cannot just remove it from persistence because then it will be lost.
	// We handle this by writing the task back to persistence with a higher taskID.
	// This will allow subsequent tasks to make progress, and hopefully by the time this task is picked-up
	// again the underlying reason for failing to start will be resolved.
	// Note that RecordTaskStarted only fails after retrying for a long time, so a single task will not be
	// re-written to persistence frequently.
	_, err = d.BufferAndWriteTask(info.Data).Get(context.Background())
	if err != nil {
		d.logger.Error("dbTaskManager encountered error when moving task to end of task queue",
			tag.Error(err),
			tag.WorkflowTaskQueueName(d.taskQueueKey.TaskQueueName),
			tag.WorkflowTaskQueueType(d.taskQueueKey.TaskQueueType))
		d.Stop()
		return
	}
	d.taskReader.ackTask(info.TaskId)
}
