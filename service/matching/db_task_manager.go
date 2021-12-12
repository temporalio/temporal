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
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

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
	dbTaskFlushInterval = 24 * time.Millisecond

	dbTaskDeletionInterval = 10 * time.Second

	dbTaskUpdateAckInterval   = time.Minute
	dbTaskUpdateQueueInterval = time.Minute
)

type (
	dbTaskManager struct {
		status             int32
		taskQueueKey       persistence.TaskQueueKey
		store              persistence.TaskManager
		taskQueueOwnership *dbTaskQueueOwnershipImpl
		taskReader         *dbTaskWriter
		taskWriter         *dbTaskReader

		dispatchTaskFn func(*internalTask) error
		finishTaskFn   func(*persistencespb.AllocatedTaskInfo, error)
		logger         log.Logger

		shutdownChan              chan struct{}
		dispatchChan              chan struct{}
		maxDeletedTaskIDInclusive int64 // in mem only
	}
)

func newDBTaskManager(
	taskQueueKey persistence.TaskQueueKey,
	taskQueueKind enumspb.TaskQueueKind,
	taskIDRangeSize int64,
	store persistence.TaskManager,
	logger log.Logger,
	dispatchTaskFn func(*internalTask) error,
	finishTaskFn func(*persistencespb.AllocatedTaskInfo, error),
) (*dbTaskManager, error) {
	taskOwnership := newDBTaskQueueOwnership(
		taskQueueKey,
		taskQueueKind,
		taskIDRangeSize,
		store,
		logger,
	)
	if err := taskOwnership.takeTaskQueueOwnership(); err != nil {
		return nil, err
	}

	return &dbTaskManager{
		status:             common.DaemonStatusInitialized,
		taskQueueKey:       taskQueueKey,
		store:              store,
		taskQueueOwnership: taskOwnership,
		taskReader: newDBTaskWriter(
			taskQueueKey,
			taskOwnership,
			logger,
		),
		taskWriter: newDBTaskReader(
			taskQueueKey,
			store,
			taskOwnership.getAckedTaskID(),
			logger,
		),
		dispatchTaskFn: dispatchTaskFn,
		finishTaskFn:   finishTaskFn,
		logger:         logger,

		shutdownChan:              make(chan struct{}),
		dispatchChan:              make(chan struct{}, 1),
		maxDeletedTaskIDInclusive: taskOwnership.getAckedTaskID(),
	}, nil
}

func (d *dbTaskManager) Start() {
	if !atomic.CompareAndSwapInt32(
		&d.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	d.SignalDispatch()
	go d.readerEventLoop()
	go d.writerEventLoop()
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

func (d *dbTaskManager) SignalDispatch() {
	select {
	case d.dispatchChan <- struct{}{}:
	default: // channel already has an event, don't block
	}
}

func (d *dbTaskManager) isStopped() bool {
	return atomic.LoadInt32(&d.status) == common.DaemonStatusStopped
}

func (d *dbTaskManager) writerEventLoop() {
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
			d.persistTaskQueue()
		case <-flushTicker.C:
			d.taskReader.flushTasks()
			d.SignalDispatch()
		case <-d.taskReader.notifyFlushChan():
			d.taskReader.flushTasks()
			d.SignalDispatch()
		}
	}
}

func (d *dbTaskManager) readerEventLoop() {
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
			d.deleteAckedTasks()
		case <-d.dispatchChan:
			d.readDispatchTask()
		}
	}
}

func (d *dbTaskManager) writeAppendTask(
	task *persistencespb.TaskInfo,
) future.Future {
	return d.taskReader.appendTask(task)
}

func (d *dbTaskManager) readDispatchTask() {
	iter := d.taskWriter.taskIterator(d.taskQueueOwnership.getLastAllocatedTaskID())
	for iter.HasNext() {
		item, err := iter.Next()
		if err != nil {
			d.logger.Error("dbTaskManager encountered error when fetching tasks", tag.Error(err))
			d.SignalDispatch()
			return
		}

		task := item.(*persistencespb.AllocatedTaskInfo)
		d.mustDispatch(task)
	}
}

func (d *dbTaskManager) mustDispatch(
	task *persistencespb.AllocatedTaskInfo,
) {
	for !d.isStopped() {
		if taskqueue.IsTaskExpired(task) {
			d.taskWriter.ackTask(task.TaskId)
			return
		}

		err := d.dispatchTaskFn(newInternalTask(
			task,
			d.finishTaskFn,
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
	ackedTaskID := d.taskWriter.moveAckedTaskID()
	d.taskQueueOwnership.updateAckedTaskID(ackedTaskID)
}

func (d *dbTaskManager) deleteAckedTasks() {
	ackedTaskID := d.taskQueueOwnership.getAckedTaskID()
	if ackedTaskID <= d.maxDeletedTaskIDInclusive {
		return
	}
	_, err := d.store.CompleteTasksLessThan(&persistence.CompleteTasksLessThanRequest{
		NamespaceID:   d.taskQueueKey.NamespaceID,
		TaskQueueName: d.taskQueueKey.TaskQueueName,
		TaskType:      d.taskQueueKey.TaskQueueType,
		TaskID:        ackedTaskID,
		Limit:         100000, // TODO @wxing1292 why delete with limit? history service is not doing similar thing
	})
	if err != nil {
		d.logger.Error("dbTaskManager encountered task deletion error", tag.Error(err))
		return
	}
	d.maxDeletedTaskIDInclusive = ackedTaskID
}

func (d *dbTaskManager) persistTaskQueue() {
	err := d.taskQueueOwnership.persistTaskQueue()
	if err != nil {
		d.logger.Error("dbTaskManager encountered unknown error", tag.Error(err))
	}
}
