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

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/internal/goro"
	"go.temporal.io/server/service/worker/scanner/taskqueue"
)

const (
	taskReaderOfferThrottleWait = time.Second
)

type (
	taskReader struct {
		status     int32
		taskBuffer chan *persistencespb.AllocatedTaskInfo // tasks loaded from persistence
		notifyC    chan struct{}                          // Used as signal to notify pump of new tasks
		tlMgr      *taskQueueManagerImpl
		gorogrp    goro.Group
	}
)

func newTaskReader(tlMgr *taskQueueManagerImpl) *taskReader {
	return &taskReader{
		status:  common.DaemonStatusInitialized,
		tlMgr:   tlMgr,
		notifyC: make(chan struct{}, 1),
		// we always dequeue the head of the buffer and try to dispatch it to a poller
		// so allocate one less than desired target buffer size
		taskBuffer: make(chan *persistencespb.AllocatedTaskInfo, tlMgr.config.GetTasksBatchSize()-1),
	}
}

// Start reading pump for the given task queue.
// The pump fills up taskBuffer from persistence.
func (tr *taskReader) Start() {
	if !atomic.CompareAndSwapInt32(
		&tr.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	tr.gorogrp.Go(tr.dispatchBufferedTasks)
	tr.gorogrp.Go(tr.getTasksPump)
}

// Stop pump that fills up taskBuffer from persistence.
func (tr *taskReader) Stop() {
	if !atomic.CompareAndSwapInt32(
		&tr.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	tr.gorogrp.Cancel()
}

func (tr *taskReader) Signal() {
	var event struct{}
	select {
	case tr.notifyC <- event:
	default: // channel already has an event, don't block
	}
}

func (tr *taskReader) dispatchBufferedTasks(ctx context.Context) error {
dispatchLoop:
	for {
		select {
		case taskInfo, ok := <-tr.taskBuffer:
			if !ok { // Task queue getTasks pump is shutdown
				break dispatchLoop
			}
			task := newInternalTask(taskInfo, tr.tlMgr.completeTask, enumsspb.TASK_SOURCE_DB_BACKLOG, "", false)
			for {
				err := tr.tlMgr.DispatchTask(ctx, task)
				if err == nil {
					break
				}
				if err == context.Canceled {
					tr.tlMgr.logger.Info("Taskqueue manager context is cancelled, shutting down")
					return err
				}
				// this should never happen unless there is a bug - don't drop the task
				tr.scope().IncCounter(metrics.BufferThrottlePerTaskQueueCounter)
				tr.logger().Error("taskReader: unexpected error dispatching task", tag.Error(err))
				time.Sleep(taskReaderOfferThrottleWait)
			}

		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func (tr *taskReader) getTasksPump(ctx context.Context) error {
	if err := tr.tlMgr.WaitUntilInitialized(ctx); err != nil {
		return err
	}

	updateAckTimer := time.NewTimer(tr.tlMgr.config.UpdateAckInterval())
	defer updateAckTimer.Stop()

	tr.Signal() // prime pump
Loop:
	for {
		// Prioritize exiting over other processing
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		select {
		case <-ctx.Done():
			return nil

		case <-tr.notifyC:
			tasks, readLevel, isReadBatchDone, err := tr.getTaskBatch()
			tr.tlMgr.signalIfFatal(err)
			if err != nil {
				tr.Signal() // re-enqueue the event
				// TODO: Should we ever stop retrying on db errors?
				continue Loop
			}

			if len(tasks) == 0 {
				tr.tlMgr.taskAckManager.setReadLevelAfterGap(readLevel)
				if !isReadBatchDone {
					tr.Signal()
				}
				continue Loop
			}

			// only error here is due to context cancelation which we also
			// handle above
			tr.addTasksToBuffer(ctx, tasks)
			// There maybe more tasks. We yield now, but signal pump to check again later.
			tr.Signal()

		case <-updateAckTimer.C:
			err := tr.persistAckLevel()
			tr.tlMgr.signalIfFatal(err)
			if err != nil {
				tr.logger().Error("Persistent store operation failure",
					tag.StoreOperationUpdateTaskQueue,
					tag.Error(err))
				// keep going as saving ack is not critical
			}
			tr.Signal() // periodically signal pump to check persistence for tasks
			updateAckTimer = time.NewTimer(tr.tlMgr.config.UpdateAckInterval())
		}
	}
}

func (tr *taskReader) getTaskBatchWithRange(readLevel int64, maxReadLevel int64) ([]*persistencespb.AllocatedTaskInfo, error) {
	var response *persistence.GetTasksResponse
	var err error
	err = executeWithRetry(context.TODO(), func(ctx context.Context) error {
		response, err = tr.tlMgr.db.GetTasks(ctx, readLevel+1, maxReadLevel+1, tr.tlMgr.config.GetTasksBatchSize())
		return err
	})
	if err != nil {
		return nil, err
	}
	return response.Tasks, err
}

// Returns a batch of tasks from persistence starting form current read level.
// Also return a number that can be used to update readLevel
// Also return a bool to indicate whether read is finished
func (tr *taskReader) getTaskBatch() ([]*persistencespb.AllocatedTaskInfo, int64, bool, error) {
	var tasks []*persistencespb.AllocatedTaskInfo
	readLevel := tr.tlMgr.taskAckManager.getReadLevel()
	maxReadLevel := tr.tlMgr.taskWriter.GetMaxReadLevel()

	// counter i is used to break and let caller check whether taskqueue is still alive and need resume read.
	for i := 0; i < 10 && readLevel < maxReadLevel; i++ {
		upper := readLevel + tr.tlMgr.config.RangeSize
		if upper > maxReadLevel {
			upper = maxReadLevel
		}
		tasks, err := tr.getTaskBatchWithRange(readLevel, upper)
		if err != nil {
			return nil, readLevel, true, err
		}
		// return as long as it grabs any tasks
		if len(tasks) > 0 {
			return tasks, upper, true, nil
		}
		readLevel = upper
	}
	return tasks, readLevel, readLevel == maxReadLevel, nil // caller will update readLevel when no task grabbed
}

func (tr *taskReader) addTasksToBuffer(
	ctx context.Context,
	tasks []*persistencespb.AllocatedTaskInfo,
) error {
	for _, t := range tasks {
		if taskqueue.IsTaskExpired(t) {
			tr.scope().IncCounter(metrics.ExpiredTasksPerTaskQueueCounter)
			// Also increment readLevel for expired tasks otherwise it could result in
			// looping over the same tasks if all tasks read in the batch are expired
			tr.tlMgr.taskAckManager.setReadLevel(t.GetTaskId())
			continue
		}
		if err := tr.addSingleTaskToBuffer(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (tr *taskReader) addSingleTaskToBuffer(
	ctx context.Context,
	task *persistencespb.AllocatedTaskInfo,
) error {
	tr.tlMgr.taskAckManager.addTask(task.GetTaskId())
	select {
	case tr.taskBuffer <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (tr *taskReader) persistAckLevel() error {
	ackLevel := tr.tlMgr.taskAckManager.getAckLevel()
	tr.emitTaskLagMetric(ackLevel)
	return tr.tlMgr.db.UpdateState(context.TODO(), ackLevel)
}

func (tr *taskReader) isTaskAddedRecently(lastAddTime time.Time) bool {
	return time.Now().UTC().Sub(lastAddTime) <= tr.tlMgr.config.MaxTaskqueueIdleTime()
}

func (tr *taskReader) logger() log.Logger {
	return tr.tlMgr.logger
}

func (tr *taskReader) scope() metrics.Scope {
	return tr.tlMgr.metricScope
}

func (tr *taskReader) emitTaskLagMetric(ackLevel int64) {
	// note: this metric is only an estimation for the lag.
	// taskID in DB may not be continuous, especially when task list ownership changes.
	maxReadLevel := tr.tlMgr.taskWriter.GetMaxReadLevel()
	tr.scope().UpdateGauge(metrics.TaskLagPerTaskQueueGauge, float64(maxReadLevel-ackLevel))
}
