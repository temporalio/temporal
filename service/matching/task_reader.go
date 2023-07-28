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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/internal/goro"
)

const (
	taskReaderOfferThrottleWait  = time.Second
	taskReaderThrottleRetryDelay = 3 * time.Second
)

type (
	taskReader struct {
		status        int32
		taskBuffer    chan *persistencespb.AllocatedTaskInfo // tasks loaded from persistence
		notifyC       chan struct{}                          // Used as signal to notify pump of new tasks
		tlMgr         *taskQueueManagerImpl
		taskValidator taskValidator
		gorogrp       goro.Group

		backoffTimerLock sync.Mutex
		backoffTimer     *time.Timer
		retrier          backoff.Retrier
	}
)

func newTaskReader(tlMgr *taskQueueManagerImpl) *taskReader {
	return &taskReader{
		status:        common.DaemonStatusInitialized,
		tlMgr:         tlMgr,
		taskValidator: newTaskValidator(tlMgr.newIOContext, tlMgr.engine.historyClient),
		notifyC:       make(chan struct{}, 1),
		// we always dequeue the head of the buffer and try to dispatch it to a poller
		// so allocate one less than desired target buffer size
		taskBuffer: make(chan *persistencespb.AllocatedTaskInfo, tlMgr.config.GetTasksBatchSize()-1),
		retrier: backoff.NewRetrier(
			common.CreateReadTaskRetryPolicy(),
			backoff.SystemClock,
		),
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
	ctx = tr.tlMgr.callerInfoContext(ctx)

dispatchLoop:
	for ctx.Err() == nil {
		select {
		case taskInfo, ok := <-tr.taskBuffer:
			if !ok { // Task queue getTasks pump is shutdown
				break dispatchLoop
			}
			task := newInternalTask(taskInfo, tr.tlMgr.completeTask, enumsspb.TASK_SOURCE_DB_BACKLOG, "", false)
			for ctx.Err() == nil {
				if !tr.taskValidator.maybeValidate(taskInfo, tr.tlMgr.taskQueueID.taskType) {
					task.finish(nil)
					tr.taggedMetricsHandler().Counter(metrics.ExpiredTasksPerTaskQueueCounter.GetMetricName()).Record(1)
					// Don't try to set read level here because it may have been advanced already.
					continue dispatchLoop
				}

				taskCtx, cancel := context.WithTimeout(ctx, taskReaderOfferTimeout)
				err := tr.tlMgr.engine.DispatchSpooledTask(taskCtx, task, tr.tlMgr.taskQueueID, tr.tlMgr.stickyInfo)
				cancel()
				if err == nil {
					continue dispatchLoop
				}

				// if task is still valid (truly valid or unable to verify if task is valid)
				tr.taggedMetricsHandler().Counter(metrics.BufferThrottlePerTaskQueueCounter.GetMetricName()).Record(1)
				if !errors.Is(err, errUserDataDisabled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					// Don't log here if encounters missing user data error when dispatch a versioned task.
					tr.throttledLogger().Error("taskReader: unexpected error dispatching task", tag.Error(err))
				}
				common.InterruptibleSleep(ctx, taskReaderOfferThrottleWait)
			}
			return ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}

func (tr *taskReader) getTasksPump(ctx context.Context) error {
	ctx = tr.tlMgr.callerInfoContext(ctx)

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
			batch, err := tr.getTaskBatch(ctx)
			tr.tlMgr.signalIfFatal(err)
			if err != nil {
				// TODO: Should we ever stop retrying on db errors?
				if common.IsResourceExhausted(err) {
					tr.reEnqueueAfterDelay(taskReaderThrottleRetryDelay)
				} else {
					tr.reEnqueueAfterDelay(tr.retrier.NextBackOff())
				}
				continue Loop
			}
			tr.retrier.Reset()

			if len(batch.tasks) == 0 {
				tr.tlMgr.taskAckManager.setReadLevelAfterGap(batch.readLevel)
				if !batch.isReadBatchDone {
					tr.Signal()
				}
				continue Loop
			}

			// only error here is due to context cancellation which we also
			// handle above
			_ = tr.addTasksToBuffer(ctx, batch.tasks)
			// There maybe more tasks. We yield now, but signal pump to check again later.
			tr.Signal()

		case <-updateAckTimer.C:
			err := tr.persistAckLevel(ctx)
			isConditionFailed := tr.tlMgr.signalIfFatal(err)
			if err != nil && !isConditionFailed {
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

func (tr *taskReader) getTaskBatchWithRange(
	ctx context.Context,
	readLevel int64,
	maxReadLevel int64,
) ([]*persistencespb.AllocatedTaskInfo, error) {
	response, err := tr.tlMgr.db.GetTasks(ctx, readLevel+1, maxReadLevel+1, tr.tlMgr.config.GetTasksBatchSize())
	if err != nil {
		return nil, err
	}
	return response.Tasks, err
}

type getTasksBatchResponse struct {
	tasks           []*persistencespb.AllocatedTaskInfo
	readLevel       int64
	isReadBatchDone bool
}

// Returns a batch of tasks from persistence starting form current read level.
// Also return a number that can be used to update readLevel
// Also return a bool to indicate whether read is finished
func (tr *taskReader) getTaskBatch(ctx context.Context) (*getTasksBatchResponse, error) {
	var tasks []*persistencespb.AllocatedTaskInfo
	readLevel := tr.tlMgr.taskAckManager.getReadLevel()
	maxReadLevel := tr.tlMgr.taskWriter.GetMaxReadLevel()

	// counter i is used to break and let caller check whether taskqueue is still alive and need resume read.
	for i := 0; i < 10 && readLevel < maxReadLevel; i++ {
		upper := readLevel + tr.tlMgr.config.RangeSize
		if upper > maxReadLevel {
			upper = maxReadLevel
		}
		tasks, err := tr.getTaskBatchWithRange(ctx, readLevel, upper)
		if err != nil {
			return nil, err
		}
		// return as long as it grabs any tasks
		if len(tasks) > 0 {
			return &getTasksBatchResponse{
				tasks:           tasks,
				readLevel:       upper,
				isReadBatchDone: true,
			}, nil
		}
		readLevel = upper
	}
	return &getTasksBatchResponse{
		tasks:           tasks,
		readLevel:       readLevel,
		isReadBatchDone: readLevel == maxReadLevel,
	}, nil // caller will update readLevel when no task grabbed
}

func (tr *taskReader) addTasksToBuffer(
	ctx context.Context,
	tasks []*persistencespb.AllocatedTaskInfo,
) error {
	for _, t := range tasks {
		if IsTaskExpired(t) {
			tr.taggedMetricsHandler().Counter(metrics.ExpiredTasksPerTaskQueueCounter.GetMetricName()).Record(1)
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

func (tr *taskReader) persistAckLevel(ctx context.Context) error {
	ackLevel := tr.tlMgr.taskAckManager.getAckLevel()
	tr.emitTaskLagMetric(ackLevel)
	return tr.tlMgr.db.UpdateState(ctx, ackLevel)
}

func (tr *taskReader) logger() log.Logger {
	return tr.tlMgr.logger
}

func (tr *taskReader) throttledLogger() log.ThrottledLogger {
	return tr.tlMgr.throttledLogger
}

func (tr *taskReader) taggedMetricsHandler() metrics.Handler {
	return tr.tlMgr.metricsHandler
}

func (tr *taskReader) emitTaskLagMetric(ackLevel int64) {
	// note: this metric is only an estimation for the lag.
	// taskID in DB may not be continuous, especially when task list ownership changes.
	maxReadLevel := tr.tlMgr.taskWriter.GetMaxReadLevel()
	tr.taggedMetricsHandler().Gauge(metrics.TaskLagPerTaskQueueGauge.GetMetricName()).Record(float64(maxReadLevel - ackLevel))
}

func (tr *taskReader) reEnqueueAfterDelay(duration time.Duration) {
	tr.backoffTimerLock.Lock()
	defer tr.backoffTimerLock.Unlock()

	if tr.backoffTimer == nil {
		tr.backoffTimer = time.AfterFunc(duration, func() {
			tr.backoffTimerLock.Lock()
			defer tr.backoffTimerLock.Unlock()

			tr.Signal() // re-enqueue the event
			tr.backoffTimer = nil
		})
	}
}
