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

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/internal/goro"
	"golang.org/x/sync/semaphore"
)

const (
	taskReaderThrottleRetryDelay = 3 * time.Second

	// Load more tasks when loaded count is <= MaxBatchSize/reloadFraction.
	// E.g. if MaxBatchSize is 1000, then we'll load 1000, dispatch down to 200,
	// load another batch to make 1200, down to 200, etc.
	reloadFraction = 5 // TODO(pri): make dynamic config

	concurrentAddRetries = 10
)

type (
	taskReader struct {
		status     int32
		notifyC    chan struct{} // Used as signal to notify pump of new tasks
		backlogMgr *backlogManagerImpl
		gorogrp    goro.Group

		backoffTimerLock sync.Mutex
		backoffTimer     *time.Timer
		retrier          backoff.Retrier
		loadedTasks      atomic.Int64

		backlogAgeLock sync.Mutex
		backlogAge     backlogAgeTracker

		addRetries *semaphore.Weighted
	}
)

var addErrorRetryPolicy = backoff.NewExponentialRetryPolicy(2 * time.Second).
	WithExpirationInterval(backoff.NoInterval)

func newTaskReader(backlogMgr *backlogManagerImpl) *taskReader {
	tr := &taskReader{
		status:     common.DaemonStatusInitialized,
		backlogMgr: backlogMgr,
		notifyC:    make(chan struct{}, 1),
		retrier: backoff.NewRetrier(
			common.CreateReadTaskRetryPolicy(),
			clock.NewRealTimeSource(),
		),
		backlogAge: newBacklogAgeTracker(),
		addRetries: semaphore.NewWeighted(concurrentAddRetries),
	}
	return tr
}

// Start taskReader background goroutines.
func (tr *taskReader) Start() {
	if !atomic.CompareAndSwapInt32(
		&tr.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	tr.gorogrp.Go(tr.getTasksPump)
}

// Stop taskReader goroutines.
// Note that this does not wait until they stop before returning.
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
	select {
	case tr.notifyC <- struct{}{}:
	default: // channel already has an event, don't block
	}
}

func (tr *taskReader) getBacklogHeadAge() time.Duration {
	tr.backlogAgeLock.Lock()
	defer tr.backlogAgeLock.Unlock()
	return max(0, tr.backlogAge.getAge()) // return 0 instead of -1
}

func (tr *taskReader) completeTask(task *persistencespb.AllocatedTaskInfo, err error) {
	loaded := tr.loadedTasks.Add(-1)

	tr.backlogAgeLock.Lock()
	tr.backlogAge.record(task.Data.CreateTime, -1)
	tr.backlogAgeLock.Unlock()

	// use == so we just signal once when we cross this threshold
	if int(loaded) == tr.backlogMgr.config.GetTasksBatchSize()/reloadFraction {
		tr.Signal()
	}
	tr.backlogMgr.completeTask(task, err)
}

func (tr *taskReader) getTasksPump(ctx context.Context) error {
	ctx = tr.backlogMgr.contextInfoProvider(ctx)

	if err := tr.backlogMgr.WaitUntilInitialized(ctx); err != nil {
		return err
	}

	updateAckTicker := time.NewTicker(tr.backlogMgr.config.UpdateAckInterval())

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
			if int(tr.loadedTasks.Load()) > tr.backlogMgr.config.GetTasksBatchSize()/reloadFraction {
				// Too many loaded already, ignore this signal. We'll get another signal when
				// loadedTasks drops low enough.
				continue Loop
			}

			batch, err := tr.getTaskBatch(ctx)
			tr.backlogMgr.signalIfFatal(err)
			if err != nil {
				// TODO: Should we ever stop retrying on db errors?
				if common.IsResourceExhausted(err) {
					tr.backoffSignal(taskReaderThrottleRetryDelay)
				} else {
					tr.backoffSignal(tr.retrier.NextBackOff(err))
				}
				continue Loop
			}
			tr.retrier.Reset()

			if len(batch.tasks) == 0 {
				tr.backlogMgr.taskAckManager.setReadLevelAfterGap(batch.readLevel)
				if !batch.isReadBatchDone {
					tr.Signal()
				}
				continue Loop
			}

			tr.addTasksToMatcher(ctx, batch.tasks)
			// There may be more tasks.
			tr.Signal()

		case <-updateAckTicker.C:
			err := tr.persistAckBacklogCountLevel(ctx)
			isConditionFailed := tr.backlogMgr.signalIfFatal(err)
			if err != nil && !isConditionFailed {
				tr.logger().Error("Persistent store operation failure",
					tag.StoreOperationUpdateTaskQueue,
					tag.Error(err))
				// keep going as saving ack is not critical
			}
			// TODO(pri): don't do this, or else prove that it's needed
			tr.Signal() // periodically signal pump to check persistence for tasks
		}
	}
}

func (tr *taskReader) getTaskBatchWithRange(
	ctx context.Context,
	readLevel int64,
	maxReadLevel int64,
) ([]*persistencespb.AllocatedTaskInfo, error) {
	response, err := tr.backlogMgr.db.GetTasks(ctx, readLevel+1, maxReadLevel+1, tr.backlogMgr.config.GetTasksBatchSize())
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
	readLevel := tr.backlogMgr.taskAckManager.getReadLevel()
	maxReadLevel := tr.backlogMgr.db.GetMaxReadLevel()

	// counter i is used to break and let caller check whether taskqueue is still alive and needs to resume read.
	for i := 0; i < 10 && readLevel < maxReadLevel; i++ {
		upper := readLevel + tr.backlogMgr.config.RangeSize
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

func (tr *taskReader) addTasksToMatcher(
	ctx context.Context,
	tasks []*persistencespb.AllocatedTaskInfo,
) error {
	for _, t := range tasks {
		if IsTaskExpired(t) {
			metrics.ExpiredTasksPerTaskQueueCounter.With(tr.taggedMetricsHandler()).Record(1)
			// Also increment readLevel for expired tasks otherwise it could result in
			// looping over the same tasks if all tasks read in the batch are expired
			tr.backlogMgr.taskAckManager.setReadLevel(t.GetTaskId())
			continue
		}

		tr.backlogMgr.taskAckManager.addTask(t.GetTaskId())
		tr.loadedTasks.Add(1)
		tr.backlogAgeLock.Lock()
		tr.backlogAge.record(t.Data.CreateTime, 1)
		tr.backlogAgeLock.Unlock()
		task := newInternalTaskFromBacklog(t, tr.completeTask)

		// After we get to this point, we must eventually call task.finish or
		// task.finishForwarded, which will call tr.completeTask.

		err := tr.backlogMgr.addSpooledTask(ctx, task)

		if err != nil {
			if drop, retry := tr.addErrorBehavior(err); drop {
				task.finish(nil, false)
			} else if retry {
				// This should only be due to persistence problems. Retry in a new goroutine
				// to not block other tasks, up to some concurrency limit.
				if tr.addRetries.Acquire(ctx, 1) != nil {
					return nil
				}
				go tr.retryAddAfterError(ctx, task)
			}
		}
	}
	return nil
}

func (tr *taskReader) addErrorBehavior(err error) (drop, retry bool) {
	// addSpooledTask can only fail due to:
	// - the task queue is closed (errTaskQueueClosed or context.Canceled)
	// - ValidateDeployment failed (InvalidArgument)
	// - versioning wants to get a versioned queue and it can't be initialized
	// - versioning wants to re-spool the task on a different queue and that failed
	// - versioning says StickyWorkerUnavailable
	if errors.Is(err, errTaskQueueClosed) || common.IsContextCanceledErr(err) {
		return false, false
	}
	var stickyUnavailable *serviceerrors.StickyWorkerUnavailable
	if errors.As(err, &stickyUnavailable) {
		return true, false // drop the task
	}
	var invalid *serviceerror.InvalidArgument
	var internal *serviceerror.Internal
	if errors.As(err, &invalid) || errors.As(err, &internal) {
		tr.throttledLogger().Error("nonretryable error processing spooled task", tag.Error(err))
		return true, false // drop the task
	}
	// For any other error (this should be very rare), we can retry.
	tr.throttledLogger().Error("retryable error processing spooled task", tag.Error(err))
	return false, true
}

func (tr *taskReader) retryAddAfterError(ctx context.Context, task *internalTask) {
	defer tr.addRetries.Release(1)
	metrics.BufferThrottlePerTaskQueueCounter.With(tr.taggedMetricsHandler()).Record(1)

	// initial sleep since we just tried once
	util.InterruptibleSleep(ctx, time.Second)

	backoff.ThrottleRetryContext(
		ctx,
		func(ctx context.Context) error {
			if IsTaskExpired(task.event.AllocatedTaskInfo) {
				task.finish(nil, false)
				return nil
			}
			err := tr.backlogMgr.addSpooledTask(ctx, task)
			if drop, retry := tr.addErrorBehavior(err); drop {
				task.finish(nil, false)
			} else if retry {
				metrics.BufferThrottlePerTaskQueueCounter.With(tr.taggedMetricsHandler()).Record(1)
				return err
			}
			return nil
		},
		addErrorRetryPolicy,
		nil,
	)
}

func (tr *taskReader) persistAckBacklogCountLevel(ctx context.Context) error {
	ackLevel := tr.backlogMgr.taskAckManager.getAckLevel()
	return tr.backlogMgr.db.UpdateState(ctx, ackLevel)
}

func (tr *taskReader) logger() log.Logger {
	return tr.backlogMgr.logger
}

func (tr *taskReader) throttledLogger() log.ThrottledLogger {
	return tr.backlogMgr.throttledLogger
}

func (tr *taskReader) taggedMetricsHandler() metrics.Handler {
	return tr.backlogMgr.metricsHandler
}

func (tr *taskReader) backoffSignal(duration time.Duration) {
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

func (tr *taskReader) getLoadedTasks() int64 {
	return tr.loadedTasks.Load()
}
