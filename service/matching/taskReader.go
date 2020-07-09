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
	"runtime"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/worker/scanner/taskqueue"
)

type (
	taskReader struct {
		taskBuffer chan *persistenceblobs.AllocatedTaskInfo // tasks loaded from persistence
		notifyC    chan struct{}                            // Used as signal to notify pump of new tasks
		tlMgr      *taskQueueManagerImpl
		// The cancel objects are to cancel the ratelimiter Wait in dispatchBufferedTasks. The ideal
		// approach is to use request-scoped contexts and use a unique one for each call to Wait. However
		// in order to cancel it on shutdown, we need a new goroutine for each call that would wait on
		// the shutdown channel. To optimize on efficiency, we instead create one and tag it on the struct
		// so the cancel can be called directly on shutdown.
		cancelCtx  context.Context
		cancelFunc context.CancelFunc
		// separate shutdownC needed for dispatchTasks go routine to allow
		// getTasksPump to be stopped without stopping dispatchTasks in unit tests
		dispatcherShutdownC chan struct{}
	}
)

func newTaskReader(tlMgr *taskQueueManagerImpl) *taskReader {
	ctx, cancel := context.WithCancel(context.Background())
	return &taskReader{
		tlMgr:               tlMgr,
		cancelCtx:           ctx,
		cancelFunc:          cancel,
		notifyC:             make(chan struct{}, 1),
		dispatcherShutdownC: make(chan struct{}),
		// we always dequeue the head of the buffer and try to dispatch it to a poller
		// so allocate one less than desired target buffer size
		taskBuffer: make(chan *persistenceblobs.AllocatedTaskInfo, tlMgr.config.GetTasksBatchSize()-1),
	}
}

func (tr *taskReader) Start() {
	tr.Signal()
	go tr.dispatchBufferedTasks()
	go tr.getTasksPump()
}

func (tr *taskReader) Stop() {
	tr.cancelFunc()
	close(tr.dispatcherShutdownC)
}

func (tr *taskReader) Signal() {
	var event struct{}
	select {
	case tr.notifyC <- event:
	default: // channel already has an event, don't block
	}
}

func (tr *taskReader) dispatchBufferedTasks() {
dispatchLoop:
	for {
		select {
		case taskInfo, ok := <-tr.taskBuffer:
			if !ok { // Task queue getTasks pump is shutdown
				break dispatchLoop
			}
			task := newInternalTask(taskInfo, tr.tlMgr.completeTask, enumsspb.TASK_SOURCE_DB_BACKLOG, "", false)
			for {
				err := tr.tlMgr.DispatchTask(tr.cancelCtx, task)
				if err == nil {
					break
				}
				if err == context.Canceled {
					tr.tlMgr.logger.Info("Taskqueue manager context is cancelled, shutting down")
					break dispatchLoop
				}
				// this should never happen unless there is a bug - don't drop the task
				tr.scope().IncCounter(metrics.BufferThrottlePerTaskQueueCounter)
				tr.logger().Error("taskReader: unexpected error dispatching task", tag.Error(err))
				runtime.Gosched()
			}
		case <-tr.dispatcherShutdownC:
			break dispatchLoop
		}
	}
}

func (tr *taskReader) getTasksPump() {
	tr.tlMgr.startWG.Wait()
	defer close(tr.taskBuffer)

	updateAckTimer := time.NewTimer(tr.tlMgr.config.UpdateAckInterval())
	checkIdleTaskQueueTimer := time.NewTimer(tr.tlMgr.config.IdleTaskqueueCheckInterval())
	lastTimeWriteTask := time.Time{}
getTasksPumpLoop:
	for {
		select {
		case <-tr.tlMgr.shutdownCh:
			break getTasksPumpLoop
		case <-tr.notifyC:
			{
				lastTimeWriteTask = time.Now()

				tasks, readLevel, isReadBatchDone, err := tr.getTaskBatch()
				if err != nil {
					tr.Signal() // re-enqueue the event
					// TODO: Should we ever stop retrying on db errors?
					continue getTasksPumpLoop
				}

				if len(tasks) == 0 {
					tr.tlMgr.taskAckManager.setReadLevel(readLevel)
					if !isReadBatchDone {
						tr.Signal()
					}
					continue getTasksPumpLoop
				}

				if !tr.addTasksToBuffer(tasks, lastTimeWriteTask, checkIdleTaskQueueTimer) {
					break getTasksPumpLoop
				}
				// There maybe more tasks. We yield now, but signal pump to check again later.
				tr.Signal()
			}
		case <-updateAckTimer.C:
			{
				err := tr.persistAckLevel()
				if err != nil {
					if _, ok := err.(*persistence.ConditionFailedError); ok {
						// This indicates the task queue may have moved to another host.
						tr.tlMgr.Stop()
					} else {
						tr.logger().Error("Persistent store operation failure",
							tag.StoreOperationUpdateTaskQueue,
							tag.Error(err))
					}
					// keep going as saving ack is not critical
				}
				tr.Signal() // periodically signal pump to check persistence for tasks
				updateAckTimer = time.NewTimer(tr.tlMgr.config.UpdateAckInterval())
			}
		case <-checkIdleTaskQueueTimer.C:
			{
				if tr.isIdle(lastTimeWriteTask) {
					tr.handleIdleTimeout()
					break getTasksPumpLoop
				}
				checkIdleTaskQueueTimer = time.NewTimer(tr.tlMgr.config.IdleTaskqueueCheckInterval())
			}
		}
	}

	updateAckTimer.Stop()
	checkIdleTaskQueueTimer.Stop()
}

func (tr *taskReader) getTaskBatchWithRange(readLevel int64, maxReadLevel int64) ([]*persistenceblobs.AllocatedTaskInfo, error) {
	response, err := tr.tlMgr.executeWithRetry(func() (interface{}, error) {
		return tr.tlMgr.db.GetTasks(readLevel, maxReadLevel, tr.tlMgr.config.GetTasksBatchSize())
	})
	if err != nil {
		return nil, err
	}
	return response.(*persistence.GetTasksResponse).Tasks, err
}

// Returns a batch of tasks from persistence starting form current read level.
// Also return a number that can be used to update readLevel
// Also return a bool to indicate whether read is finished
func (tr *taskReader) getTaskBatch() ([]*persistenceblobs.AllocatedTaskInfo, int64, bool, error) {
	var tasks []*persistenceblobs.AllocatedTaskInfo
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

func (tr *taskReader) isIdle(lastWriteTime time.Time) bool {
	return !tr.isTaskAddedRecently(lastWriteTime) && len(tr.tlMgr.GetAllPollerInfo()) == 0
}

func (tr *taskReader) handleIdleTimeout() {
	_ = tr.persistAckLevel()
	tr.tlMgr.taskGC.RunNow(tr.tlMgr.taskAckManager.getAckLevel())
	tr.tlMgr.Stop()
}

func (tr *taskReader) addTasksToBuffer(tasks []*persistenceblobs.AllocatedTaskInfo, lastWriteTime time.Time, idleTimer *time.Timer) bool {
	for _, t := range tasks {
		if taskqueue.IsTaskExpired(t) {
			tr.scope().IncCounter(metrics.ExpiredTasksPerTaskQueueCounter)
			// Also increment readLevel for expired tasks otherwise it could result in
			// looping over the same tasks if all tasks read in the batch are expired
			tr.tlMgr.taskAckManager.setReadLevel(t.GetTaskId())
			continue
		}
		if !tr.addSingleTaskToBuffer(t, lastWriteTime, idleTimer) {
			return false // we are shutting down the task queue
		}
	}
	return true
}

func (tr *taskReader) addSingleTaskToBuffer(
	task *persistenceblobs.AllocatedTaskInfo, lastWriteTime time.Time, idleTimer *time.Timer) bool {
	tr.tlMgr.taskAckManager.addTask(task.GetTaskId())
	for {
		select {
		case tr.taskBuffer <- task:
			return true
		case <-idleTimer.C:
			if tr.isIdle(lastWriteTime) {
				tr.handleIdleTimeout()
				return false
			}
		case <-tr.tlMgr.shutdownCh:
			return false
		}
	}
}

func (tr *taskReader) persistAckLevel() error {
	return tr.tlMgr.db.UpdateState(tr.tlMgr.taskAckManager.getAckLevel())
}

func (tr *taskReader) isTaskAddedRecently(lastAddTime time.Time) bool {
	return time.Now().Sub(lastAddTime) <= tr.tlMgr.config.MaxTaskqueueIdleTime()
}

func (tr *taskReader) logger() log.Logger {
	return tr.tlMgr.logger
}

func (tr *taskReader) scope() metrics.Scope {
	return tr.tlMgr.metricScope()
}
