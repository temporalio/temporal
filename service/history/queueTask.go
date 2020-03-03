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

package history

import (
	"sync"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/task"
)

type (
	queueTaskBase struct {
		sync.Mutex
		queueTaskInfo

		shardID       int
		state         task.State
		priority      int
		attempt       int
		timeSource    clock.TimeSource
		submitTime    time.Time
		logger        log.Logger
		scope         metrics.Scope
		taskExecutor  queueTaskExecutor
		maxRetryCount dynamicconfig.IntPropertyFn

		// TODO: following two fields should be removed after new task lifecycle is implemented
		taskFilter        taskFilter
		shouldProcessTask bool
	}

	// TODO: we don't need the following two implementations after rewriting queueAckMgr.
	// (timer)queueAckMgr should store queueTask object instead of just the key. Then by
	// State() on the queueTask, it can know if the task has been acked or not.
	timerQueueTask struct {
		*queueTaskBase

		ackMgr timerQueueAckMgr
	}

	transferQueueTask struct {
		*queueTaskBase

		ackMgr queueAckMgr
	}
)

func newTimerQueueTask(
	shardID int,
	taskInfo queueTaskInfo,
	scope metrics.Scope,
	logger log.Logger,
	taskFilter taskFilter,
	taskExecutor queueTaskExecutor,
	timeSource clock.TimeSource,
	maxRetryCount dynamicconfig.IntPropertyFn,
	ackMgr timerQueueAckMgr,
) queueTask {
	return &timerQueueTask{
		queueTaskBase: newQueueTaskBase(
			shardID,
			taskInfo,
			scope,
			logger,
			taskFilter,
			taskExecutor,
			timeSource,
			maxRetryCount,
		),
		ackMgr: ackMgr,
	}
}

func newTransferQueueTask(
	shardID int,
	taskInfo queueTaskInfo,
	scope metrics.Scope,
	logger log.Logger,
	taskFilter taskFilter,
	taskExecutor queueTaskExecutor,
	timeSource clock.TimeSource,
	maxRetryCount dynamicconfig.IntPropertyFn,
	ackMgr queueAckMgr,
) queueTask {
	return &transferQueueTask{
		queueTaskBase: newQueueTaskBase(
			shardID,
			taskInfo,
			scope,
			logger,
			taskFilter,
			taskExecutor,
			timeSource,
			maxRetryCount,
		),
		ackMgr: ackMgr,
	}
}

func newQueueTaskBase(
	shardID int,
	queueTaskInfo queueTaskInfo,
	scope metrics.Scope,
	logger log.Logger,
	taskFilter taskFilter,
	taskExecutor queueTaskExecutor,
	timeSource clock.TimeSource,
	maxRetryCount dynamicconfig.IntPropertyFn,
) *queueTaskBase {
	return &queueTaskBase{
		queueTaskInfo: queueTaskInfo,
		shardID:       shardID,
		state:         task.TaskStatePending,
		scope:         scope,
		logger:        logger,
		attempt:       0,
		submitTime:    timeSource.Now(),
		timeSource:    timeSource,
		maxRetryCount: maxRetryCount,
		taskFilter:    taskFilter,
		taskExecutor:  taskExecutor,
	}
}

func (t *timerQueueTask) Ack() {
	t.queueTaskBase.Ack()

	timerTask, ok := t.queueTaskInfo.(*persistence.TimerTaskInfo)
	if !ok {
		return
	}
	t.ackMgr.completeTimerTask(timerTask)
}

func (t *timerQueueTask) GetQueueType() queueType {
	return timerQueueType
}

func (t *transferQueueTask) Ack() {
	t.queueTaskBase.Ack()

	t.ackMgr.completeQueueTask(t.GetTaskID())
}

func (t *transferQueueTask) GetQueueType() queueType {
	return transferQueueType
}

func (t *queueTaskBase) Execute() error {
	// TODO: after mergering active and standby queue,
	// the task should be smart enough to tell if it should be
	// processed as active or standby and use the corresponding
	// task executor.
	var err error
	t.shouldProcessTask, err = t.taskFilter(t.queueTaskInfo)
	if err != nil {
		time.Sleep(loadDomainEntryForTimerTaskRetryDelay)
		return err
	}

	executionStartTime := t.timeSource.Now()

	defer func() {
		if t.shouldProcessTask {
			t.scope.IncCounter(metrics.TaskRequests)
			t.scope.RecordTimer(metrics.TaskProcessingLatency, time.Since(executionStartTime))
		}
	}()

	return t.taskExecutor.execute(t.queueTaskInfo, t.shouldProcessTask)
}

func (t *queueTaskBase) HandleErr(
	err error,
) (retErr error) {
	defer func() {
		if retErr != nil {
			t.attempt++
			if t.attempt > t.maxRetryCount() {
				t.logger.Error("Critical error processing task, retrying.",
					tag.Error(err), tag.OperationCritical, tag.TaskType(t.GetTaskType()))
			}
		}
	}()

	if err == nil {
		return nil
	}

	if _, ok := err.(*workflow.EntityNotExistsError); ok {
		return nil
	}

	// this is a transient error
	if err == ErrTaskRetry {
		t.scope.IncCounter(metrics.TaskStandbyRetryCounter)
		return err
	}

	if err == ErrTaskDiscarded {
		t.scope.IncCounter(metrics.TaskDiscarded)
		err = nil
	}

	// this is a transient error
	// TODO remove this error check special case
	//  since the new task life cycle will not give up until task processed / verified
	if _, ok := err.(*workflow.DomainNotActiveError); ok {
		if t.timeSource.Now().Sub(t.submitTime) > 2*cache.DomainCacheRefreshInterval {
			t.scope.IncCounter(metrics.TaskNotActiveCounter)
			return nil
		}

		return err
	}

	t.scope.IncCounter(metrics.TaskFailures)

	if _, ok := err.(*persistence.CurrentWorkflowConditionFailedError); ok {
		t.logger.Error("More than 2 workflow are running.", tag.Error(err), tag.LifeCycleProcessingFailed)
		return nil
	}

	t.logger.Error("Fail to process task", tag.Error(err), tag.LifeCycleProcessingFailed)
	return err
}

func (t *queueTaskBase) RetryErr(
	err error,
) bool {
	return true
}

func (t *queueTaskBase) Ack() {
	t.Lock()
	defer t.Unlock()

	t.state = task.TaskStateAcked
	if t.shouldProcessTask {
		t.scope.RecordTimer(metrics.TaskAttemptTimer, time.Duration(t.attempt))
		t.scope.RecordTimer(metrics.TaskLatency, time.Since(t.submitTime))
		t.scope.RecordTimer(metrics.TaskQueueLatency, time.Since(t.GetVisibilityTimestamp()))
	}
}

func (t *queueTaskBase) Nack() {
	t.Lock()
	defer t.Unlock()

	t.state = task.TaskStateNacked

	// TODO: Nack should also notify the queue processor to redispatch the task
	// implement this after redispatch functionality is added to the processor
}

func (t *queueTaskBase) State() task.State {
	t.Lock()
	defer t.Unlock()

	return t.state
}

func (t *queueTaskBase) Priority() int {
	t.Lock()
	defer t.Unlock()

	return t.priority
}

func (t *queueTaskBase) SetPriority(
	priority int,
) {
	t.Lock()
	defer t.Unlock()

	t.priority = priority
}

func (t *queueTaskBase) GetShardID() int {
	return t.shardID
}
