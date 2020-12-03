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

package history

import (
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/common/task"
	"go.temporal.io/server/service/history/shard"
)

type (
	queueTaskBase struct {
		sync.Mutex
		queueTaskInfo

		shard         shard.Context
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

		ackMgr          timerQueueAckMgr
		redispatchQueue collection.Queue
	}

	transferQueueTask struct {
		*queueTaskBase

		ackMgr          queueAckMgr
		redispatchQueue collection.Queue
	}

	visibilityQueueTask struct {
		*queueTaskBase

		ackMgr          queueAckMgr
		redispatchQueue collection.Queue
	}
)

func newTimerQueueTask(
	shard shard.Context,
	taskInfo queueTaskInfo,
	scope metrics.Scope,
	logger log.Logger,
	taskFilter taskFilter,
	taskExecutor queueTaskExecutor,
	redispatchQueue collection.Queue,
	timeSource clock.TimeSource,
	maxRetryCount dynamicconfig.IntPropertyFn,
	ackMgr timerQueueAckMgr,
) queueTask {
	return &timerQueueTask{
		queueTaskBase: newQueueTaskBase(
			shard,
			taskInfo,
			scope,
			logger,
			taskFilter,
			taskExecutor,
			timeSource,
			maxRetryCount,
		),
		ackMgr:          ackMgr,
		redispatchQueue: redispatchQueue,
	}
}

func newTransferQueueTask(
	shard shard.Context,
	taskInfo queueTaskInfo,
	scope metrics.Scope,
	logger log.Logger,
	taskFilter taskFilter,
	taskExecutor queueTaskExecutor,
	redispatchQueue collection.Queue,
	timeSource clock.TimeSource,
	maxRetryCount dynamicconfig.IntPropertyFn,
	ackMgr queueAckMgr,
) queueTask {
	return &transferQueueTask{
		queueTaskBase: newQueueTaskBase(
			shard,
			taskInfo,
			scope,
			logger,
			taskFilter,
			taskExecutor,
			timeSource,
			maxRetryCount,
		),
		ackMgr:          ackMgr,
		redispatchQueue: redispatchQueue,
	}
}

func newVisibilityQueueTask(
	shard shard.Context,
	taskInfo queueTaskInfo,
	scope metrics.Scope,
	logger log.Logger,
	taskFilter taskFilter,
	taskExecutor queueTaskExecutor,
	redispatchQueue collection.Queue,
	timeSource clock.TimeSource,
	maxRetryCount dynamicconfig.IntPropertyFn,
	ackMgr queueAckMgr,
) queueTask {
	return &visibilityQueueTask{
		queueTaskBase: newQueueTaskBase(
			shard,
			taskInfo,
			scope,
			logger,
			taskFilter,
			taskExecutor,
			timeSource,
			maxRetryCount,
		),
		ackMgr:          ackMgr,
		redispatchQueue: redispatchQueue,
	}
}

func newQueueTaskBase(
	shard shard.Context,
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
		shard:         shard,
		state:         task.TaskStatePending,
		scope:         scope,
		logger:        logger,
		attempt:       1,
		submitTime:    timeSource.Now(),
		timeSource:    timeSource,
		maxRetryCount: maxRetryCount,
		taskFilter:    taskFilter,
		taskExecutor:  taskExecutor,
	}
}

func (t *timerQueueTask) Ack() {
	t.queueTaskBase.Ack()

	timerTask, ok := t.queueTaskInfo.(*persistencespb.TimerTaskInfo)
	if !ok {
		return
	}
	t.ackMgr.completeTimerTask(timerTask)
}

func (t *timerQueueTask) Nack() {
	t.queueTaskBase.Nack()

	// don't move redispatchQueue to queueTaskBase as we need to
	// redispatch timeQueueTask, not queueTaskBase
	t.redispatchQueue.Add(t)
}

func (t *timerQueueTask) GetQueueType() queueType {
	return timerQueueType
}

func (t *transferQueueTask) Ack() {
	t.queueTaskBase.Ack()

	t.ackMgr.completeQueueTask(t.GetTaskId())
}

func (t *transferQueueTask) Nack() {
	t.queueTaskBase.Nack()

	// don't move redispatchQueue to queueTaskBase as we need to
	// redispatch transferQueueTask, not queueTaskBase
	t.redispatchQueue.Add(t)
}

func (t *transferQueueTask) GetQueueType() queueType {
	return transferQueueType
}

func (t *visibilityQueueTask) Ack() {
	t.queueTaskBase.Ack()

	t.ackMgr.completeQueueTask(t.GetTaskId())
}

func (t *visibilityQueueTask) Nack() {
	t.queueTaskBase.Nack()

	// don't move redispatchQueue to queueTaskBase as we need to
	// redispatch visibilityQueueTask, not queueTaskBase
	t.redispatchQueue.Add(t)
}

func (t *visibilityQueueTask) GetQueueType() queueType {
	return visibilityQueueType
}

func (t *queueTaskBase) Execute() error {
	// TODO: after mergering active and standby queue,
	// the task should be smart enough to tell if it should be
	// processed as active or standby and use the corresponding
	// task executor.
	var err error
	t.shouldProcessTask, err = t.taskFilter(t.queueTaskInfo)
	if err != nil {
		time.Sleep(loadNamespaceEntryForTimerTaskRetryDelay)
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

	if _, ok := err.(*serviceerror.NotFound); ok {
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
	if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
		submitTimeDiff := t.timeSource.Now().Sub(t.submitTime)
		if submitTimeDiff >= 2*cache.NamespaceCacheRefreshInterval {
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
	if common.IsContextDeadlineExceededErr(err) {
		return false
	}
	return true
}

func (t *queueTaskBase) Ack() {
	t.Lock()
	defer t.Unlock()

	t.state = task.TaskStateAcked
	if t.shouldProcessTask {
		t.scope.RecordTimer(metrics.TaskAttemptTimer, time.Duration(t.attempt))
		t.scope.RecordTimer(metrics.TaskLatency, time.Since(t.submitTime))
		t.scope.RecordTimer(metrics.TaskQueueLatency, time.Since(timestamp.TimeValue(t.GetVisibilityTime())))
	}
}

func (t *queueTaskBase) Nack() {
	t.Lock()
	defer t.Unlock()

	t.state = task.TaskStateNacked
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

func (t *queueTaskBase) GetShard() shard.Context {
	return t.shard
}
