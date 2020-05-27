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

package task

import (
	"errors"
	"sync"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	ctask "github.com/uber/cadence/common/task"
	"github.com/uber/cadence/service/history/shard"
)

const (
	loadDomainEntryForTaskRetryDelay = 100 * time.Millisecond
)

var (
	// ErrTaskDiscarded is the error indicating that the timer / transfer task is pending for too long and discarded.
	ErrTaskDiscarded = errors.New("passive task pending for too long")
	// ErrTaskRetry is the error indicating that the timer / transfer task should be retried.
	ErrTaskRetry = errors.New("passive task should retry due to condition in mutable state is not met")
)

type (
	// TimerQueueAckMgr is the interface for acking timer task
	TimerQueueAckMgr interface {
		CompleteTimerTask(timerTask *persistence.TimerTaskInfo)
	}

	// QueueAckMgr is the interface for acking transfer task
	QueueAckMgr interface {
		CompleteQueueTask(taskID int64)
	}

	taskBase struct {
		sync.Mutex
		Info

		shard         shard.Context
		state         ctask.State
		priority      int
		attempt       int
		timeSource    clock.TimeSource
		submitTime    time.Time
		logger        log.Logger
		scope         metrics.Scope
		taskExecutor  Executor
		maxRetryCount dynamicconfig.IntPropertyFn

		// TODO: following three fields should be removed after new task lifecycle is implemented
		taskFilter        Filter
		queueType         QueueType
		shouldProcessTask bool
	}

	// TODO: we don't need the following two implementations after rewriting QueueAckMgr.
	// (timer)QueueAckMgr should store queueTask object instead of just the key. Then by
	// State() on the queueTask, it can know if the task has been acked or not.
	timerTask struct {
		*taskBase

		ackMgr          TimerQueueAckMgr
		redispatchQueue collection.Queue
	}

	transferTask struct {
		*taskBase

		ackMgr          QueueAckMgr
		redispatchQueue collection.Queue
	}
)

// NewTimerTask creates a new timer task
func NewTimerTask(
	shard shard.Context,
	taskInfo Info,
	queueType QueueType,
	scope metrics.Scope,
	logger log.Logger,
	taskFilter Filter,
	taskExecutor Executor,
	redispatchQueue collection.Queue,
	timeSource clock.TimeSource,
	maxRetryCount dynamicconfig.IntPropertyFn,
	ackMgr TimerQueueAckMgr,
) Task {
	return &timerTask{
		taskBase: newQueueTaskBase(
			shard,
			taskInfo,
			queueType,
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

// NewTransferTask creates a new transfer task
func NewTransferTask(
	shard shard.Context,
	taskInfo Info,
	queueType QueueType,
	scope metrics.Scope,
	logger log.Logger,
	taskFilter Filter,
	taskExecutor Executor,
	redispatchQueue collection.Queue,
	timeSource clock.TimeSource,
	maxRetryCount dynamicconfig.IntPropertyFn,
	ackMgr QueueAckMgr,
) Task {
	return &transferTask{
		taskBase: newQueueTaskBase(
			shard,
			taskInfo,
			queueType,
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
	queueTaskInfo Info,
	queueType QueueType,
	scope metrics.Scope,
	logger log.Logger,
	taskFilter Filter,
	taskExecutor Executor,
	timeSource clock.TimeSource,
	maxRetryCount dynamicconfig.IntPropertyFn,
) *taskBase {
	return &taskBase{
		Info:          queueTaskInfo,
		shard:         shard,
		state:         ctask.TaskStatePending,
		queueType:     queueType,
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

func (t *timerTask) Ack() {
	t.taskBase.Ack()

	timerTask, ok := t.Info.(*persistence.TimerTaskInfo)
	if !ok {
		return
	}
	t.ackMgr.CompleteTimerTask(timerTask)
}

func (t *timerTask) Nack() {
	t.taskBase.Nack()

	// don't move redispatchQueue to taskBase as we need to
	// redispatch timeQueueTask, not taskBase
	t.redispatchQueue.Add(t)
}

func (t *transferTask) Ack() {
	t.taskBase.Ack()

	t.ackMgr.CompleteQueueTask(t.GetTaskID())
}

func (t *transferTask) Nack() {
	t.taskBase.Nack()

	// don't move redispatchQueue to taskBase as we need to
	// redispatch transferTask, not taskBase
	t.redispatchQueue.Add(t)
}

func (t *taskBase) Execute() error {
	// TODO: after mergering active and standby queue,
	// the task should be smart enough to tell if it should be
	// processed as active or standby and use the corresponding
	// task executor.
	var err error
	t.shouldProcessTask, err = t.taskFilter(t.Info)
	if err != nil {
		time.Sleep(loadDomainEntryForTaskRetryDelay)
		return err
	}

	executionStartTime := t.timeSource.Now()

	defer func() {
		if t.shouldProcessTask {
			t.scope.IncCounter(metrics.TaskRequests)
			t.scope.RecordTimer(metrics.TaskProcessingLatency, time.Since(executionStartTime))
		}
	}()

	return t.taskExecutor.Execute(t.Info, t.shouldProcessTask)
}

func (t *taskBase) HandleErr(
	err error,
) (retErr error) {
	defer func() {
		if retErr != nil {
			t.Lock()
			defer t.Unlock()

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

func (t *taskBase) RetryErr(
	err error,
) bool {
	return true
}

func (t *taskBase) Ack() {
	t.Lock()
	defer t.Unlock()

	t.state = ctask.TaskStateAcked
	if t.shouldProcessTask {
		t.scope.RecordTimer(metrics.TaskAttemptTimer, time.Duration(t.attempt))
		t.scope.RecordTimer(metrics.TaskLatency, time.Since(t.submitTime))
		t.scope.RecordTimer(metrics.TaskQueueLatency, time.Since(t.GetVisibilityTimestamp()))
	}
}

func (t *taskBase) Nack() {
	t.Lock()
	defer t.Unlock()

	t.state = ctask.TaskStateNacked
}

func (t *taskBase) State() ctask.State {
	t.Lock()
	defer t.Unlock()

	return t.state
}

func (t *taskBase) Priority() int {
	t.Lock()
	defer t.Unlock()

	return t.priority
}

func (t *taskBase) SetPriority(
	priority int,
) {
	t.Lock()
	defer t.Unlock()

	t.priority = priority
}

func (t *taskBase) GetShard() shard.Context {
	return t.shard
}

func (t *taskBase) GetAttempt() int {
	t.Lock()
	defer t.Unlock()

	return t.attempt
}

func (t *taskBase) GetQueueType() QueueType {
	return t.queueType
}
