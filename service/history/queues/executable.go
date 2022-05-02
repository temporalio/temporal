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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination executable_mock.go

package queues

import (
	"context"
	"sync"
	time "time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	backoff "go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/tasks"
)

type (
	Executable interface {
		ctasks.PriorityTask
		tasks.Task

		Attempt() int
		Logger() log.Logger
		GetTask() tasks.Task

		QueueType() QueueType
	}

	Executor interface {
		Execute(context.Context, Executable) error
	}

	// TaskFilter determines if the given task should be executed
	// TODO: remove after merging active/standby queue processor
	// task should always be executed as active or verified as standby
	TaskFilter func(task tasks.Task) bool
)

var (
	// schedulerRetryPolicy is the retry policy for retrying the executable
	// in one submission to scheduler, the goroutine for processing this executable
	// is held during the retry
	schedulerRetryPolicy = common.CreateTaskProcessingRetryPolicy()
	// reschedulePolicy is the policy for determine reschedule backoff duration
	// across multiple submissions to scheduler
	reschedulePolicy = common.CreateTaskReschedulePolicy()
)

const (
	// resubmitMaxAttempts is the max number of attempts we may skip rescheduler when a task is Nacked.
	// check the comment in shouldResubmitOnNack() for more details
	resubmitMaxAttempts = 10
)

type (
	executableImpl struct {
		tasks.Task

		sync.Mutex
		state    ctasks.State
		priority int
		attempt  int

		executor    Executor
		scheduler   Scheduler
		rescheduler Rescheduler
		timeSource  clock.TimeSource

		loadTime                      time.Time
		userLatency                   time.Duration
		logger                        log.Logger
		scope                         metrics.Scope
		criticalRetryAttempt          dynamicconfig.IntPropertyFn
		namespaceCacheRefreshInterval dynamicconfig.DurationPropertyFn
		queueType                     QueueType
		filter                        TaskFilter
		shouldProcess                 bool
	}
)

func NewExecutable(
	task tasks.Task,
	filter TaskFilter,
	executor Executor,
	scheduler Scheduler,
	rescheduler Rescheduler,
	timeSource clock.TimeSource,
	logger log.Logger,
	scope metrics.Scope,
	criticalRetryAttempt dynamicconfig.IntPropertyFn,
	queueType QueueType,
	namespaceCacheRefreshInterval dynamicconfig.DurationPropertyFn,
) Executable {
	return &executableImpl{
		Task:        task,
		state:       ctasks.TaskStatePending,
		attempt:     1,
		executor:    executor,
		scheduler:   scheduler,
		rescheduler: rescheduler,
		timeSource:  timeSource,
		loadTime:    timeSource.Now(),
		logger: log.NewLazyLogger(
			logger,
			func() []tag.Tag {
				return tasks.Tags(task)
			},
		),
		scope:                         scope,
		queueType:                     queueType,
		criticalRetryAttempt:          criticalRetryAttempt,
		filter:                        filter,
		namespaceCacheRefreshInterval: namespaceCacheRefreshInterval,
	}
}

func (e *executableImpl) Execute() error {
	// this filter should also contain the logic for overriding
	// results from task allocator (force executing some standby task types)
	e.shouldProcess = e.filter(e.Task)
	if !e.shouldProcess {
		return nil
	}

	ctx := metrics.AddMetricsContext(context.Background())
	startTime := e.timeSource.Now()
	err := e.executor.Execute(ctx, e)
	var userLatency time.Duration
	if duration, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency); ok {
		userLatency = time.Duration(duration)
	}
	e.userLatency += userLatency

	e.scope.IncCounter(metrics.TaskRequests)
	e.scope.RecordTimer(metrics.TaskProcessingLatency, time.Since(startTime))
	e.scope.RecordTimer(metrics.TaskNoUserProcessingLatency, time.Since(startTime)-userLatency)
	return err
}

func (e *executableImpl) HandleErr(err error) (retErr error) {
	defer func() {
		if retErr != nil {
			e.Lock()
			defer e.Unlock()

			e.attempt++
			if e.attempt > e.criticalRetryAttempt() {
				e.scope.RecordDistribution(metrics.TaskAttemptTimer, e.attempt)
				e.logger.Error("Critical error processing task, retrying.", tag.Error(err), tag.OperationCritical)
			}
		}
	}()

	if err == nil {
		return nil
	}

	if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
		return nil
	}

	// This means that namespace is deleted, and it is safe to drop the task (=ignore the error).
	if _, isNotFound := err.(*serviceerror.NamespaceNotFound); isNotFound {
		return nil
	}

	if err == consts.ErrTaskRetry {
		e.scope.IncCounter(metrics.TaskStandbyRetryCounter)
		return err
	}

	if err == consts.ErrWorkflowBusy {
		e.scope.IncCounter(metrics.TaskWorkflowBusyCounter)
		return err
	}

	if err == consts.ErrTaskDiscarded {
		e.scope.IncCounter(metrics.TaskDiscarded)
		return nil
	}

	// this is a transient error
	// TODO remove this error check special case
	//  since the new task life cycle will not give up until task processed / verified
	if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
		if e.timeSource.Now().Sub(e.loadTime) > 2*e.namespaceCacheRefreshInterval() {
			e.scope.IncCounter(metrics.TaskNotActiveCounter)
			return nil
		}

		return err
	}

	e.scope.IncCounter(metrics.TaskFailures)

	e.logger.Error("Fail to process task", tag.Error(err), tag.LifeCycleProcessingFailed)
	return err
}

func (e *executableImpl) IsRetryableError(err error) bool {
	// this determines if the executable should be retried within one submission to scheduler

	// don't retry immediately for resource exhausted which may incur more load
	// context deadline exceed may also suggested downstream is overloaded, so don't retry immediately
	if common.IsResourceExhausted(err) || common.IsContextDeadlineExceededErr(err) {
		return false
	}

	// ErrTaskRetry means mutable state is not ready for standby task processing
	// there's no point for retrying the task immediately which will hold the worker corouinte
	// TODO: change ErrTaskRetry to a better name
	return err != consts.ErrTaskRetry && err != consts.ErrWorkflowBusy
}

func (e *executableImpl) RetryPolicy() backoff.RetryPolicy {
	// this is the retry policy for one submission
	// not for calculating the backoff after the task is nacked
	return schedulerRetryPolicy
}

func (e *executableImpl) Ack() {
	e.Lock()
	defer e.Unlock()

	e.state = ctasks.TaskStateAcked

	if e.shouldProcess {
		e.scope.RecordDistribution(metrics.TaskAttemptTimer, e.attempt)
		e.scope.RecordTimer(metrics.TaskLatency, time.Since(e.loadTime))
		e.scope.RecordTimer(metrics.TaskQueueLatency, time.Since(e.GetVisibilityTime()))
		e.scope.RecordTimer(metrics.TaskUserLatency, e.userLatency)
		e.scope.RecordTimer(metrics.TaskNoUserLatency, time.Since(e.loadTime)-e.userLatency)
		e.scope.RecordTimer(metrics.TaskNoUserQueueLatency, time.Since(e.GetVisibilityTime())-e.userLatency)
	}
}

func (e *executableImpl) Nack(err error) {
	submitted := false
	attempt := e.Attempt()
	if e.shouldResubmitOnNack(attempt, err) {
		// we do not need to know if there any error during submission
		// as long as it's not submitted, the execuable should be add
		// to the rescheduler
		submitted, _ = e.scheduler.TrySubmit(e)
	}

	if !submitted {
		e.rescheduler.Add(e, e.rescheduleBackoff(attempt))
	}
}

func (e *executableImpl) Reschedule() {
	e.rescheduler.Add(e, e.rescheduleBackoff(e.Attempt()))
}

func (e *executableImpl) State() ctasks.State {
	e.Lock()
	defer e.Unlock()

	return e.state
}

func (e *executableImpl) GetPriority() int {
	e.Lock()
	defer e.Unlock()

	return e.priority
}

func (e *executableImpl) SetPriority(priority int) {
	e.Lock()
	defer e.Unlock()

	e.priority = priority
}

func (e *executableImpl) Attempt() int {
	e.Lock()
	defer e.Unlock()

	return e.attempt
}

func (e *executableImpl) Logger() log.Logger {
	return e.logger
}

func (e *executableImpl) GetTask() tasks.Task {
	return e.Task
}

func (e *executableImpl) QueueType() QueueType {
	return e.queueType
}

func (e *executableImpl) shouldResubmitOnNack(attempt int, err error) bool {
	// this is an optimization for skipping rescheduler and retry the task sooner
	// this can be useful for errors like unable to get workflow lock, which doesn't
	// have to backoff for a long time and wait for the periodic rescheduling.
	return (err == consts.ErrWorkflowBusy || e.IsRetryableError(err)) && e.Attempt() < resubmitMaxAttempts
}

func (e *executableImpl) rescheduleBackoff(attempt int) time.Duration {
	// elapsedTime (the first parameter) is not relevant here since reschedule policy
	// has no expiration interval.
	return reschedulePolicy.ComputeNextDelay(0, attempt)
}
