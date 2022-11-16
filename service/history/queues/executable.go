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
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	Executable interface {
		ctasks.Task
		tasks.Task

		Attempt() int
		GetTask() tasks.Task
		GetPriority() ctasks.Priority
		GetScheduledTime() time.Time
		SetScheduledTime(time.Time)
	}

	Executor interface {
		// TODO: remove isActive return value after deprecating
		// active/standby queue processing logic
		Execute(context.Context, Executable) (tags []metrics.Tag, isActive bool, err error)
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
	reschedulePolicy                           = common.CreateTaskReschedulePolicy()
	taskNotReadyReschedulePolicy               = common.CreateTaskNotReadyReschedulePolicy()
	taskResourceExhuastedReschedulePolicy      = common.CreateTaskResourceExhaustedReschedulePolicy()
	dependencyTaskNotCompletedReschedulePolicy = common.CreateDependencyTaskNotCompletedReschedulePolicy()
)

const (
	// resubmitMaxAttempts is the max number of attempts we may skip rescheduler when a task is Nacked.
	// check the comment in shouldResubmitOnNack() for more details
	// TODO: evaluate the performance when this numbers is greatly reduced to a number like 3.
	// especially, if that will increase the latency for workflow busy case by a lot.
	resubmitMaxAttempts = 10
	// resourceExhaustedResubmitMaxAttempts is the same as resubmitMaxAttempts but only applies to resource
	// exhausted error
	resourceExhaustedResubmitMaxAttempts = 1
)

type (
	executableImpl struct {
		tasks.Task

		sync.Mutex
		state          ctasks.State
		priority       ctasks.Priority // priority for the current attempt
		lowestPriority ctasks.Priority // priority for emitting metrics across multiple attempts
		attempt        int

		executor          Executor
		scheduler         Scheduler
		rescheduler       Rescheduler
		priorityAssigner  PriorityAssigner
		timeSource        clock.TimeSource
		namespaceRegistry namespace.Registry

		readerID                      int32
		loadTime                      time.Time
		scheduledTime                 time.Time
		userLatency                   time.Duration
		lastActiveness                bool
		resourceExhaustedCount        int
		logger                        log.Logger
		metricsHandler                metrics.MetricsHandler
		taggedMetricsHandler          metrics.MetricsHandler
		criticalRetryAttempt          dynamicconfig.IntPropertyFn
		namespaceCacheRefreshInterval dynamicconfig.DurationPropertyFn
		filter                        TaskFilter
		shouldProcess                 bool
	}
)

// TODO: Remove filter, queueType, and namespaceCacheRefreshInterval
// parameters after deprecating old queue processing logic.
// CriticalRetryAttempt probably should also be removed as it's only
// used for emiting logs and metrics when # of attempts is high, and
// doesn't have to be a dynamic config.
func NewExecutable(
	readerID int32,
	task tasks.Task,
	filter TaskFilter,
	executor Executor,
	scheduler Scheduler,
	rescheduler Rescheduler,
	priorityAssigner PriorityAssigner,
	timeSource clock.TimeSource,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
	criticalRetryAttempt dynamicconfig.IntPropertyFn,
	namespaceCacheRefreshInterval dynamicconfig.DurationPropertyFn,
) Executable {
	executable := &executableImpl{
		Task:              task,
		state:             ctasks.TaskStatePending,
		attempt:           1,
		executor:          executor,
		scheduler:         scheduler,
		rescheduler:       rescheduler,
		priorityAssigner:  priorityAssigner,
		timeSource:        timeSource,
		namespaceRegistry: namespaceRegistry,
		readerID:          readerID,
		loadTime:          util.MaxTime(timeSource.Now(), task.GetKey().FireTime),
		logger: log.NewLazyLogger(
			logger,
			func() []tag.Tag {
				return tasks.Tags(task)
			},
		),
		metricsHandler:                metricsHandler,
		taggedMetricsHandler:          metricsHandler,
		criticalRetryAttempt:          criticalRetryAttempt,
		filter:                        filter,
		namespaceCacheRefreshInterval: namespaceCacheRefreshInterval,
	}
	executable.updatePriority()
	return executable
}

func (e *executableImpl) Execute() error {
	if e.State() == ctasks.TaskStateCancelled {
		return nil
	}

	// this filter should also contain the logic for overriding
	// results from task allocator (force executing some standby task types)
	e.shouldProcess = true
	if e.filter != nil {
		if e.shouldProcess = e.filter(e.Task); !e.shouldProcess {
			return nil
		}
	}

	ctx := metrics.AddMetricsContext(context.Background())
	namespace, _ := e.namespaceRegistry.GetNamespaceName(namespace.ID(e.GetNamespaceID()))

	ctx = headers.SetCallerInfo(ctx, headers.NewBackgroundCallerInfo(namespace.String()))

	startTime := e.timeSource.Now()

	metricsTags, isActive, err := e.executor.Execute(ctx, e)
	e.taggedMetricsHandler = e.metricsHandler.WithTags(metricsTags...)

	if isActive != e.lastActiveness {
		// namespace did a failover, reset task attempt
		e.Lock()
		e.attempt = 0
		e.Unlock()
	}
	e.lastActiveness = isActive

	e.userLatency = 0
	if duration, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.GetMetricName()); ok {
		e.userLatency = time.Duration(duration)
	}

	e.taggedMetricsHandler.Timer(metrics.TaskProcessingLatency.GetMetricName()).Record(time.Since(startTime))
	e.taggedMetricsHandler.Timer(metrics.TaskProcessingUserLatency.GetMetricName()).Record(e.userLatency)

	priorityTaggedProvider := e.taggedMetricsHandler.WithTags(metrics.TaskPriorityTag(e.priority.String()))
	priorityTaggedProvider.Counter(metrics.TaskRequests.GetMetricName()).Record(1)
	priorityTaggedProvider.Timer(metrics.TaskScheduleLatency.GetMetricName()).Record(startTime.Sub(e.scheduledTime))

	return err
}

func (e *executableImpl) HandleErr(err error) (retErr error) {
	defer func() {
		if retErr != nil {
			e.Lock()
			defer e.Unlock()

			e.attempt++
			if e.attempt > e.criticalRetryAttempt() {
				e.taggedMetricsHandler.Histogram(metrics.TaskAttempt.GetMetricName(), metrics.TaskAttempt.GetMetricUnit()).Record(int64(e.attempt))
				e.logger.Error("Critical error processing task, retrying.", tag.Error(err), tag.OperationCritical)
			}
		}
	}()

	if err == nil {
		return nil
	}

	if common.IsResourceExhausted(err) {
		e.resourceExhaustedCount++
		e.taggedMetricsHandler.Counter(metrics.TaskThrottledCounter.GetMetricName()).Record(1)
		return err
	}
	e.resourceExhaustedCount = 0

	if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
		return nil
	}

	// This means that namespace is deleted, and it is safe to drop the task (=ignore the error).
	if _, isNotFound := err.(*serviceerror.NamespaceNotFound); isNotFound {
		return nil
	}

	if err == consts.ErrDependencyTaskNotCompleted {
		e.taggedMetricsHandler.Counter(metrics.TasksDependencyTaskNotCompleted.GetMetricName()).Record(1)
		return err
	}

	if err == consts.ErrTaskRetry {
		e.taggedMetricsHandler.Counter(metrics.TaskStandbyRetryCounter.GetMetricName()).Record(1)
		return err
	}

	if err == consts.ErrWorkflowBusy {
		e.taggedMetricsHandler.Counter(metrics.TaskWorkflowBusyCounter.GetMetricName()).Record(1)
		return err
	}

	if err == consts.ErrTaskDiscarded {
		e.taggedMetricsHandler.Counter(metrics.TaskDiscarded.GetMetricName()).Record(1)
		return nil
	}

	if err == consts.ErrTaskVersionMismatch {
		e.taggedMetricsHandler.Counter(metrics.TaskVersionMisMatch.GetMetricName()).Record(1)
		return nil
	}

	if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
		// TODO remove this error check special case after multi-cursor is enabled by default,
		// since the new task life cycle will not give up until task processed / verified
		// Currently, only run this check if filter is not nil which means we are running the old
		// active/passive queue logic.
		if e.filter != nil && e.timeSource.Now().Sub(e.loadTime) > 2*e.namespaceCacheRefreshInterval() {
			e.taggedMetricsHandler.Counter(metrics.TaskNotActiveCounter.GetMetricName()).Record(1)
			return nil
		}

		// error is expected when there's namespace failover,
		// so don't count it into task failures.
		return err
	}

	e.taggedMetricsHandler.Counter(metrics.TaskFailures.GetMetricName()).Record(1)

	e.logger.Error("Fail to process task", tag.Error(err), tag.LifeCycleProcessingFailed)
	return err
}

func (e *executableImpl) IsRetryableError(err error) bool {
	// this determines if the executable should be retried when hold the worker goroutine

	if e.State() == ctasks.TaskStateCancelled {
		return false
	}

	if shard.IsShardOwnershipLostError(err) {
		return false
	}

	// don't retry immediately for resource exhausted which may incur more load
	// context deadline exceed may also suggested downstream is overloaded, so don't retry immediately
	if common.IsResourceExhausted(err) || common.IsContextDeadlineExceededErr(err) {
		return false
	}

	// ErrTaskRetry means mutable state is not ready for standby task processing
	// there's no point for retrying the task immediately which will hold the worker corouinte
	// TODO: change ErrTaskRetry to a better name
	return err != consts.ErrTaskRetry && err != consts.ErrWorkflowBusy && err != consts.ErrDependencyTaskNotCompleted
}

func (e *executableImpl) RetryPolicy() backoff.RetryPolicy {
	// this is the retry policy for one submission
	// not for calculating the backoff after the task is nacked
	return schedulerRetryPolicy
}

func (e *executableImpl) Cancel() {
	e.Lock()
	defer e.Unlock()

	if e.state == ctasks.TaskStatePending {
		e.state = ctasks.TaskStateCancelled
	}
}

func (e *executableImpl) Ack() {
	e.Lock()
	defer e.Unlock()

	if e.state == ctasks.TaskStateCancelled {
		return
	}

	e.state = ctasks.TaskStateAcked

	if e.shouldProcess {
		e.taggedMetricsHandler.Timer(metrics.TaskLoadLatency.GetMetricName()).Record(
			e.loadTime.Sub(e.GetVisibilityTime()),
			metrics.QueueReaderIDTag(e.readerID),
		)
		e.taggedMetricsHandler.Histogram(metrics.TaskAttempt.GetMetricName(), metrics.TaskAttempt.GetMetricUnit()).Record(int64(e.attempt))

		priorityTaggedProvider := e.taggedMetricsHandler.WithTags(metrics.TaskPriorityTag(e.lowestPriority.String()))
		priorityTaggedProvider.Timer(metrics.TaskLatency.GetMetricName()).Record(time.Since(e.loadTime))

		readerIDTaggedProvider := priorityTaggedProvider.WithTags(metrics.QueueReaderIDTag(e.readerID))
		readerIDTaggedProvider.Timer(metrics.TaskQueueLatency.GetMetricName()).Record(time.Since(e.GetVisibilityTime()))
	}
}

func (e *executableImpl) Nack(err error) {
	if e.State() == ctasks.TaskStateCancelled {
		return
	}

	e.updatePriority()

	submitted := false
	if e.shouldResubmitOnNack(e.Attempt(), err) {
		// we do not need to know if there any error during submission
		// as long as it's not submitted, the execuable should be add
		// to the rescheduler
		e.SetScheduledTime(e.timeSource.Now())
		submitted = e.scheduler.TrySubmit(e)
	}

	if !submitted {
		e.rescheduler.Add(e, e.rescheduleTime(err, e.Attempt()))
	}
}

func (e *executableImpl) Reschedule() {
	if e.State() == ctasks.TaskStateCancelled {
		return
	}

	e.updatePriority()

	e.rescheduler.Add(e, e.rescheduleTime(nil, e.Attempt()))
}

func (e *executableImpl) State() ctasks.State {
	e.Lock()
	defer e.Unlock()

	return e.state
}

func (e *executableImpl) GetPriority() ctasks.Priority {
	e.Lock()
	defer e.Unlock()

	return e.priority
}

func (e *executableImpl) Attempt() int {
	e.Lock()
	defer e.Unlock()

	return e.attempt
}

func (e *executableImpl) GetTask() tasks.Task {
	return e.Task
}

func (e *executableImpl) GetScheduledTime() time.Time {
	return e.scheduledTime
}

func (e *executableImpl) SetScheduledTime(t time.Time) {
	e.scheduledTime = t
}

func (e *executableImpl) shouldResubmitOnNack(attempt int, err error) bool {
	// this is an optimization for skipping rescheduler and retry the task sooner.
	// this is useful for errors like workflow busy, which doesn't have to wait for
	// the longer rescheduling backoff.
	if attempt > resubmitMaxAttempts {
		return false
	}

	if common.IsResourceExhausted(err) &&
		e.resourceExhaustedCount > resourceExhaustedResubmitMaxAttempts {
		return false
	}

	if shard.IsShardOwnershipLostError(err) {
		return false
	}

	return err != consts.ErrTaskRetry && err != consts.ErrDependencyTaskNotCompleted
}

func (e *executableImpl) rescheduleTime(
	err error,
	attempt int,
) time.Time {
	// elapsedTime (the first parameter in ComputeNextDelay) is not relevant here
	// since reschedule policy has no expiration interval.

	if err == consts.ErrTaskRetry {
		// using a different reschedule policy to slow down retry
		// as the error means mutable state is not ready to handle the task,
		// need to wait for replication.
		return e.timeSource.Now().Add(taskNotReadyReschedulePolicy.ComputeNextDelay(0, attempt))
	} else if err == consts.ErrDependencyTaskNotCompleted {
		return e.timeSource.Now().Add(dependencyTaskNotCompletedReschedulePolicy.ComputeNextDelay(0, attempt))
	}

	backoff := reschedulePolicy.ComputeNextDelay(0, attempt)
	if common.IsResourceExhausted(err) {
		// try a different reschedule policy to slow down retry
		// upon resource exhausted error and pick the longer backoff
		// duration
		backoff = util.Max(backoff, taskResourceExhuastedReschedulePolicy.ComputeNextDelay(0, e.resourceExhaustedCount))
	}

	return e.timeSource.Now().Add(backoff)
}

func (e *executableImpl) updatePriority() {
	// do NOT invoke Assign while holding the lock
	newPriority := e.priorityAssigner.Assign(e)

	e.Lock()
	defer e.Unlock()
	e.priority = newPriority
	if e.priority > e.lowestPriority {
		e.lowestPriority = e.priority
	}
}
