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
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
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

	ExecutorWrapper interface {
		Wrap(delegate Executor) Executor
	}
)

var (
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
	// taskCriticalLogMetricAttempts, if exceeded, task attempts metrics and critical processing error log will be emitted
	// while task is retrying
	taskCriticalLogMetricAttempts = 30
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
		clusterMetadata   cluster.Metadata
		logger            log.Logger
		metricsHandler    metrics.Handler

		readerID                     int64
		loadTime                     time.Time
		scheduledTime                time.Time
		scheduleLatency              time.Duration
		attemptNoUserLatency         time.Duration
		inMemoryNoUserLatency        time.Duration
		lastActiveness               bool
		systemResourceExhaustedCount int
		taggedMetricsHandler         metrics.Handler
	}
)

func NewExecutable(
	readerID int64,
	task tasks.Task,
	executor Executor,
	scheduler Scheduler,
	rescheduler Rescheduler,
	priorityAssigner PriorityAssigner,
	timeSource clock.TimeSource,
	namespaceRegistry namespace.Registry,
	clusterMetadata cluster.Metadata,
	logger log.Logger,
	metricsHandler metrics.Handler,
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
		clusterMetadata:   clusterMetadata,
		readerID:          readerID,
		loadTime:          util.MaxTime(timeSource.Now(), task.GetKey().FireTime),
		logger: log.NewLazyLogger(
			logger,
			func() []tag.Tag {
				return tasks.Tags(task)
			},
		),
		metricsHandler:       metricsHandler,
		taggedMetricsHandler: metricsHandler,
	}
	executable.updatePriority()
	return executable
}

func (e *executableImpl) Execute() (retErr error) {

	startTime := e.timeSource.Now()
	e.scheduleLatency = startTime.Sub(e.scheduledTime)

	e.Lock()
	if e.state != ctasks.TaskStatePending {
		e.Unlock()
		return nil
	}

	ns, _ := e.namespaceRegistry.GetNamespaceName(namespace.ID(e.GetNamespaceID()))
	var callerInfo headers.CallerInfo
	switch e.priority {
	case ctasks.PriorityHigh:
		callerInfo = headers.NewBackgroundCallerInfo(ns.String())
	default:
		// priority low or unknown
		callerInfo = headers.NewPreemptableCallerInfo(ns.String())
	}
	ctx := headers.SetCallerInfo(
		metrics.AddMetricsContext(context.Background()),
		callerInfo,
	)
	e.Unlock()

	defer func() {
		if panicObj := recover(); panicObj != nil {
			err, ok := panicObj.(error)
			if !ok {
				err = serviceerror.NewInternal(fmt.Sprintf("panic: %v", panicObj))
			}

			e.logger.Error("Panic is captured", tag.SysStackTrace(string(debug.Stack())), tag.Error(err))
			retErr = err

			// we need to guess the metrics tags here as we don't know which execution logic
			// is actually used which is upto the executor implementation
			e.taggedMetricsHandler = e.metricsHandler.WithTags(e.estimateTaskMetricTag()...)
		}

		attemptUserLatency := time.Duration(0)
		if duration, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.GetMetricName()); ok {
			attemptUserLatency = time.Duration(duration)
		}

		attemptLatency := e.timeSource.Now().Sub(startTime)
		e.attemptNoUserLatency = attemptLatency - attemptUserLatency
		// emit total attempt latency so that we know how much time a task will occpy a worker goroutine
		e.taggedMetricsHandler.Timer(metrics.TaskProcessingLatency.GetMetricName()).Record(attemptLatency)

		priorityTaggedProvider := e.taggedMetricsHandler.WithTags(metrics.TaskPriorityTag(e.priority.String()))
		priorityTaggedProvider.Counter(metrics.TaskRequests.GetMetricName()).Record(1)
		priorityTaggedProvider.Timer(metrics.TaskScheduleLatency.GetMetricName()).Record(e.scheduleLatency)

		if retErr == nil {
			e.inMemoryNoUserLatency += e.scheduleLatency + e.attemptNoUserLatency
		}
		// if retErr is not nil, HandleErr will take care of the inMemoryNoUserLatency calculation
		// Not doing it here as for certain errors latency for the attempt should not be counted
	}()

	metricsTags, isActive, err := e.executor.Execute(ctx, e)
	e.taggedMetricsHandler = e.metricsHandler.WithTags(metricsTags...)

	if isActive != e.lastActiveness {
		// namespace did a failover,
		// reset task attempt since the execution logic used will change
		e.resetAttempt()
	}
	e.lastActiveness = isActive

	return err
}

func (e *executableImpl) HandleErr(err error) (retErr error) {
	if err == nil {
		return nil
	}

	defer func() {
		if !errors.Is(retErr, consts.ErrResourceExhaustedBusyWorkflow) {
			// if err is due to workflow busy, do not take any latency related to this attempt into account
			e.inMemoryNoUserLatency += e.scheduleLatency + e.attemptNoUserLatency
		}

		if retErr != nil {
			e.Lock()
			defer e.Unlock()

			e.attempt++
			if e.attempt > taskCriticalLogMetricAttempts {
				e.taggedMetricsHandler.Histogram(metrics.TaskAttempt.GetMetricName(), metrics.TaskAttempt.GetMetricUnit()).Record(int64(e.attempt))
				e.logger.Error("Critical error processing task, retrying.", tag.Attempt(int32(e.attempt)), tag.Error(err), tag.OperationCritical)
			}
		}
	}()

	var resourceExhaustedErr *serviceerror.ResourceExhausted
	if errors.As(err, &resourceExhaustedErr) {
		if resourceExhaustedErr.Cause != enums.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW {
			e.systemResourceExhaustedCount++
			e.taggedMetricsHandler.Counter(metrics.TaskThrottledCounter.GetMetricName()).Record(1)
			return err
		}

		err = consts.ErrResourceExhaustedBusyWorkflow
	}
	e.systemResourceExhaustedCount = 0

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

	if errors.Is(err, consts.ErrResourceExhaustedBusyWorkflow) {
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

	if err.Error() == consts.ErrNamespaceHandover.Error() {
		e.taggedMetricsHandler.Counter(metrics.TaskNamespaceHandoverCounter.GetMetricName()).Record(1)
		err = consts.ErrNamespaceHandover
		return err
	}

	if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
		// error is expected when there's namespace failover,
		// so don't count it into task failures.
		e.taggedMetricsHandler.Counter(metrics.TaskNotActiveCounter.GetMetricName()).Record(1)
		return err
	}

	var deserializationError *serialization.DeserializationError
	var encodingTypeError *serialization.UnknownEncodingTypeError
	if errors.As(err, &deserializationError) || errors.As(err, &encodingTypeError) {
		// likely due to data corruption, emit logs, metrics & drop the task by return nil so that
		// task will be marked as completed.
		e.taggedMetricsHandler.Counter(metrics.TaskCorruptionCounter.GetMetricName()).Record(1)
		e.logger.Error("Drop task due to serialization error", tag.Error(err))
		return nil
	}

	e.taggedMetricsHandler.Counter(metrics.TaskFailures.GetMetricName()).Record(1)

	e.logger.Error("Fail to process task", tag.Error(err), tag.LifeCycleProcessingFailed)
	return err
}

func (e *executableImpl) IsRetryableError(err error) bool {
	// this determines if the executable should be retried when hold the worker goroutine
	//
	// never retry task while holding the goroutine, and rely on shouldResubmitOnNack
	return false
}

func (e *executableImpl) RetryPolicy() backoff.RetryPolicy {
	// this is the retry policy for one submission
	// not for calculating the backoff after the task is nacked
	//
	// never retry task while holding the goroutine, and rely on shouldResubmitOnNack
	return backoff.DisabledRetryPolicy
}

func (e *executableImpl) Abort() {
	e.Lock()
	defer e.Unlock()

	if e.state == ctasks.TaskStatePending {
		e.state = ctasks.TaskStateAborted
	}
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

	if e.state != ctasks.TaskStatePending {
		return
	}

	e.state = ctasks.TaskStateAcked

	e.taggedMetricsHandler.Timer(metrics.TaskLoadLatency.GetMetricName()).Record(
		e.loadTime.Sub(e.GetVisibilityTime()),
		metrics.QueueReaderIDTag(e.readerID),
	)
	e.taggedMetricsHandler.Histogram(metrics.TaskAttempt.GetMetricName(), metrics.TaskAttempt.GetMetricUnit()).Record(int64(e.attempt))

	priorityTaggedProvider := e.taggedMetricsHandler.WithTags(metrics.TaskPriorityTag(e.lowestPriority.String()))
	priorityTaggedProvider.Timer(metrics.TaskLatency.GetMetricName()).Record(e.inMemoryNoUserLatency)

	readerIDTaggedProvider := priorityTaggedProvider.WithTags(metrics.QueueReaderIDTag(e.readerID))
	readerIDTaggedProvider.Timer(metrics.TaskQueueLatency.GetMetricName()).Record(time.Since(e.GetVisibilityTime()))
}

func (e *executableImpl) Nack(err error) {
	state := e.State()
	if state != ctasks.TaskStatePending {
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
		backoffDuration := e.backoffDuration(err, e.Attempt())
		e.rescheduler.Add(e, e.timeSource.Now().Add(backoffDuration))
		if !errors.Is(err, consts.ErrResourceExhaustedBusyWorkflow) {
			e.inMemoryNoUserLatency += backoffDuration
		}
	}
}

func (e *executableImpl) Reschedule() {
	state := e.State()
	if state != ctasks.TaskStatePending {
		return
	}

	e.updatePriority()

	e.rescheduler.Add(e, e.timeSource.Now().Add(e.backoffDuration(nil, e.Attempt())))
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

	if !errors.Is(err, consts.ErrResourceExhaustedBusyWorkflow) &&
		common.IsResourceExhausted(err) &&
		e.systemResourceExhaustedCount > resourceExhaustedResubmitMaxAttempts {
		return false
	}

	if shard.IsShardOwnershipLostError(err) {
		return false
	}

	if common.IsInternalError(err) {
		return false
	}

	return err != consts.ErrTaskRetry &&
		err != consts.ErrDependencyTaskNotCompleted &&
		err != consts.ErrNamespaceHandover
}

func (e *executableImpl) backoffDuration(
	err error,
	attempt int,
) time.Duration {
	// elapsedTime, the first parameter in ComputeNextDelay is not relevant here
	// since reschedule policy has no expiration interval.

	if err == consts.ErrTaskRetry ||
		err == consts.ErrNamespaceHandover ||
		common.IsInternalError(err) {
		// using a different reschedule policy to slow down retry
		// as immediate retry typically won't resolve the issue.
		return taskNotReadyReschedulePolicy.ComputeNextDelay(0, attempt)
	}

	if err == consts.ErrDependencyTaskNotCompleted {
		return dependencyTaskNotCompletedReschedulePolicy.ComputeNextDelay(0, attempt)
	}

	backoffDuration := reschedulePolicy.ComputeNextDelay(0, attempt)
	if !errors.Is(err, consts.ErrResourceExhaustedBusyWorkflow) && common.IsResourceExhausted(err) {
		// try a different reschedule policy to slow down retry
		// upon system resource exhausted error and pick the longer backoff duration
		backoffDuration = util.Max(
			backoffDuration,
			taskResourceExhuastedReschedulePolicy.ComputeNextDelay(0, e.systemResourceExhaustedCount),
		)
	}

	return backoffDuration
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

func (e *executableImpl) resetAttempt() {
	e.Lock()
	defer e.Unlock()

	e.attempt = 1
}

func (e *executableImpl) estimateTaskMetricTag() []metrics.Tag {
	namespaceTag := metrics.NamespaceUnknownTag()
	isActive := true

	namespace, err := e.namespaceRegistry.GetNamespaceByID(namespace.ID(e.GetNamespaceID()))
	if err == nil {
		namespaceTag = metrics.NamespaceTag(namespace.Name().String())
		isActive = namespace.ActiveInCluster(e.clusterMetadata.GetCurrentClusterName())
	}

	taskType := getTaskTypeTagValue(e, isActive)
	return []metrics.Tag{
		namespaceTag,
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType), // for backward compatibility
	}
}
