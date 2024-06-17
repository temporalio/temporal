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
	"math"
	"regexp"
	"runtime/debug"
	"sync"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/circuitbreaker"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
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
		Execute(context.Context, Executable) ExecuteResponse
	}

	ExecuteResponse struct {
		// Following two fields are metadata of the execution
		// and should be populated by the executor even
		// when the actual task execution fails
		ExecutionMetricTags []metrics.Tag
		ExecutedAsActive    bool

		ExecutionErr error
	}

	ExecutorWrapper interface {
		Wrap(delegate Executor) Executor
	}

	// MaybeTerminalTaskError are errors which (if IsTerminalTaskError returns true) cannot be retried and should
	// not be rescheduled. Tasks should be enqueued to the DLQ immediately if an error is marked as terminal.
	MaybeTerminalTaskError interface {
		IsTerminalTaskError() bool
	}
)

var (
	ErrTerminalTaskFailure = errors.New("original task failed and this task is now to send the original to the DLQ")

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

// UnprocessableTaskError is an indicator that an executor does not know how to handle a task. Considered terminal.
type UnprocessableTaskError struct {
	Message string
}

// NewUnprocessableTaskError returns a new UnprocessableTaskError from given message.
func NewUnprocessableTaskError(message string) UnprocessableTaskError {
	return UnprocessableTaskError{Message: message}
}

func (e UnprocessableTaskError) Error() string {
	return "unprocessable task: " + e.Message
}

// IsTerminalTaskError marks this error as terminal to be handled appropriately.
func (UnprocessableTaskError) IsTerminalTaskError() bool {
	return true
}

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
		dlqWriter         *DLQWriter

		readerID                   int64
		loadTime                   time.Time
		scheduledTime              time.Time
		scheduleLatency            time.Duration
		attemptNoUserLatency       time.Duration
		inMemoryNoUserLatency      time.Duration
		lastActiveness             bool
		resourceExhaustedCount     int // does NOT include consts.ErrResourceExhaustedBusyWorkflow
		taggedMetricsHandler       metrics.Handler
		dlqEnabled                 dynamicconfig.BoolPropertyFn
		terminalFailureCause       error
		unexpectedErrorAttempts    int
		maxUnexpectedErrorAttempts dynamicconfig.IntPropertyFn
		dlqInternalErrors          dynamicconfig.BoolPropertyFn
		dlqErrorPattern            dynamicconfig.StringPropertyFn
	}
	ExecutableParams struct {
		DLQEnabled                 dynamicconfig.BoolPropertyFn
		DLQWriter                  *DLQWriter
		MaxUnexpectedErrorAttempts dynamicconfig.IntPropertyFn
		DLQInternalErrors          dynamicconfig.BoolPropertyFn
		DLQErrorPattern            dynamicconfig.StringPropertyFn
	}
	ExecutableOption func(*ExecutableParams)
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
	opts ...ExecutableOption,
) Executable {
	params := ExecutableParams{
		DLQEnabled: func() bool {
			return false
		},
		DLQWriter: nil,
		MaxUnexpectedErrorAttempts: func() int {
			return math.MaxInt
		},
		DLQInternalErrors: func() bool {
			return false
		},
		DLQErrorPattern: func() string {
			return ""
		},
	}
	for _, opt := range opts {
		opt(&params)
	}
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
		metricsHandler:             metricsHandler,
		taggedMetricsHandler:       metricsHandler,
		dlqWriter:                  params.DLQWriter,
		dlqEnabled:                 params.DLQEnabled,
		maxUnexpectedErrorAttempts: params.MaxUnexpectedErrorAttempts,
		dlqInternalErrors:          params.DLQInternalErrors,
		dlqErrorPattern:            params.DLQErrorPattern,
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
			e.taggedMetricsHandler = e.metricsHandler.WithTags(EstimateTaskMetricTag(e, e.namespaceRegistry, e.clusterMetadata.GetCurrentClusterName())...)
		}

		attemptUserLatency := time.Duration(0)
		if duration, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name()); ok {
			attemptUserLatency = time.Duration(duration)
		}

		attemptLatency := e.timeSource.Now().Sub(startTime)
		e.attemptNoUserLatency = attemptLatency - attemptUserLatency
		// emit total attempt latency so that we know how much time a task will occpy a worker goroutine
		metrics.TaskProcessingLatency.With(e.taggedMetricsHandler).Record(attemptLatency)

		priorityTaggedProvider := e.taggedMetricsHandler.WithTags(metrics.TaskPriorityTag(e.priority.String()))
		metrics.TaskRequests.With(priorityTaggedProvider).Record(1)
		metrics.TaskScheduleLatency.With(priorityTaggedProvider).Record(e.scheduleLatency)

		if retErr == nil {
			e.inMemoryNoUserLatency += e.scheduleLatency + e.attemptNoUserLatency
		}
		// if retErr is not nil, HandleErr will take care of the inMemoryNoUserLatency calculation
		// Not doing it here as for certain errors latency for the attempt should not be counted
	}()

	// A previous attempt has marked this executable as no longer retryable.
	// Instead of executing it, we try to write to the DLQ if enabled, otherwise - drop it.
	if e.terminalFailureCause != nil {
		if e.dlqEnabled() {
			return e.writeToDLQ(ctx)
		}
		if errors.As(e.terminalFailureCause, new(MaybeTerminalTaskError)) {
			e.logger.Warn(
				"Dropping task with terminal failure because DLQ was disabled",
				tag.Error(e.terminalFailureCause),
			)
			return nil
		}
		e.logger.Info("Retrying task with non-terminal DLQ failure because DLQ was disabled", tag.Error(e.terminalFailureCause))
		e.terminalFailureCause = nil
	}

	resp := e.executor.Execute(ctx, e)
	e.taggedMetricsHandler = e.metricsHandler.WithTags(resp.ExecutionMetricTags...)

	if resp.ExecutedAsActive != e.lastActiveness {
		// namespace did a failover,
		// reset task attempt since the execution logic used will change
		e.resetAttempt()
	}
	e.lastActiveness = resp.ExecutedAsActive

	return resp.ExecutionErr
}

func (e *executableImpl) writeToDLQ(ctx context.Context) error {
	start := e.timeSource.Now()
	err := e.dlqWriter.WriteTaskToDLQ(
		ctx,
		e.clusterMetadata.GetCurrentClusterName(),
		e.clusterMetadata.GetCurrentClusterName(),
		e.GetTask(),
	)
	if err != nil {
		metrics.TaskDLQFailures.With(e.taggedMetricsHandler).Record(1)
		e.logger.Error("Failed to write task to DLQ", tag.Error(err))
	}
	metrics.TaskDLQSendLatency.With(e.taggedMetricsHandler).Record(e.timeSource.Now().Sub(start))
	return err
}

func (e *executableImpl) isSafeToDropError(err error) bool {
	if errors.Is(err, consts.ErrStaleReference) {
		// The task is stale and is safe to be dropped.
		// Even though ErrStaleReference is castable to serviceerror.NotFound, we give this error special treatment
		// because we're interested in the metric.
		metrics.TaskSkipped.With(e.taggedMetricsHandler).Record(1)
		e.logger.Info("Skipped task due with stale reference", tag.Error(err))
		return true
	}

	if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
		return true
	}

	// This means that namespace is deleted, and it is safe to drop the task (=ignore the error).
	if _, isNotFound := err.(*serviceerror.NamespaceNotFound); isNotFound {
		return true
	}

	if err == consts.ErrTaskDiscarded {
		metrics.TaskDiscarded.With(e.taggedMetricsHandler).Record(1)
		return true
	}

	if err == consts.ErrTaskVersionMismatch {
		metrics.TaskVersionMisMatch.With(e.taggedMetricsHandler).Record(1)
		return true
	}

	return false
}

// Returns true when the error is expected and should be retried. You're expected to return
// an error in this case, as that possible-rewritten-error is what we'll return
func (e *executableImpl) isExpectedRetryableError(err error) (isRetryable bool, retErr error) {
	defer func() {
		// This is a guard against programming mistakes. Please don't return (true, nil).
		if isRetryable && retErr == nil {
			retErr = err
		}
	}()

	var resourceExhaustedErr *serviceerror.ResourceExhausted
	if errors.As(err, &resourceExhaustedErr) {
		if resourceExhaustedErr.Cause != enums.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW {
			if resourceExhaustedErr.Cause == enums.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT {
				err = consts.ErrResourceExhaustedAPSLimit
			}
			e.resourceExhaustedCount++
			metrics.TaskThrottledCounter.With(e.taggedMetricsHandler).Record(1)
			return true, err
		}

		err = consts.ErrResourceExhaustedBusyWorkflow
	}
	e.resourceExhaustedCount = 0

	if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
		// error is expected when there's namespace failover,
		// so don't count it into task failures.
		metrics.TaskNotActiveCounter.With(e.taggedMetricsHandler).Record(1)
		return true, err
	}

	if err == consts.ErrDependencyTaskNotCompleted {
		metrics.TasksDependencyTaskNotCompleted.With(e.taggedMetricsHandler).Record(1)
		return true, err
	}

	if err == consts.ErrTaskRetry {
		metrics.TaskStandbyRetryCounter.With(e.taggedMetricsHandler).Record(1)
		return true, err
	}

	if errors.Is(err, consts.ErrResourceExhaustedBusyWorkflow) {
		metrics.TaskWorkflowBusyCounter.With(e.taggedMetricsHandler).Record(1)
		return true, err
	}

	if err.Error() == consts.ErrNamespaceHandover.Error() {
		metrics.TaskNamespaceHandoverCounter.With(e.taggedMetricsHandler).Record(1)
		return true, consts.ErrNamespaceHandover
	}

	return false, nil
}

func (e *executableImpl) isUnexpectedNonRetryableError(err error) bool {
	var terr MaybeTerminalTaskError
	if errors.As(err, &terr) {
		return terr.IsTerminalTaskError()
	}

	if _, isDataLoss := err.(*serviceerror.DataLoss); isDataLoss {
		return true
	}

	isInternalError := common.IsInternalError(err)
	if isInternalError {
		metrics.TaskInternalErrorCounter.With(e.taggedMetricsHandler).Record(1)
		// Only DQL/drop when configured to
		return e.dlqInternalErrors()
	}

	return false
}

// HandleErr processes the error returned by task execution.
//
// Returns nil if the task should be completed, and an error if the task should be retried.
func (e *executableImpl) HandleErr(err error) (retErr error) {
	if err == nil {
		return nil
	}

	defer func() {
		if !errors.Is(retErr, consts.ErrResourceExhaustedBusyWorkflow) &&
			!errors.Is(retErr, consts.ErrResourceExhaustedAPSLimit) {
			// if err is due to workflow busy or APS limit, do not take any latency related to this attempt into account
			e.inMemoryNoUserLatency += e.scheduleLatency + e.attemptNoUserLatency
		}

		if retErr != nil {
			e.Lock()
			defer e.Unlock()

			e.attempt++
			if e.attempt > taskCriticalLogMetricAttempts {
				metrics.TaskAttempt.With(e.taggedMetricsHandler).Record(int64(e.attempt))
				e.logger.Error("Critical error processing task, retrying.",
					tag.Attempt(int32(e.attempt)),
					tag.UnexpectedErrorAttempts(int32(e.unexpectedErrorAttempts)),
					tag.Error(err),
					tag.OperationCritical)
			}
		}
	}()

	if len(e.dlqErrorPattern()) > 0 {
		match, mErr := regexp.MatchString(e.dlqErrorPattern(), err.Error())
		if mErr != nil {
			e.logger.Error(fmt.Sprintf("Failed to match task processing error with %s", dynamicconfig.HistoryTaskDLQErrorPattern.Key()))
		} else if match {
			e.logger.Error(
				fmt.Sprintf("Error matches with %s. Marking task as terminally failed, will send to DLQ",
					dynamicconfig.HistoryTaskDLQErrorPattern.Key()),
				tag.Error(err),
				tag.ErrorType(err))
			e.terminalFailureCause = err
			metrics.TaskTerminalFailures.With(e.taggedMetricsHandler).Record(1)
			return fmt.Errorf("%w: %v", ErrTerminalTaskFailure, err)
		}
	}

	if e.isSafeToDropError(err) {
		return nil
	}

	if ok, rewrittenErr := e.isExpectedRetryableError(err); ok {
		return rewrittenErr
	}
	// Unexpected errors handled below
	e.unexpectedErrorAttempts++
	metrics.TaskFailures.With(e.taggedMetricsHandler).Record(1)
	e.logger.Warn("Fail to process task", tag.Error(err), tag.ErrorType(err), tag.UnexpectedErrorAttempts(int32(e.unexpectedErrorAttempts)), tag.LifeCycleProcessingFailed)

	if e.isUnexpectedNonRetryableError(err) {
		// Terminal errors are likely due to data corruption.
		// Drop the task by returning nil so that task will be marked as completed,
		// or send it to the DLQ if that is enabled.
		metrics.TaskCorruptionCounter.With(e.taggedMetricsHandler).Record(1)
		if e.dlqEnabled() {
			// Keep this message in sync with the log line mentioned in Investigation section of docs/admin/dlq.md
			e.logger.Error("Marking task as terminally failed, will send to DLQ", tag.Error(err), tag.ErrorType(err))
			e.terminalFailureCause = err // <- Execute() examines this attribute on the next attempt.
			metrics.TaskTerminalFailures.With(e.taggedMetricsHandler).Record(1)
			return fmt.Errorf("%w: %v", ErrTerminalTaskFailure, err)
		}
		e.logger.Error("Dropping task due to terminal error", tag.Error(err), tag.ErrorType(err))
		return nil
	}

	// Unexpected but retryable error
	if e.unexpectedErrorAttempts >= e.maxUnexpectedErrorAttempts() && e.dlqEnabled() {
		// Keep this message in sync with the log line mentioned in Investigation section of docs/admin/dlq.md
		e.logger.Error("Marking task as terminally failed, will send to DLQ. Maximum number of attempts with unexpected errors",
			tag.UnexpectedErrorAttempts(int32(e.unexpectedErrorAttempts)), tag.Error(err))
		e.terminalFailureCause = err // <- Execute() examines this attribute on the next attempt.
		metrics.TaskTerminalFailures.With(e.taggedMetricsHandler).Record(1)
		return fmt.Errorf("%w: %w", ErrTerminalTaskFailure, e.terminalFailureCause)
	}

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

	metrics.TaskLoadLatency.With(e.taggedMetricsHandler).Record(
		e.loadTime.Sub(e.GetVisibilityTime()),
		metrics.QueueReaderIDTag(e.readerID),
	)
	metrics.TaskAttempt.With(e.taggedMetricsHandler).Record(int64(e.attempt))

	priorityTaggedProvider := e.taggedMetricsHandler.WithTags(metrics.TaskPriorityTag(e.lowestPriority.String()))
	metrics.TaskLatency.With(priorityTaggedProvider).Record(e.inMemoryNoUserLatency)
	metrics.TaskQueueLatency.With(priorityTaggedProvider.WithTags(metrics.QueueReaderIDTag(e.readerID))).
		Record(time.Since(e.GetVisibilityTime()))
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
		if !errors.Is(err, consts.ErrResourceExhaustedBusyWorkflow) &&
			!errors.Is(err, consts.ErrResourceExhaustedAPSLimit) {
			e.inMemoryNoUserLatency += backoffDuration
		}

		e.rescheduler.Add(e, e.timeSource.Now().Add(backoffDuration))
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

// GetDestination returns the embedded task's destination if it exists. Defaults to an empty string.
func (e *executableImpl) GetDestination() string {
	if t, ok := e.Task.(tasks.HasDestination); ok {
		return t.GetDestination()
	}
	return ""
}

// StateMachineTaskType returns the embedded task's state machine task type if it exists. Defaults to 0.
func (e *executableImpl) StateMachineTaskType() int32 {
	if t, ok := e.Task.(tasks.HasStateMachineTaskType); ok {
		return t.StateMachineTaskType()
	}
	return 0
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
		e.resourceExhaustedCount > resourceExhaustedResubmitMaxAttempts {
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
		backoffDuration = max(
			backoffDuration,
			taskResourceExhuastedReschedulePolicy.ComputeNextDelay(0, e.resourceExhaustedCount),
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

func EstimateTaskMetricTag(
	e Executable,
	namespaceRegistry namespace.Registry,
	currentClusterName string,
) []metrics.Tag {
	namespaceTag := metrics.NamespaceUnknownTag()
	isActive := true

	ns, err := namespaceRegistry.GetNamespaceByID(namespace.ID(e.GetNamespaceID()))
	if err == nil {
		namespaceTag = metrics.NamespaceTag(ns.Name().String())
		isActive = ns.ActiveInCluster(currentClusterName)
	}

	taskType := getTaskTypeTagValue(e, isActive)
	return []metrics.Tag{
		namespaceTag,
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType), // for backward compatibility
		// TODO: add task priority tag here as well
	}
}

// CircuitBreakerExecutable wraps Executable with a circuit breaker.
// If the executable returns DestinationDownError, it will signal the circuit breaker
// of failure, and return the inner error.
type CircuitBreakerExecutable struct {
	Executable
	cb circuitbreaker.TwoStepCircuitBreaker
}

func NewCircuitBreakerExecutable(
	e Executable,
	cb circuitbreaker.TwoStepCircuitBreaker,
) *CircuitBreakerExecutable {
	return &CircuitBreakerExecutable{
		Executable: e,
		cb:         cb,
	}
}

// This is roughly the same implementation of the `gobreaker.CircuitBreaker.Execute` function,
// but checks if the error is `DestinationDownError` to report success, and unwrap it.
func (e *CircuitBreakerExecutable) Execute() error {
	doneCb, err := e.cb.Allow()
	if err != nil {
		return err
	}

	defer func() {
		e := recover()
		if e != nil {
			doneCb(false)
			panic(e)
		}
	}()

	err = e.Executable.Execute()
	var destinationDownErr *DestinationDownError
	if errors.As(err, &destinationDownErr) {
		err = destinationDownErr.Unwrap()
	}

	doneCb(destinationDownErr == nil)
	return err
}

// DestinationDownError indicates the destination is down and wraps another error.
// It is a useful specific error that can be used, for example, in a circuit breaker
// to distinguish when a destination service is down and an internal error.
type DestinationDownError struct {
	Message string
	err     error
}

func NewDestinationDownError(msg string, err error) *DestinationDownError {
	return &DestinationDownError{
		Message: "destination down: " + msg,
		err:     err,
	}
}

func (e *DestinationDownError) Error() string {
	msg := e.Message
	if e.err != nil {
		msg += "\n" + e.err.Error()
	}
	return msg
}

func (e *DestinationDownError) Unwrap() error {
	return e.err
}
