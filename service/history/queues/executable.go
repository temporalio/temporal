//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination executable_mock.go

package queues

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"runtime/debug"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
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
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/consts"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	Executable interface {
		ctasks.Task
		tasks.Task

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

var defaultExecutableMetricsTags = []metrics.Tag{
	metrics.NamespaceUnknownTag(),
	metrics.TaskTypeTag("__unknown__"),
	metrics.OperationTag("__unknown__"),
}

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
		state ctasks.State

		executor            Executor
		scheduler           Scheduler
		rescheduler         Rescheduler
		priorityAssigner    PriorityAssigner
		timeSource          clock.TimeSource
		namespaceRegistry   namespace.Registry
		clusterMetadata     cluster.Metadata
		chasmRegistry       *chasm.Registry
		taskTypeTagProvider TaskTypeTagProvider
		logger              log.Logger
		metricsHandler      metrics.Handler
		tracer              trace.Tracer
		dlqWriter           *DLQWriter

		readerID                   int64
		attempt                    int
		priority                   ctasks.Priority
		scheduledTime              time.Time
		scheduleLatency            time.Duration
		attemptNoUserLatency       time.Duration
		inMemoryNoUserLatency      time.Duration
		lastActiveness             bool
		invalidTask                bool
		resourceExhaustedCount     int // does NOT include consts.ErrResourceExhaustedBusyWorkflow
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

	TaskTypeTagProvider func(t tasks.Task, isActive bool, chasmRegistry *chasm.Registry) string
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
	chasmRegistry *chasm.Registry,
	taskTypeTagProvider TaskTypeTagProvider,
	logger log.Logger,
	metricsHandler metrics.Handler,
	tracer trace.Tracer,
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
	e := &executableImpl{
		Task:  task,
		state: ctasks.TaskStatePending,

		attempt:             1,
		executor:            executor,
		scheduler:           scheduler,
		rescheduler:         rescheduler,
		priorityAssigner:    priorityAssigner,
		timeSource:          timeSource,
		namespaceRegistry:   namespaceRegistry,
		clusterMetadata:     clusterMetadata,
		chasmRegistry:       chasmRegistry,
		taskTypeTagProvider: taskTypeTagProvider,
		readerID:            readerID,
		logger: log.NewLazyLogger(
			logger,
			func() []tag.Tag {
				return tasks.Tags(task)
			},
		),
		metricsHandler: metricsHandler.WithTags(estimateTaskMetricTags(
			task,
			namespaceRegistry,
			clusterMetadata.GetCurrentClusterName(),
			chasmRegistry,
			taskTypeTagProvider,
		)...),
		tracer:                     tracer,
		dlqWriter:                  params.DLQWriter,
		dlqEnabled:                 params.DLQEnabled,
		maxUnexpectedErrorAttempts: params.MaxUnexpectedErrorAttempts,
		dlqInternalErrors:          params.DLQInternalErrors,
		dlqErrorPattern:            params.DLQErrorPattern,
	}
	e.priority = priorityAssigner.Assign(e)

	loadTime := util.MaxTime(timeSource.Now(), task.GetKey().FireTime)
	metrics.TaskLoadLatency.With(e.metricsHandler).Record(
		loadTime.Sub(task.GetVisibilityTime()),
		metrics.QueueReaderIDTag(readerID),
	)
	return e
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
		callerInfo = headers.NewBackgroundHighCallerInfo(ns.String())
	case ctasks.PriorityLow:
		callerInfo = headers.NewBackgroundLowCallerInfo(ns.String())
	default:
		// priority preemptable or unknown
		callerInfo = headers.NewPreemptableCallerInfo(ns.String())
	}
	ctx := headers.SetCallerInfo(
		metrics.AddMetricsContext(context.Background()),
		callerInfo,
	)
	e.Unlock()

	// Wrapped in if block to avoid unnecessary allocations when OTEL is disabled.
	if telemetry.IsEnabled(e.tracer) {
		var span trace.Span
		ctx, span = e.tracer.Start(
			ctx,
			fmt.Sprintf("queue.Execute/%v", e.GetType().String()),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.Key(telemetry.WorkflowIDKey).String(e.GetWorkflowID()),
				attribute.Key(telemetry.WorkflowRunIDKey).String(e.GetRunID()),
				attribute.Key("queue.task.type").String(e.GetType().String()),
				attribute.Key("queue.task.id").Int64(e.GetTaskID())))

		if telemetry.DebugMode() {
			if taskPayload, err := json.Marshal(e.GetTask()); err != nil {
				e.logger.Error("failed to serialize task payload for OTEL span", tag.Error(err))
			} else {
				span.SetAttributes(attribute.Key("queue.task.payload").String(string(taskPayload)))
			}
		}

		defer func() {
			if retErr != nil {
				span.RecordError(retErr)
			}
			span.End()
		}()
	}

	defer func() {
		if pObj := recover(); pObj != nil {
			err, ok := pObj.(error)
			if !ok {
				err = serviceerror.NewInternalf("panic: %v", pObj)
			}

			e.logger.Error("Panic is captured", tag.SysStackTrace(string(debug.Stack())), tag.Error(err))
			retErr = err

			// we need to guess the metrics tags here as we don't know which execution logic
			// is actually used which is upto the executor implementation
			e.metricsHandler = e.metricsHandler.WithTags(
				estimateTaskMetricTags(
					e.GetTask(),
					e.namespaceRegistry,
					e.clusterMetadata.GetCurrentClusterName(),
					e.chasmRegistry,
					e.taskTypeTagProvider,
				)...)
		}

		attemptUserLatency := time.Duration(0)
		if duration, ok := metrics.ContextCounterGet(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name()); ok {
			attemptUserLatency = time.Duration(duration)
		}

		attemptLatency := e.timeSource.Now().Sub(startTime)
		e.attemptNoUserLatency = attemptLatency - attemptUserLatency
		// emit total attempt latency so that we know how much time a task will occpy a worker goroutine
		metrics.TaskProcessingLatency.With(e.metricsHandler).Record(attemptLatency)

		priorityTaggedProvider := e.metricsHandler.WithTags(metrics.TaskPriorityTag(e.priority.String()))
		metrics.TaskRequests.With(priorityTaggedProvider).Record(1)
		metrics.TaskScheduleLatency.With(priorityTaggedProvider).Record(e.scheduleLatency)
		metrics.OperationCounter.With(e.metricsHandler).Record(1)

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
	e.metricsHandler = e.metricsHandler.WithTags(resp.ExecutionMetricTags...)

	if resp.ExecutedAsActive != e.lastActiveness {
		// namespace did a failover,
		// reset task attempt since the execution logic used will change
		e.attempt = 1
	}
	e.lastActiveness = resp.ExecutedAsActive

	return resp.ExecutionErr
}

func (e *executableImpl) writeToDLQ(ctx context.Context) error {

	currentClusterName := e.clusterMetadata.GetCurrentClusterName()
	numShards := e.clusterMetadata.GetAllClusterInfo()[currentClusterName].ShardCount

	start := e.timeSource.Now()
	err := e.dlqWriter.WriteTaskToDLQ(
		ctx,
		currentClusterName,
		currentClusterName,
		tasks.GetShardIDForTask(e.Task, int(numShards)),
		e.GetTask(),
		e.lastActiveness,
	)
	if err != nil {
		metrics.TaskDLQFailures.With(e.metricsHandler).Record(1)
		e.logger.Error("Failed to write task to DLQ", tag.Error(err))
	}
	metrics.TaskDLQSendLatency.With(e.metricsHandler).Record(e.timeSource.Now().Sub(start))
	return err
}

func (e *executableImpl) isUserError(err error) bool {
	// All namespace level resource exhausted errors are considered user errors.
	var resourceExhaustedErr *serviceerror.ResourceExhausted
	if ok := errors.As(err, &resourceExhaustedErr); !ok {
		return false
	}
	return resourceExhaustedErr.Scope == enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE
}

func (e *executableImpl) isInvalidTaskError(err error) bool {
	if errors.Is(err, consts.ErrStaleReference) {
		// The task is stale and is safe to be dropped.
		// Even though ErrStaleReference is castable to serviceerror.NotFound, we give this error special treatment
		// because we're interested in the metric.
		metrics.TaskSkipped.With(e.metricsHandler).Record(1)
		e.logger.Info("Skipped task due to stale reference", tag.Error(err))
		return true
	}

	if errors.As(err, new(*serviceerror.NotFound)) {
		return true
	}

	// This means that namespace is deleted, and it is safe to drop the task (=ignore the error).
	if _, isNotFound := err.(*serviceerror.NamespaceNotFound); isNotFound {
		return true
	}

	if err == consts.ErrTaskVersionMismatch {
		metrics.TaskVersionMisMatch.With(e.metricsHandler).Record(1)
		return true
	}

	return false
}

func (e *executableImpl) isSafeToDropError(err error) bool {
	if err == consts.ErrTaskDiscarded {
		metrics.TaskDiscarded.With(e.metricsHandler).Record(1)
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
		switch resourceExhaustedErr.Cause { //nolint:exhaustive
		case enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW:
			err = consts.ErrResourceExhaustedBusyWorkflow
		case enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT:
			err = consts.ErrResourceExhaustedAPSLimit
			e.resourceExhaustedCount++
		default:
			e.resourceExhaustedCount++
		}

		metrics.TaskThrottledCounter.With(e.metricsHandler).Record(
			1, metrics.ResourceExhaustedCauseTag(resourceExhaustedErr.Cause))
		return true, err
	}
	e.resourceExhaustedCount = 0

	if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
		// error is expected when there's namespace failover,
		// so don't count it into task failures.
		metrics.TaskNotActiveCounter.With(e.metricsHandler).Record(1)
		return true, err
	}

	if err == consts.ErrDependencyTaskNotCompleted {
		metrics.TasksDependencyTaskNotCompleted.With(e.metricsHandler).Record(1)
		return true, err
	}

	if err == consts.ErrTaskRetry {
		metrics.TaskStandbyRetryCounter.With(e.metricsHandler).Record(1)
		return true, err
	}

	if err.Error() == consts.ErrNamespaceHandover.Error() {
		metrics.TaskNamespaceHandoverCounter.With(e.metricsHandler).Record(1)
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
		metrics.TaskInternalErrorCounter.With(e.metricsHandler).Record(1)
		// Only DQL/drop when configured to
		shouldDLQ := e.dlqInternalErrors()
		return shouldDLQ
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
		// If err is due to user error, do not take any latency related to this attempt into account
		if !e.isUserError(retErr) {
			e.inMemoryNoUserLatency += e.scheduleLatency + e.attemptNoUserLatency
		}
	}()

	if matchedErr := e.matchDLQErrorPattern(err); matchedErr != nil {
		e.incAttempt()
		return matchedErr
	}

	if e.isInvalidTaskError(err) {
		// only consider task invalid if it's the first attempt
		// otherwise we have no idea if it's invalid due to the (failed) write operation in previous attempts.
		e.invalidTask = e.attempt == 1
		return nil
	}

	if e.isSafeToDropError(err) {
		return nil
	}

	e.incAttempt()

	if ok, rewrittenErr := e.isExpectedRetryableError(err); ok {
		return rewrittenErr
	}

	// Unexpected errors handled below
	e.unexpectedErrorAttempts++
	metrics.TaskFailures.With(e.metricsHandler).Record(1)
	logger := log.With(e.logger,
		tag.Error(err),
		tag.ErrorType(err),
		tag.Attempt(int32(e.attempt)),
		tag.UnexpectedErrorAttempts(int32(e.unexpectedErrorAttempts)),
		tag.LifeCycleProcessingFailed,
		tag.NewStringTag("task-category", e.GetCategory().Name()),
	)
	if e.attempt > taskCriticalLogMetricAttempts {
		logger.Error("Critical error processing task, retrying.", tag.OperationCritical)
	} else {
		logger.Warn("Fail to process task")
	}

	if e.isUnexpectedNonRetryableError(err) {
		// Terminal errors are likely due to data corruption.
		// Drop the task by returning nil so that task will be marked as completed,
		// or send it to the DLQ if that is enabled.
		metrics.TaskCorruptionCounter.With(e.metricsHandler).Record(1)
		if e.dlqEnabled() {
			// Keep this message in sync with the log line mentioned in Investigation section of docs/admin/dlq.md
			e.logger.Error("Marking task as terminally failed, will send to DLQ", tag.Error(err), tag.ErrorType(err))
			e.terminalFailureCause = err // <- Execute() examines this attribute on the next attempt.
			metrics.TaskTerminalFailures.With(e.metricsHandler).Record(1)
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
		metrics.TaskTerminalFailures.With(e.metricsHandler).Record(1)
		return fmt.Errorf("%w: %w", ErrTerminalTaskFailure, e.terminalFailureCause)
	}

	return err
}

func (e *executableImpl) matchDLQErrorPattern(err error) error {
	if len(e.dlqErrorPattern()) <= 0 {
		return nil
	}
	match, mErr := regexp.MatchString(e.dlqErrorPattern(), err.Error())
	if mErr != nil {
		e.logger.Error(fmt.Sprintf("Failed to match task processing error with %s", dynamicconfig.HistoryTaskDLQErrorPattern.Key()))
		return nil
	}
	if !match {
		return nil
	}

	e.logger.Error(
		fmt.Sprintf("Error matches with %s. Marking task as terminally failed, will send to DLQ",
			dynamicconfig.HistoryTaskDLQErrorPattern.Key()),
		tag.Error(err),
		tag.ErrorType(err))
	e.terminalFailureCause = err
	metrics.TaskTerminalFailures.With(e.metricsHandler).Record(1)
	return fmt.Errorf("%w: %v", ErrTerminalTaskFailure, err)
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

	if e.invalidTask {
		// do not emit metrics for invalid tasks
		// as they are expected to have to high latency due to reprocessing upon shard movement.
		return
	}

	metrics.TaskAttempt.With(e.metricsHandler).Record(int64(e.attempt))

	priorityTaggedProvider := e.metricsHandler.WithTags(metrics.TaskPriorityTag(e.priority.String()))
	metrics.TaskLatency.With(priorityTaggedProvider).Record(e.inMemoryNoUserLatency)
	metrics.TaskQueueLatency.With(priorityTaggedProvider.WithTags(metrics.QueueReaderIDTag(e.readerID))).
		Record(time.Since(e.GetVisibilityTime()))
}

func (e *executableImpl) Nack(err error) {
	state := e.State()
	if state != ctasks.TaskStatePending {
		return
	}

	submitted := false
	if e.shouldResubmitOnNack(err) {
		// we do not need to know if there any error during submission
		// as long as it's not submitted, the execuable should be add
		// to the rescheduler
		e.SetScheduledTime(e.timeSource.Now())
		submitted = e.scheduler.TrySubmit(e)
	}

	if !submitted {
		backoffDuration := e.backoffDuration(err)
		// If err is due to user error, do not take any latency related to this attempt into account
		if !e.isUserError(err) {
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

	e.rescheduler.Add(e, e.timeSource.Now().Add(e.backoffDuration(nil)))
}

func (e *executableImpl) State() ctasks.State {
	e.Lock()
	defer e.Unlock()

	return e.state
}

func (e *executableImpl) GetPriority() ctasks.Priority {
	return e.priority
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
func (e *executableImpl) StateMachineTaskType() string {
	if t, ok := e.Task.(tasks.HasStateMachineTaskType); ok {
		return t.StateMachineTaskType()
	}
	return ""
}

func (e *executableImpl) shouldResubmitOnNack(err error) bool {
	// this is an optimization for skipping rescheduler and retry the task sooner.
	// this is useful for errors like workflow busy, which doesn't have to wait for
	// the longer rescheduling backoff.
	if e.attempt > resubmitMaxAttempts {
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
) time.Duration {
	// elapsedTime, the first parameter in ComputeNextDelay is not relevant here
	// since reschedule policy has no expiration interval.

	if err == consts.ErrTaskRetry ||
		err == consts.ErrNamespaceHandover ||
		common.IsInternalError(err) {
		// using a different reschedule policy to slow down retry
		// as immediate retry typically won't resolve the issue.
		return taskNotReadyReschedulePolicy.ComputeNextDelay(0, e.attempt, err)
	}

	if err == consts.ErrDependencyTaskNotCompleted {
		return dependencyTaskNotCompletedReschedulePolicy.ComputeNextDelay(0, e.attempt, err)
	}

	backoffDuration := reschedulePolicy.ComputeNextDelay(0, e.attempt, err)
	if !errors.Is(err, consts.ErrResourceExhaustedBusyWorkflow) && common.IsResourceExhausted(err) {
		// try a different reschedule policy to slow down retry
		// upon system resource exhausted error and pick the longer backoff duration
		backoffDuration = max(
			backoffDuration,
			taskResourceExhuastedReschedulePolicy.ComputeNextDelay(0, e.resourceExhaustedCount, err),
		)
	}

	return backoffDuration
}

func (e *executableImpl) incAttempt() {
	e.attempt++

	if e.attempt > taskCriticalLogMetricAttempts {
		metrics.TaskAttempt.With(e.metricsHandler).Record(int64(e.attempt))
	}
}

func estimateTaskMetricTags(
	task tasks.Task,
	namespaceRegistry namespace.Registry,
	currentClusterName string,
	chasmRegistry *chasm.Registry,
	taskTypeTagProvider TaskTypeTagProvider,
) []metrics.Tag {
	namespaceTag := metrics.NamespaceUnknownTag()
	isActive := true

	ns, err := namespaceRegistry.GetNamespaceByID(namespace.ID(task.GetNamespaceID()))
	if err == nil {
		namespaceTag = metrics.NamespaceTag(ns.Name().String())
		isActive = ns.ActiveInCluster(currentClusterName)
	}

	taskType := taskTypeTagProvider(task, isActive, chasmRegistry)
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

	metricsHandler metrics.Handler
}

func NewCircuitBreakerExecutable(
	e Executable,
	cb circuitbreaker.TwoStepCircuitBreaker,
	metricsHandler metrics.Handler,
) *CircuitBreakerExecutable {
	return &CircuitBreakerExecutable{
		Executable: e,
		cb:         cb,

		metricsHandler: metricsHandler,
	}
}

// This is roughly the same implementation of the `gobreaker.CircuitBreaker.Execute` function,
// but checks if the error is `DestinationDownError` to report success, and unwrap it.
func (e *CircuitBreakerExecutable) Execute() error {
	doneCb, err := e.cb.Allow()
	if err != nil {
		metrics.CircuitBreakerExecutableBlocked.With(e.metricsHandler).Record(1)
		// Return a resource exhausted error to ensure that this task is retried less aggressively
		// and does not go to the DLQ.
		return fmt.Errorf(
			"%w: %w",
			serviceerror.NewResourceExhausted(
				enumspb.RESOURCE_EXHAUSTED_CAUSE_CIRCUIT_BREAKER_OPEN,
				"circuit breaker rejection",
			),
			err,
		)
	}

	defer func() {
		e := recover()
		if e != nil {
			doneCb(false)
			panic(e)
		}
	}()

	err = e.Executable.Execute()
	var destinationDownErr *queueserrors.DestinationDownError
	if errors.As(err, &destinationDownErr) {
		err = destinationDownErr.Unwrap()
	}

	doneCb(destinationDownErr == nil)
	return err
}
