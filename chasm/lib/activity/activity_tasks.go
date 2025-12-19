package activity

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/util"
	"go.uber.org/fx"
)

type activityDispatchTaskExecutorOptions struct {
	fx.In

	MatchingClient resource.MatchingClient
}

type activityDispatchTaskExecutor struct {
	opts activityDispatchTaskExecutorOptions
}

func newActivityDispatchTaskExecutor(opts activityDispatchTaskExecutorOptions) *activityDispatchTaskExecutor {
	return &activityDispatchTaskExecutor{
		opts,
	}
}

func (e *activityDispatchTaskExecutor) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskAttributes,
	task *activitypb.ActivityDispatchTask,
) (bool, error) {
	// TODO(saa-preview): make sure we handle resets when we support them, as they will reset the attempt count
	return (TransitionStarted.Possible(activity) &&
		task.Attempt == activity.LastAttempt.Get(ctx).GetCount()), nil
}

func (e *activityDispatchTaskExecutor) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityDispatchTask,
) error {
	request, err := chasm.ReadComponent(
		ctx,
		activityRef,
		(*Activity).createAddActivityTaskRequest,
		activityRef.NamespaceID,
	)
	if err != nil {
		return err
	}

	_, err = e.opts.MatchingClient.AddActivityTask(ctx, request)

	return err
}

type timeoutTaskExecutorOptions struct {
	fx.In

	Config            *Config
	MetricsHandler    metrics.Handler
	NamespaceRegistry namespace.Registry
}

type scheduleToStartTimeoutTaskExecutor struct {
	opts timeoutTaskExecutorOptions
}

func newScheduleToStartTimeoutTaskExecutor(opts timeoutTaskExecutorOptions) *scheduleToStartTimeoutTaskExecutor {
	return &scheduleToStartTimeoutTaskExecutor{
		opts,
	}
}

func (e *scheduleToStartTimeoutTaskExecutor) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskAttributes,
	task *activitypb.ScheduleToStartTimeoutTask,
) (bool, error) {
	return (activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED &&
		task.Attempt == activity.LastAttempt.Get(ctx).GetCount()), nil
}

func (e *scheduleToStartTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToStartTimeoutTask,
) error {
	nsID := namespace.ID(ctx.ExecutionKey().NamespaceID)
	namespaceName, err := e.opts.NamespaceRegistry.GetNamespaceName(nsID)
	if err != nil {
		return err
	}

	metricsHandler := enrichMetricsHandler(
		activity,
		e.opts.MetricsHandler,
		namespaceName.String(),
		metrics.TimerActiveTaskActivityTimeoutScope,
		e.opts.Config.BreakdownMetricsByTaskQueue)

	event := timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
	}

	return TransitionTimedOut.Apply(activity, ctx, event)
}

type scheduleToCloseTimeoutTaskExecutor struct {
	opts timeoutTaskExecutorOptions
}

func newScheduleToCloseTimeoutTaskExecutor(opts timeoutTaskExecutorOptions) *scheduleToCloseTimeoutTaskExecutor {
	return &scheduleToCloseTimeoutTaskExecutor{
		opts,
	}
}

func (e *scheduleToCloseTimeoutTaskExecutor) Validate(
	_ chasm.Context,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToCloseTimeoutTask,
) (bool, error) {
	return TransitionTimedOut.Possible(activity), nil
}

func (e *scheduleToCloseTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToCloseTimeoutTask,
) error {
	nsID := namespace.ID(ctx.ExecutionKey().NamespaceID)
	namespaceName, err := e.opts.NamespaceRegistry.GetNamespaceName(nsID)
	if err != nil {
		return err
	}

	metricsHandler := enrichMetricsHandler(
		activity,
		e.opts.MetricsHandler,
		namespaceName.String(),
		metrics.TimerActiveTaskActivityTimeoutScope,
		e.opts.Config.BreakdownMetricsByTaskQueue)

	event := timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
	}

	return TransitionTimedOut.Apply(activity, ctx, event)
}

type startToCloseTimeoutTaskExecutor struct {
	opts timeoutTaskExecutorOptions
}

func newStartToCloseTimeoutTaskExecutor(opts timeoutTaskExecutorOptions) *startToCloseTimeoutTaskExecutor {
	return &startToCloseTimeoutTaskExecutor{
		opts,
	}
}

func (e *startToCloseTimeoutTaskExecutor) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskAttributes,
	task *activitypb.StartToCloseTimeoutTask,
) (bool, error) {
	valid := (activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_STARTED &&
		task.Attempt == activity.LastAttempt.Get(ctx).GetCount())
	return valid, nil
}

// Execute executes a StartToCloseTimeoutTask. It fails the attempt, leading to retry or activity
// failure.
func (e *startToCloseTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.StartToCloseTimeoutTask,
) error {
	rescheduled, err := activity.tryReschedule(ctx, 0, createStartToCloseTimeoutFailure())
	if err != nil {
		return err
	}

	nsID := namespace.ID(ctx.ExecutionKey().NamespaceID)
	namespaceName, err := e.opts.NamespaceRegistry.GetNamespaceName(nsID)
	if err != nil {
		return err
	}

	metricsHandler := enrichMetricsHandler(
		activity,
		e.opts.MetricsHandler,
		namespaceName.String(),
		metrics.TimerActiveTaskActivityTimeoutScope,
		e.opts.Config.BreakdownMetricsByTaskQueue)

	if rescheduled {
		activity.emitOnAttemptTimedOutMetrics(ctx, metricsHandler, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)

		return nil
	}

	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
	})
}

// HeartbeatTimeoutTask is a pure task that enforces heartbeat timeouts.
type heartbeatTimeoutTaskExecutor struct {
	opts timeoutTaskExecutorOptions
}

func newHeartbeatTimeoutTaskExecutor(opts timeoutTaskExecutorOptions) *heartbeatTimeoutTaskExecutor {
	return &heartbeatTimeoutTaskExecutor{
		opts,
	}
}

// Validate validates a HeartbeatTimeoutTask.
func (e *heartbeatTimeoutTaskExecutor) Validate(
	ctx chasm.Context,
	activity *Activity,
	taskAttrs chasm.TaskAttributes,
	task *activitypb.HeartbeatTimeoutTask,
) (bool, error) {
	// Let T = user-configured heartbeat timeout and let hb_i be the time of the ith user-submitted
	// heartbeat request. (hb_0 = 0 since we always start a timer task when an attempt starts).

	// There are two concurrent sequences of events:
	// 1. A worker is sending heartbeats at times hb_i.
	// 2. This task is being executed at (shortly after) times hb_i + T.

	// On the i-th execution of this function, we look back into the past and determine whether the
	// last heartbeat was received after hb_i. If so, we reject this timeout task. Otherwise, the
	// Execute function runs and we fail the attempt.
	if activity.Status != activitypb.ACTIVITY_EXECUTION_STATUS_STARTED &&
		activity.Status != activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		return false, nil
	}
	// Task attempt must still match current attempt.
	attempt := activity.LastAttempt.Get(ctx)
	if attempt.GetCount() != task.Attempt {
		return false, nil
	}

	// Must not have been a heartbeat since this task was created
	hbTimeout := activity.GetHeartbeatTimeout().AsDuration() // T
	attemptStartTime := attempt.GetStartedTime().AsTime()
	lastHb, _ := activity.LastHeartbeat.TryGet(ctx) // could be nil, or from a previous attempt
	// No hbs in attempt so far is equivalent to hb having been sent at attempt start time.
	lastHbTime := util.MaxTime(lastHb.GetRecordedTime().AsTime(), attemptStartTime)
	thisTaskHbTime := taskAttrs.ScheduledTime.Add(-hbTimeout) // hb_i
	if lastHbTime.After(thisTaskHbTime) {
		// another heartbeat has invalidated this task's heartbeat
		return false, nil
	}
	return true, nil
}

// Execute executes a HeartbeatTimeoutTask. It fails the attempt, leading to retry or activity
// failure.
func (e *heartbeatTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.HeartbeatTimeoutTask,
) error {
	rescheduled, err := activity.tryReschedule(ctx, 0, createHeartbeatTimeoutFailure())
	if err != nil {
		return err
	}

	nsID := namespace.ID(ctx.ExecutionKey().NamespaceID)
	namespaceName, err := e.opts.NamespaceRegistry.GetNamespaceName(nsID)
	if err != nil {
		return err
	}

	metricsHandler := enrichMetricsHandler(
		activity,
		e.opts.MetricsHandler,
		namespaceName.String(),
		metrics.TimerActiveTaskActivityTimeoutScope,
		e.opts.Config.BreakdownMetricsByTaskQueue)

	if rescheduled {
		activity.emitOnAttemptTimedOutMetrics(ctx, metricsHandler, enumspb.TIMEOUT_TYPE_HEARTBEAT)
		return nil
	}

	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
	})
}
