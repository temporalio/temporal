package activity

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/util"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	// TODO make sure we handle resets when we support them, as they will reset the attempt count
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

type scheduleToStartTimeoutTaskExecutor struct{}

func newScheduleToStartTimeoutTaskExecutor() *scheduleToStartTimeoutTaskExecutor {
	return &scheduleToStartTimeoutTaskExecutor{}
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
	return TransitionTimedOut.Apply(activity, ctx, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START)
}

type scheduleToCloseTimeoutTaskExecutor struct{}

func newScheduleToCloseTimeoutTaskExecutor() *scheduleToCloseTimeoutTaskExecutor {
	return &scheduleToCloseTimeoutTaskExecutor{}
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
	return TransitionTimedOut.Apply(activity, ctx, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
}

type startToCloseTimeoutTaskExecutor struct{}

func newStartToCloseTimeoutTaskExecutor() *startToCloseTimeoutTaskExecutor {
	return &startToCloseTimeoutTaskExecutor{}
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

func (e *startToCloseTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.StartToCloseTimeoutTask,
) error {
	shouldRetry, retryInterval, err := activity.shouldRetry(ctx, 0)
	if err != nil {
		return err
	}

	// Retry task if we have remaining attempts and time. A retry involves transitioning the activity back to scheduled state.
	if shouldRetry {
		return TransitionRescheduled.Apply(activity, ctx, rescheduleEvent{
			retryInterval: retryInterval,
			failure:       createStartToCloseTimeoutFailure(),
		})
	}

	// Reached maximum attempts, timeout the activity
	return TransitionTimedOut.Apply(activity, ctx, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
}

// HeartbeatTimeoutTask is a pure task that enforces heartbeat timeouts.
type heartbeatTimeoutTaskExecutor struct{}

func newHeartbeatTimeoutTaskExecutor() *heartbeatTimeoutTaskExecutor {
	return &heartbeatTimeoutTaskExecutor{}
}

// Validate validates a HeartbeatTimeoutTask.
func (e *heartbeatTimeoutTaskExecutor) Validate(
	ctx chasm.Context,
	activity *Activity,
	taskAttrs chasm.TaskAttributes,
	task *activitypb.HeartbeatTimeoutTask,
) (bool, error) {
	validStatus := activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_STARTED ||
		activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED
	if !validStatus || activity.LastAttempt.Get(ctx).GetCount() != task.Attempt {
		return false, nil
	}
	// High-water-mark: reject tasks that have already been executed.
	hwm := activity.GetLastHeartbeatTaskScheduledTime().AsTime()
	return taskAttrs.ScheduledTime.After(hwm), nil
}

// Execute executes a HeartbeatTimeoutTask.
func (e *heartbeatTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	taskAttrs chasm.TaskAttributes,
	_ *activitypb.HeartbeatTimeoutTask,
) error {
	// There are two concurrent processes:
	// 1. A worker is sending heartbeats.
	// 2. This task is being executed at (shortly after) certain scheduled times.

	// Each time we execute this function, our task is to look back into the past and determine
	// whether more than (user-configured heartbeat timeout) has elapsed since the last heartbeat.
	// If it has, we fail the attempt (and decide between retrying or failing the activity). If it
	// has not, then we schedule a new timer task to execute this function at the new deadline.

	// Update high-water-mark so this task is invalidated after execution.
	activity.LastHeartbeatTaskScheduledTime = timestamppb.New(taskAttrs.ScheduledTime)

	attempt := activity.LastAttempt.Get(ctx)
	hbTimeout := activity.GetHeartbeatTimeout().AsDuration()
	attemptStartTime := attempt.GetStartedTime().AsTime()
	heartbeat, _ := activity.LastHeartbeat.TryGet(ctx)
	lastHbTime := heartbeat.GetRecordedTime().AsTime() // could be from a previous attempt or could be zero
	// No heartbeats in the attempt so far is equivalent to a heartbeat having been sent at attempt
	// start time.
	hbDeadline := util.MaxTime(lastHbTime, attemptStartTime).Add(hbTimeout)

	if ctx.Now(activity).Before(hbDeadline) {
		// Deadline has not expired; schedule a new task.
		ctx.AddTask(
			activity,
			chasm.TaskAttributes{
				ScheduledTime: hbDeadline,
			},
			&activitypb.HeartbeatTimeoutTask{
				Attempt: attempt.GetCount(),
			},
		)
		return nil
	}

	// Fail this attempt due to heartbeat timeout.
	shouldRetry, retryInterval, err := activity.shouldRetry(ctx, 0)
	if err != nil {
		return err
	}
	if shouldRetry {
		return TransitionRescheduled.Apply(activity, ctx, rescheduleEvent{
			retryInterval: retryInterval,
			failure:       createHeartbeatTimeoutFailure(),
		})
	}
	return TransitionTimedOut.Apply(activity, ctx, enumspb.TIMEOUT_TYPE_HEARTBEAT)
}
