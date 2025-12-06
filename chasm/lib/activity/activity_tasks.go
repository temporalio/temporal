package activity

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/resource"
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
	attempt, err := activity.LastAttempt.Get(ctx)
	if err != nil {
		return false, err
	}

	// TODO make sure we handle resets when we support them, as they will reset the attempt count
	if !TransitionStarted.Possible(activity) || task.Attempt != attempt.Count {
		return false, nil
	}

	return true, nil
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
	attempt, err := activity.LastAttempt.Get(ctx)
	if err != nil {
		return false, err
	}

	valid := activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED && task.Attempt == attempt.Count
	return valid, nil
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
	attempt, err := activity.LastAttempt.Get(ctx)
	if err != nil {
		return false, err
	}

	valid := activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_STARTED && task.Attempt == attempt.Count
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
