package activity

import (
	"context"
	"fmt"

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
	attempt, err := activity.Attempt.Get(ctx)
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
	fmt.Printf("CHASM Activity ScheduleToStartTimeout.Validate: Called\n")

	attempt, err := activity.Attempt.Get(ctx)
	if err != nil {
		fmt.Printf("CHASM Activity ScheduleToStartTimeout.Validate: Error getting attempt: %v\n", err)
		return false, err
	}

	valid := activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED && task.Attempt == attempt.Count
	fmt.Printf("CHASM Activity ScheduleToStartTimeout.Validate: Status=%v (expect %v), task.Attempt=%d, attempt.Count=%d, valid=%v\n",
		activity.Status, activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, task.Attempt, attempt.Count, valid)

	return valid, nil
}

func (e *scheduleToStartTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToStartTimeoutTask,
) error {
	fmt.Printf("CHASM Activity ScheduleToStartTimeout.Execute: Called - applying timeout\n")
	return TransitionTimedOut.Apply(activity, ctx, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START)
}

type scheduleToCloseTimeoutTaskExecutor struct{}

func newScheduleToCloseTimeoutTaskExecutor() *scheduleToCloseTimeoutTaskExecutor {
	return &scheduleToCloseTimeoutTaskExecutor{}
}

func (e *scheduleToCloseTimeoutTaskExecutor) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskAttributes,
	task *activitypb.ScheduleToCloseTimeoutTask,
) (bool, error) {
	attempt, err := activity.Attempt.Get(ctx)
	if err != nil {
		return false, err
	}

	valid := TransitionTimedOut.Possible(activity) && task.Attempt == attempt.Count
	return valid, nil
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
	attempt, err := activity.Attempt.Get(ctx)
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
	task *activitypb.StartToCloseTimeoutTask,
) error {
	retryPolicy := activity.RetryPolicy

	// Only retry if MaximumAttempts is explicitly set to > 1 (0 means no retries for start-to-close timeouts)
	// Note: MaximumAttempts == 0 should mean no retries, not unlimited retries for timeouts
	enoughAttempts := retryPolicy.GetMaximumAttempts() > 1 && task.GetAttempt() < retryPolicy.GetMaximumAttempts()
	enoughTime, err := activity.hasEnoughTimeForRetry(ctx)
	if err != nil {
		return err
	}

	// Retry task if we have remaining attempts and time. A retry involves transitioning the activity back to scheduled state.
	if enoughAttempts && enoughTime {
		return TransitionRescheduled.Apply(activity, ctx, nil)
	}

	// Reached maximum attempts, timeout the activity
	return TransitionTimedOut.Apply(activity, ctx, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
}
