package activity

import (
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/resource"
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
	attempt, err := activity.Attempt.Get(ctx)
	if err != nil {
		return false, err
	}

	// TODO make sure we handle resets when we support them, as they will reset the attempt count
	if activity.Status != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED || task.Attempt != attempt.Count {
		return false, nil
	}

	return true, nil
}

func (e *scheduleToStartTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToStartTimeoutTask,
) error {
	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START})
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

	// TODO make sure we handle resets when we support them, as they will reset the attempt count
	if !TransitionTimedOut.Possible(activity) || task.Attempt != attempt.Count {
		return false, nil
	}

	return true, nil
}

func (e *scheduleToCloseTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToCloseTimeoutTask,
) error {
	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE})
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

	// TODO make sure we handle resets when we support them, as they will reset the attempt count
	if activity.Status != activitypb.ACTIVITY_EXECUTION_STATUS_STARTED || task.Attempt != attempt.Count {
		fmt.Println("Validating StartToClose timeout task for activity FALSE:", activity, "attempt:", task.Attempt, "time: ", ctx.Now(activity))

		return false, nil
	}

	fmt.Println("Validating StartToClose timeout task for activity TRUE:", activity, "attempt:", task.Attempt, "time: ", ctx.Now(activity))

	return true, nil
}

func (e *startToCloseTimeoutTaskExecutor) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	task *activitypb.StartToCloseTimeoutTask,
) error {
	retryPolicy := activity.RetryPolicy
	attempt, err := activity.Attempt.Get(ctx)
	if err != nil {
		return err
	}

	interval := calculateRetryInterval(retryPolicy, attempt.Count)
	isNegativeInterval := retryPolicy.GetMaximumInterval().AsDuration() == 0 && interval <= 0 // From existing workflow activity retry behavior

	// Retry task if we have remaining attempts
	if (retryPolicy.GetMaximumAttempts() == 0 || task.GetAttempt() < retryPolicy.GetMaximumAttempts()) && (!isNegativeInterval) {
		attempt.LastAttemptCompleteTime = timestamppb.New(ctx.Now(activity))
		attempt.Count += 1

		return TransitionScheduled.Apply(activity, ctx, scheduledEvent{TaskStartDelay: interval})
	}

	// Reached maximum attempts, timeout the activity
	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE})
}

func calculateRetryInterval(retryPolicy *commonpb.RetryPolicy, attempt int32) time.Duration {
	intervalCalculator := backoff.MakeBackoffAlgorithm(nil)
	interval := intervalCalculator(retryPolicy.GetInitialInterval(), retryPolicy.GetBackoffCoefficient(), attempt)

	maxInterval := retryPolicy.GetMaximumInterval()

	// Cap interval to maximum if it's set
	if maxInterval.AsDuration() != 0 && (interval <= 0 || interval > maxInterval.AsDuration()) {
		interval = maxInterval.AsDuration()
	}

	return interval
}
