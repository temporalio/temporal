package activity

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
)

type ActivityStartTaskExecutorOptions struct {
	fx.In

	MatchingClient resource.MatchingClient
}

type ActivityStartTaskExecutor struct {
	MatchingClient resource.MatchingClient
}

func newActivityStartTaskExecutor(opts ActivityStartTaskExecutorOptions) *ActivityStartTaskExecutor {
	return &ActivityStartTaskExecutor{
		MatchingClient: opts.MatchingClient,
	}
}

func (e *ActivityStartTaskExecutor) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskAttributes,
	task *activitypb.ActivityStartExecuteTask,
) (bool, error) {
	attempt, err := activity.Attempt.Get(ctx)
	if err != nil {
		return false, err
	}

	// TODO make sure we handle resets when we support them, as they will reset the attempt count
	if activity.State() != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED || task.Attempt != attempt.Count {
		return false, nil
	}

	return true, nil
}

func (e *ActivityStartTaskExecutor) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityStartExecuteTask,
) error {
	activityState, err := GetActivityState(ctx, activityRef)
	if err != nil {
		return err
	}

	protoRef, err := chasm.ComponentRefToProtoRef(activityRef, nil)
	if err != nil {
		return err
	}

	// Note: No need to set the vector clock here, as the components track version conflicts for read/write
	request := &matchingservice.AddActivityTaskRequest{
		NamespaceId:            activityRef.NamespaceID,
		TaskQueue:              activityState.ActivityOptions.TaskQueue,
		ScheduleToStartTimeout: activityState.ActivityOptions.ScheduleToStartTimeout,
		Priority:               activityState.Priority,
		ComponentRef:           protoRef,
	}

	_, err = e.MatchingClient.AddActivityTask(ctx, request)

	return err
}
