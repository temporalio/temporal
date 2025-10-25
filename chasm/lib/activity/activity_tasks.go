package activity

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
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
	if activity.State() != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED || task.Attempt != attempt.Count {
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
		func(a *Activity, ctx chasm.Context, activityRef chasm.ComponentRef) (*matchingservice.AddActivityTaskRequest, error) {

			protoRef, err := chasm.ComponentRefToProtoRef(activityRef, nil)
			if err != nil {
				return nil, err
			}

			// Note: No need to set the vector clock here, as the components track version conflicts for read/write
			return &matchingservice.AddActivityTaskRequest{
				NamespaceId:            activityRef.NamespaceID,
				TaskQueue:              a.ActivityOptions.TaskQueue,
				ScheduleToStartTimeout: a.ActivityOptions.ScheduleToStartTimeout,
				Priority:               a.Priority,
				ComponentRef:           protoRef,
			}, nil
		},
		activityRef,
	)
	if err != nil {
		return err
	}

	_, err = e.opts.MatchingClient.AddActivityTask(ctx, request)

	return err
}
