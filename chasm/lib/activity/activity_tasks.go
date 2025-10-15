package activity

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
)

type ActivityStartTaskExecutorOptions struct {
	fx.In
	// TODO add configs

	MatchingClient resource.MatchingClient
}

// ActivityStartTaskExecutor handles starting activity by adding a new activity task to the matching service which will
// eventually get picked up by the worker.
type ActivityStartTaskExecutor struct {
	MatchingClient resource.MatchingClient
}

func newActivityStartTaskExecutor(opts ActivityStartTaskExecutorOptions) *ActivityStartTaskExecutor {
	return &ActivityStartTaskExecutor{
		MatchingClient: opts.MatchingClient,
	}
}

func (e *ActivityStartTaskExecutor) Validate(
	_ chasm.Context,
	_ *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityStartExecuteTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("ActivityStartTaskExecutor is not yet supported")
}

func (e *ActivityStartTaskExecutor) Execute(
	_ context.Context,
	_ chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityStartExecuteTask,
) error {
	return serviceerror.NewUnimplemented("ActivityStartTaskExecutor is not yet supported")
}

// ScheduleToCloseTimeoutTaskExecutor handles detecting any violations of activity schedule to close timeouts.
type ScheduleToCloseTimeoutTaskExecutor struct {
}

func newScheduleToCloseTimeoutTaskExecutor() *ScheduleToCloseTimeoutTaskExecutor {
	return &ScheduleToCloseTimeoutTaskExecutor{}
}

func (e *ScheduleToCloseTimeoutTaskExecutor) Validate(
	_ chasm.Context,
	_ *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityStartExecuteTask,
) (bool, error) {
	return false, serviceerror.NewUnimplemented("ScheduleToCloseTimeoutTaskExecutor is not yet supported")
}

func (e *ScheduleToCloseTimeoutTaskExecutor) Execute(
	_ chasm.MutableContext,
	_ *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityStartExecuteTask,
) error {
	return serviceerror.NewUnimplemented("ScheduleToCloseTimeoutTaskExecutor is not yet supported")
}
