package activity

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/vclock"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
)

type ActivityStartTaskExecutorOptions struct {
	fx.In
	// TODO add configs

	MatchingClient resource.MatchingClient
}

type ActivityStartTaskExecutor struct {
	//ActivityStartTaskExecutorOptions
	MatchingClient resource.MatchingClient
}

func newActivityStartTaskExecutor(opts ActivityStartTaskExecutorOptions) *ActivityStartTaskExecutor {
	return &ActivityStartTaskExecutor{
		//ActivityStartTaskExecutorOptions: opts,
		MatchingClient: opts.MatchingClient,
	}
}

func (e *ActivityStartTaskExecutor) Validate(
	_ chasm.Context,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityStartExecuteTask,
) (bool, error) {
	return true, nil
}

func (e *ActivityStartTaskExecutor) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityStartExecuteTask,
) error {
	request := &matchingservice.AddActivityTaskRequest{
		NamespaceId: activityRef.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: activityRef.BusinessID,
			RunId:      "my-run-id",
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: "my-task-queue",
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		ScheduledEventId:       11,
		ScheduleToStartTimeout: durationpb.New(0),
		Clock:                  vclock.NewVectorClock(1, 1, 1),
	}
	r, err := e.MatchingClient.AddActivityTask(ctx, request)
	if err != nil {
		return err
	}

	fmt.Println(r)

	return nil
}
