package temporaltest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/temporaltest"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestLocalExecutionStateGatesTaskCompletions(t *testing.T) {
	server := temporaltest.NewServer(temporaltest.WithT(t))
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	const taskQueue = "local-execution-state-task"

	run, err := server.GetDefaultClient().ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{ID: "local-execution-state-task", TaskQueue: taskQueue},
		"unregistered-workflow",
	)
	require.NoError(t, err)
	execution := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	task := pollWorkflowTask(ctx, t, server, taskQueue)
	adminClient, _ := localFirstAdminClient(ctx, t, server)
	historyBeforePause := localFirstHistory(ctx, t, server, execution)
	updateLocalExecutionState(
		ctx,
		t,
		adminClient,
		server.GetDefaultNamespace(),
		execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_PAUSED,
	)
	historyAtPause := localFirstHistory(ctx, t, server, execution)
	require.Equal(t, historyBeforePause, historyAtPause)

	_, err = server.GetDefaultClient().WorkflowService().RespondWorkflowTaskCompleted(
		ctx,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace:                  server.GetDefaultNamespace(),
			TaskToken:                  task.GetTaskToken(),
			ForceCreateNewWorkflowTask: true,
		},
	)
	require.ErrorAs(t, err, new(*serviceerror.Unavailable))
	require.Equal(t, historyAtPause, localFirstHistory(ctx, t, server, execution))

	updateLocalExecutionState(
		ctx,
		t,
		adminClient,
		server.GetDefaultNamespace(),
		execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_RUNNABLE,
	)
	_, err = server.GetDefaultClient().WorkflowService().RespondWorkflowTaskCompleted(
		ctx,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace:                  server.GetDefaultNamespace(),
			TaskToken:                  task.GetTaskToken(),
			ForceCreateNewWorkflowTask: true,
		},
	)
	require.NoError(t, err)

	nextTask := pollWorkflowTask(ctx, t, server, taskQueue)
	historyBeforeLoss := localFirstHistory(ctx, t, server, execution)
	updateLocalExecutionState(
		ctx,
		t,
		adminClient,
		server.GetDefaultNamespace(),
		execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_OWNERSHIP_LOST,
	)
	_, err = server.GetDefaultClient().WorkflowService().RespondWorkflowTaskCompleted(
		ctx,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskToken: nextTask.GetTaskToken(),
		},
	)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	require.Equal(t, historyBeforeLoss, localFirstHistory(ctx, t, server, execution))
}

func TestLocalExecutionStateStopsTimerAdvancement(t *testing.T) {
	server := temporaltest.NewServer(temporaltest.WithT(t))
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	const taskQueue = "local-execution-state-timer"

	run, err := server.GetDefaultClient().ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{ID: "local-execution-state-timer", TaskQueue: taskQueue},
		"unregistered-workflow",
	)
	require.NoError(t, err)
	execution := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	task := pollWorkflowTask(ctx, t, server, taskQueue)
	_, err = server.GetDefaultClient().WorkflowService().RespondWorkflowTaskCompleted(
		ctx,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskToken: task.GetTaskToken(),
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{
					StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
						TimerId:            "timer",
						StartToFireTimeout: durationpb.New(250 * time.Millisecond),
					},
				},
			}},
		},
	)
	require.NoError(t, err)
	adminClient, _ := localFirstAdminClient(ctx, t, server)
	updateLocalExecutionState(
		ctx,
		t,
		adminClient,
		server.GetDefaultNamespace(),
		execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_PAUSED,
	)

	require.Never(t, func() bool {
		return countEventType(
			localFirstHistory(ctx, t, server, execution),
			enumspb.EVENT_TYPE_TIMER_FIRED,
		) > 0
	}, time.Second, 20*time.Millisecond)

	updateLocalExecutionState(
		ctx,
		t,
		adminClient,
		server.GetDefaultNamespace(),
		execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_RUNNABLE,
	)
	await.RequireTrue(t, func() bool {
		return countEventType(
			localFirstHistory(ctx, t, server, execution),
			enumspb.EVENT_TYPE_TIMER_FIRED,
		) == 1
	}, 5*time.Second, 20*time.Millisecond)
}

func TestLocalExecutionStateInvalidatesActivityToken(t *testing.T) {
	server := temporaltest.NewServer(temporaltest.WithT(t))
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	const taskQueue = "local-execution-state-activity"

	run, err := server.GetDefaultClient().ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{ID: "local-execution-state-activity", TaskQueue: taskQueue},
		"unregistered-workflow",
	)
	require.NoError(t, err)
	execution := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	workflowTask := pollWorkflowTask(ctx, t, server, taskQueue)
	_, err = server.GetDefaultClient().WorkflowService().RespondWorkflowTaskCompleted(
		ctx,
		&workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskToken: workflowTask.GetTaskToken(),
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:          "activity",
						ActivityType:        &commonpb.ActivityType{Name: "activity"},
						TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
						StartToCloseTimeout: durationpb.New(time.Minute),
					},
				},
			}},
		},
	)
	require.NoError(t, err)
	activityTask, err := server.GetDefaultClient().WorkflowService().PollActivityTaskQueue(
		ctx,
		&workflowservice.PollActivityTaskQueueRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "local-execution-state-test",
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, activityTask.GetTaskToken())

	adminClient, _ := localFirstAdminClient(ctx, t, server)
	historyBeforeLoss := localFirstHistory(ctx, t, server, execution)
	updateLocalExecutionState(
		ctx,
		t,
		adminClient,
		server.GetDefaultNamespace(),
		execution,
		adminservice.UpdateLocalExecutionStateRequest_STATE_OWNERSHIP_LOST,
	)
	_, err = server.GetDefaultClient().WorkflowService().RespondActivityTaskCompleted(
		ctx,
		&workflowservice.RespondActivityTaskCompletedRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskToken: activityTask.GetTaskToken(),
		},
	)
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
	require.Equal(t, historyBeforeLoss, localFirstHistory(ctx, t, server, execution))
}

func pollWorkflowTask(
	ctx context.Context,
	t *testing.T,
	server *temporaltest.TestServer,
	taskQueue string,
) *workflowservice.PollWorkflowTaskQueueResponse {
	t.Helper()
	response, err := server.GetDefaultClient().WorkflowService().PollWorkflowTaskQueue(
		ctx,
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "local-execution-state-test",
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, response.GetTaskToken())
	return response
}

func updateLocalExecutionState(
	ctx context.Context,
	t *testing.T,
	adminClient adminservice.AdminServiceClient,
	namespace string,
	execution *commonpb.WorkflowExecution,
	state adminservice.UpdateLocalExecutionStateRequest_State,
) {
	t.Helper()
	_, err := adminClient.UpdateLocalExecutionState(ctx, &adminservice.UpdateLocalExecutionStateRequest{
		Namespace:     namespace,
		Execution:     execution,
		LocalServerId: "local-execution-state-test",
		FencingEpoch:  1,
		State:         state,
	})
	require.NoError(t, err)
}
