package tests

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNexusOperationStartToCloseTimeoutAfterDeferredCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	env := newNexusTestEnv(t, true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, false),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, false),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSignalBacklinks, false),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, false),
		testcore.WithDynamicConfig(chasmnexus.ChasmWorkflowOperationsRolloutPercent, 0),
	)
	taskQueue := testcore.RandomizeStr(t.Name())

	startResponse := make(chan struct{})
	cancelSent := make(chan struct{}, 1)
	handler := nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case <-startResponse:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "operation-token"}, nil
		},
		OnCancelOperation: func(context.Context, string, string, string, nexus.CancelOperationOptions) error {
			select {
			case cancelSent <- struct{}{}:
			default:
			}
			return nil
		},
	}
	endpoint := env.createRandomExternalNexusServer(ctx, t, handler)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 30 * time.Second,
	}, "workflow")
	require.NoError(t, err)
	defer func() {
		_ = env.SdkClient().TerminateWorkflow(testcore.NewContext(), run.GetID(), run.GetRunID(), "test cleanup")
	}()

	firstTask := pollDeferredCancellationWorkflowTask(ctx, t, env, taskQueue)
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test-worker",
		TaskToken: firstTask.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
						Endpoint:            endpoint,
						Service:             "service",
						Operation:           "operation",
						Input:               testcore.MustToPayload(t, "input"),
						StartToCloseTimeout: durationpb.New(4 * time.Second),
					},
				},
			},
			{
				CommandType: enumspb.COMMAND_TYPE_START_TIMER,
				Attributes: &commandpb.Command_StartTimerCommandAttributes{
					StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
						TimerId:            "schedule-cancellation-task",
						StartToFireTimeout: durationpb.New(10 * time.Millisecond),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	secondTask := pollDeferredCancellationWorkflowTask(ctx, t, env, taskQueue)
	scheduledEventID := findNexusEventID(secondTask.History.Events, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED)
	require.Positive(t, scheduledEventID)
	require.Zero(t, findNexusEventID(secondTask.History.Events, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED))

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Identity:  "test-worker",
		TaskToken: secondTask.TaskToken,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
				Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
					RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
						ScheduledEventId: scheduledEventID,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	await.Require(ctx, t, func(t *await.T) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		require.NoError(t, err)
		require.Len(t, desc.PendingNexusOperations, 1)
		require.NotNil(t, desc.PendingNexusOperations[0].CancellationInfo)
	}, 10*time.Second, 50*time.Millisecond)

	close(startResponse)
	select {
	case <-cancelSent:
	case <-time.After(10 * time.Second):
		t.Fatal("cancel request was not sent after the operation started")
	}

	await.Require(ctx, t, func(t *await.T) {
		history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: run.GetID(),
			RunId:      run.GetRunID(),
		})
		timedOutEventID := findNexusEventID(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT)
		require.Positive(t, timedOutEventID)
	}, 10*time.Second, 50*time.Millisecond)
}

func pollDeferredCancellationWorkflowTask(
	ctx context.Context,
	t *testing.T,
	env *NexusTestEnv,
	taskQueue string,
) *workflowservice.PollWorkflowTaskQueueResponse {
	t.Helper()
	resp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: taskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		Identity: "test-worker",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.TaskToken)
	return resp
}

func findNexusEventID(events []*historypb.HistoryEvent, eventType enumspb.EventType) int64 {
	for _, event := range events {
		if event.GetEventType() == eventType {
			return event.GetEventId()
		}
	}
	return 0
}
