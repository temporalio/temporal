package temporaltest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/localexecution"
	"go.temporal.io/server/temporaltest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestLocalExecutionAcquisition(t *testing.T) {
	server := temporaltest.NewServer(
		temporaltest.WithT(t),
		temporaltest.WithDynamicConfig(dynamicconfig.EnableLocalExecution, true),
	)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	const taskQueue = "local-execution-acquisition"

	run, err := server.GetDefaultClient().ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{ID: "local-execution-acquisition", TaskQueue: taskQueue},
		"unregistered-workflow",
	)
	require.NoError(t, err)

	response, err := server.GetDefaultClient().WorkflowService().PollWorkflowTaskQueue(
		ctx,
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test-local-server",
			LocalExecutionOptions: &workflowservice.LocalExecutionPollOptions{
				LocalServerId:          "test-local-server",
				ProtocolVersion:        localexecution.ProtocolVersion,
				SyncInterval:           durationpb.New(time.Second),
				RequestedLeaseDuration: durationpb.New(3 * time.Second),
			},
		},
	)
	require.NoError(t, err)
	require.Empty(t, response.GetTaskToken())
	require.Equal(t, run.GetID(), response.GetWorkflowExecution().GetWorkflowId())
	require.Equal(t, run.GetRunID(), response.GetWorkflowExecution().GetRunId())
	require.Len(t, response.GetHistory().GetEvents(), 2)
	require.Zero(t, response.GetStartedEventId())

	ownership := response.GetLocalExecutionInfo()
	require.NotNil(t, ownership)
	require.Len(t, ownership.GetOwnershipToken(), localExecutionOwnershipTokenSizeForTest)
	require.Equal(t, int64(1), ownership.GetFencingEpoch())
	require.Equal(t, int64(2), ownership.GetLastSynchronizedEventId())

	mutableState, err := server.GetDefaultClient().WorkflowService().DescribeWorkflowExecution(
		ctx,
		&workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: server.GetDefaultNamespace(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()},
		},
	)
	require.NoError(t, err)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, mutableState.GetWorkflowExecutionInfo().GetStatus())

	adminClient, namespaceID := localFirstAdminClient(ctx, t, server)
	described, err := adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: server.GetDefaultNamespace(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()},
	})
	require.NoError(t, err)
	persisted := described.GetDatabaseMutableState().GetExecutionInfo().GetLocalExecutionInfo()
	require.Equal(t, persistencespb.LocalExecutionInfo_STATE_OWNED, persisted.GetState())
	require.Equal(t, ownership.GetFencingEpoch(), persisted.GetFencingEpoch())
	require.Equal(t, "test-local-server", persisted.GetLocalServerId())
	require.NotEqual(t, ownership.GetOwnershipToken(), persisted.GetOwnershipTokenHash())
	require.Len(t, persisted.GetOwnershipTokenHash(), localExecutionOwnershipTokenSizeForTest)
	require.Equal(t, ownership.GetLastSynchronizedEventId(), persisted.GetLastSynchronizedEventId())

	_, err = adminClient.RefreshWorkflowTasks(ctx, &adminservice.RefreshWorkflowTasksRequest{
		NamespaceId: namespaceID,
		Execution:   &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()},
	})
	require.NoError(t, err)
	ordinaryPollCtx, cancelOrdinaryPoll := context.WithTimeout(ctx, 500*time.Millisecond)
	ordinaryResponse, ordinaryErr := server.GetDefaultClient().WorkflowService().PollWorkflowTaskQueue(
		ordinaryPollCtx,
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "ordinary-worker",
		},
	)
	cancelOrdinaryPoll()
	if ordinaryErr == nil {
		require.Empty(t, ordinaryResponse.GetTaskToken())
	} else {
		require.NotEqual(t, codes.OK, status.Code(ordinaryErr))
	}
	require.Len(t, localFirstHistory(ctx, t, server, &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}), 2)

	emptySync := func(syncID string, token []byte, epoch int64) *adminservice.SyncLocalExecutionRequest {
		return &adminservice.SyncLocalExecutionRequest{
			Namespace:            server.GetDefaultNamespace(),
			Execution:            &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()},
			ProtocolVersion:      localexecution.ProtocolVersion,
			LocalServerId:        "test-local-server",
			SyncId:               syncID,
			PreviousEventId:      ownership.GetLastSynchronizedEventId(),
			PreviousEventVersion: ownership.GetLastSynchronizedEventVersion(),
			NewEventId:           ownership.GetLastSynchronizedEventId(),
			NewEventVersion:      ownership.GetLastSynchronizedEventVersion(),
			VersionHistory: &historyspb.VersionHistory{Items: []*historyspb.VersionHistoryItem{{
				EventId: ownership.GetLastSynchronizedEventId(),
				Version: ownership.GetLastSynchronizedEventVersion(),
			}}},
			OwnershipToken: token,
			FencingEpoch:   epoch,
		}
	}
	_, err = adminClient.SyncLocalExecution(ctx, emptySync("wrong-token", []byte("wrong"), ownership.GetFencingEpoch()))
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	renewed, err := adminClient.SyncLocalExecution(
		ctx,
		emptySync("renew-owner", ownership.GetOwnershipToken(), ownership.GetFencingEpoch()),
	)
	require.NoError(t, err)
	require.True(t, renewed.GetLeaseExpirationTime().AsTime().After(ownership.GetLeaseExpirationTime().AsTime()))

	reacquired, err := server.GetDefaultClient().WorkflowService().PollWorkflowTaskQueue(
		ctx,
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "replacement-local-server",
			LocalExecutionOptions: &workflowservice.LocalExecutionPollOptions{
				LocalServerId:          "replacement-local-server",
				ProtocolVersion:        localexecution.ProtocolVersion,
				SyncInterval:           durationpb.New(time.Second),
				RequestedLeaseDuration: durationpb.New(3 * time.Second),
			},
		},
	)
	require.NoError(t, err)
	require.Empty(t, reacquired.GetTaskToken())
	require.Equal(t, int64(2), reacquired.GetLocalExecutionInfo().GetFencingEpoch())
	require.NotEqual(t, ownership.GetOwnershipToken(), reacquired.GetLocalExecutionInfo().GetOwnershipToken())
	require.Equal(t, ownership.GetLastSynchronizedEventId(), reacquired.GetLocalExecutionInfo().GetLastSynchronizedEventId())
	require.Len(t, reacquired.GetHistory().GetEvents(), 2)

	_, err = adminClient.SyncLocalExecution(
		ctx,
		emptySync("stale-owner", ownership.GetOwnershipToken(), ownership.GetFencingEpoch()),
	)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestLocalExecutionLeaseTimerRegeneratesWorkflowTask(t *testing.T) {
	server := temporaltest.NewServer(
		temporaltest.WithT(t),
		temporaltest.WithGlobalNamespace(),
		temporaltest.WithDynamicConfig(dynamicconfig.EnableLocalExecution, true),
	)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	const taskQueue = "local-execution-expiration"

	_, err := server.GetDefaultClient().ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{ID: "local-execution-expiration", TaskQueue: taskQueue},
		"unregistered-workflow",
	)
	require.NoError(t, err)
	poll := func(localServerID string) *workflowservice.PollWorkflowTaskQueueResponse {
		response, err := server.GetDefaultClient().WorkflowService().PollWorkflowTaskQueue(
			ctx,
			&workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: server.GetDefaultNamespace(),
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Identity:  localServerID,
				LocalExecutionOptions: &workflowservice.LocalExecutionPollOptions{
					LocalServerId:          localServerID,
					ProtocolVersion:        localexecution.ProtocolVersion,
					SyncInterval:           durationpb.New(time.Second),
					RequestedLeaseDuration: durationpb.New(3 * time.Second),
				},
			},
		)
		require.NoError(t, err)
		return response
	}

	first := poll("first-local-server")
	require.Equal(t, int64(1), first.GetLocalExecutionInfo().GetFencingEpoch())
	require.Len(t, first.GetHistory().GetEvents(), 2)

	second := poll("second-local-server")
	require.Equal(t, int64(2), second.GetLocalExecutionInfo().GetFencingEpoch())
	require.Equal(t, int64(4), second.GetLocalExecutionInfo().GetLastSynchronizedEventId())
	require.Len(t, second.GetHistory().GetEvents(), 4)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, second.GetHistory().GetEvents()[2].GetEventType())
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, second.GetHistory().GetEvents()[3].GetEventType())
}

const localExecutionOwnershipTokenSizeForTest = 32
