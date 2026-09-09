package localexecution

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBridgeRuntimePollsWithFinalConfiguration(t *testing.T) {
	ctrl := gomock.NewController(t)
	upstreamAdmin := adminservicemock.NewMockAdminServiceClient(ctrl)
	localAdmin := adminservicemock.NewMockAdminServiceClient(ctrl)
	store := newTestBridgeStateStore(t)
	configuration := validRuntimeConfiguration()

	ctx, cancel := context.WithCancel(context.Background())
	workflowClient := workflowTaskPollerFunc(
		func(
			pollContext context.Context,
			request *workflowservice.PollWorkflowTaskQueueRequest,
			_ ...grpc.CallOption,
		) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
			require.Equal(t, "task-queue", request.GetTaskQueue().GetName())
			require.Equal(t, store.LocalServerID(), request.GetIdentity())
			require.Equal(t, store.LocalServerID(), request.GetLocalExecutionOptions().GetLocalServerId())
			require.Equal(t, int32(ProtocolVersion), request.GetLocalExecutionOptions().GetProtocolVersion())
			require.Equal(t, time.Second, request.GetLocalExecutionOptions().GetSyncInterval().AsDuration())
			require.Equal(t, 3*time.Second, request.GetLocalExecutionOptions().GetRequestedLeaseDuration().AsDuration())
			cancel()
			return nil, pollContext.Err()
		},
	)

	runtime, err := NewBridgeRuntime(BridgeRuntimeOptions{
		Configuration:       configuration,
		StateStore:          store,
		UpstreamNamespaceID: "upstream-namespace-id",
		LocalNamespaceID:    "local-namespace-id",
		UpstreamWorkflow:    workflowClient,
		UpstreamAdmin:       upstreamAdmin,
		LocalAdmin:          localAdmin,
	})
	require.NoError(t, err)
	configuration.Registrations.TaskQueue = "mutated-task-queue"
	configuration.Registrations.WorkflowTypes[0] = "mutated-workflow"

	done, err := runtime.Start(ctx)
	require.NoError(t, err)
	require.NoError(t, <-done)
	_, err = runtime.Start(ctx)
	require.EqualError(t, err, "bridge runtime is already started")
}

func TestBridgeRuntimeRequestSynchronization(t *testing.T) {
	execution := &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"}
	trigger := make(chan struct{}, 1)
	runtime := &BridgeRuntime{
		syncTriggers: map[string]chan struct{}{"workflow-id\x00run-id": trigger},
	}
	require.NoError(t, runtime.RequestSynchronization(execution))
	require.NoError(t, runtime.RequestSynchronization(execution))
	require.Len(t, trigger, 1)
	<-trigger
	require.EqualError(
		t,
		runtime.RequestSynchronization(&commonpb.WorkflowExecution{WorkflowId: "other", RunId: "run-id"}),
		"workflow execution is not managed by this bridge",
	)
}

func TestBridgeRuntimeRejectsLegacyQueryWithoutStopping(t *testing.T) {
	ctrl := gomock.NewController(t)
	workflowClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	runtime := &BridgeRuntime{
		configuration:    BridgeConfiguration{Namespace: "namespace"},
		upstreamWorkflow: workflowClient,
	}
	response := &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken:         []byte("query-token"),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"},
		Query:             &querypb.WorkflowQuery{QueryType: "__temporal_workflow_metadata"},
		PollerGroupId:     "poller-group-id",
	}

	workflowClient.EXPECT().RespondQueryTaskCompleted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			request *workflowservice.RespondQueryTaskCompletedRequest,
			_ ...grpc.CallOption,
		) (*workflowservice.RespondQueryTaskCompletedResponse, error) {
			require.Equal(t, "namespace", request.GetNamespace())
			require.Equal(t, response.GetTaskToken(), request.GetTaskToken())
			require.Equal(t, "poller-group-id", request.GetPollerGroupId())
			require.Equal(t, "queries for locally owned workflows are not supported yet", request.GetErrorMessage())
			require.Equal(t, enumspb.QUERY_RESULT_TYPE_FAILED, request.GetCompletedType())
			return &workflowservice.RespondQueryTaskCompletedResponse{}, nil
		},
	)

	managed, err := runtime.handleAcquisitionResponse(context.Background(), response)
	require.NoError(t, err)
	require.Nil(t, managed)
}

func TestBridgeRuntimeQueryResponseFailureDoesNotStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	workflowClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	runtime := &BridgeRuntime{
		configuration:    BridgeConfiguration{Namespace: "namespace"},
		upstreamWorkflow: workflowClient,
	}
	response := &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken:         []byte("query-token"),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"},
		Query:             &querypb.WorkflowQuery{QueryType: "query"},
	}
	workflowClient.EXPECT().RespondQueryTaskCompleted(gomock.Any(), gomock.Any()).Return(
		nil,
		serviceerror.NewUnavailable("upstream unavailable"),
	)

	managed, err := runtime.handleAcquisitionResponse(context.Background(), response)
	require.NoError(t, err)
	require.Nil(t, managed)
}

func TestBridgeRuntimeStillRejectsOrdinaryWorkflowTask(t *testing.T) {
	runtime := &BridgeRuntime{}
	response := &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken:         []byte("workflow-task-token"),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"},
	}

	managed, err := runtime.handleAcquisitionResponse(context.Background(), response)
	require.EqualError(t, err, "upstream returned an ordinary workflow task to a local execution poll")
	require.Nil(t, managed)
}

func TestBridgeRuntimePersistsAcquisitionBeforeImport(t *testing.T) {
	ctrl := gomock.NewController(t)
	workflowClient := unusedWorkflowTaskPoller
	upstreamAdmin := adminservicemock.NewMockAdminServiceClient(ctrl)
	localAdmin := adminservicemock.NewMockAdminServiceClient(ctrl)
	store := newTestBridgeStateStore(t)
	runtime := newTestBridgeRuntime(t, store, workflowClient, upstreamAdmin, localAdmin)
	execution := &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{{EventId: 2, Version: 1}},
	}
	historyBatch := &commonpb.DataBlob{Data: []byte("history")}

	upstreamAdmin.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ context.Context,
			_ *adminservice.GetWorkflowExecutionRawHistoryV2Request,
			_ ...grpc.CallOption,
		) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
			records, err := store.LoadExecutions()
			require.NoError(t, err)
			require.Len(t, records, 1)
			require.Equal(t, BridgeExecutionPhaseImporting, records[0].Phase)
			return &adminservice.GetWorkflowExecutionRawHistoryV2Response{
				HistoryBatches: []*commonpb.DataBlob{historyBatch},
				VersionHistory: versionHistory,
			}, nil
		},
	)
	gomock.InOrder(
		localAdmin.EXPECT().ImportWorkflowExecution(gomock.Any(), gomock.Any()).Return(
			&adminservice.ImportWorkflowExecutionResponse{Token: []byte("import-token")}, nil,
		),
		localAdmin.EXPECT().ImportWorkflowExecution(gomock.Any(), gomock.Any()).Return(
			&adminservice.ImportWorkflowExecutionResponse{}, nil,
		),
		localAdmin.EXPECT().UpdateLocalExecutionState(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				request *adminservice.UpdateLocalExecutionStateRequest,
				_ ...grpc.CallOption,
			) (*adminservice.UpdateLocalExecutionStateResponse, error) {
				require.Equal(t, adminservice.UpdateLocalExecutionStateRequest_STATE_PAUSED, request.GetState())
				return &adminservice.UpdateLocalExecutionStateResponse{}, nil
			},
		),
		localAdmin.EXPECT().UpdateLocalExecutionState(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				request *adminservice.UpdateLocalExecutionStateRequest,
				_ ...grpc.CallOption,
			) (*adminservice.UpdateLocalExecutionStateResponse, error) {
				require.Equal(t, adminservice.UpdateLocalExecutionStateRequest_STATE_RUNNABLE, request.GetState())
				return &adminservice.UpdateLocalExecutionStateResponse{}, nil
			},
		),
	)

	managed, err := runtime.adoptExecution(context.Background(), testAcquisition(execution, time.Now().Add(3*time.Second)))
	require.NoError(t, err)
	require.NotNil(t, managed)
	records, err := store.LoadExecutions()
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, BridgeExecutionPhaseReady, records[0].Phase)
	require.Equal(t, execution.GetWorkflowId(), records[0].WorkflowID)
	require.Equal(t, int64(2), records[0].LastSynchronizedEventID)
}

func TestBridgeRuntimeRevalidatesRecoveredExecution(t *testing.T) {
	ctrl := gomock.NewController(t)
	upstreamAdmin := adminservicemock.NewMockAdminServiceClient(ctrl)
	localAdmin := adminservicemock.NewMockAdminServiceClient(ctrl)
	store := newTestBridgeStateStore(t)
	record := validBridgeExecutionRecord()
	record.NamespaceID = "upstream-namespace-id"
	record.LeaseExpiration = time.Now().Add(3 * time.Second)
	require.NoError(t, store.SaveExecution(record))
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{{EventId: 2, Version: 1}},
	}
	renewedLease := time.Now().Add(6 * time.Second)

	gomock.InOrder(
		localAdmin.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
			&adminservice.GetWorkflowExecutionRawHistoryV2Response{}, nil,
		),
		localAdmin.EXPECT().UpdateLocalExecutionState(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				request *adminservice.UpdateLocalExecutionStateRequest,
				_ ...grpc.CallOption,
			) (*adminservice.UpdateLocalExecutionStateResponse, error) {
				require.Equal(t, adminservice.UpdateLocalExecutionStateRequest_STATE_PAUSED, request.GetState())
				return &adminservice.UpdateLocalExecutionStateResponse{}, nil
			},
		),
		localAdmin.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
			&adminservice.GetWorkflowExecutionRawHistoryV2Response{VersionHistory: versionHistory}, nil,
		),
		upstreamAdmin.EXPECT().SyncLocalExecution(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				request *adminservice.SyncLocalExecutionRequest,
				_ ...grpc.CallOption,
			) (*adminservice.SyncLocalExecutionResponse, error) {
				require.False(t, request.GetRelease())
				return &adminservice.SyncLocalExecutionResponse{
					SyncId:                   request.GetSyncId(),
					AcknowledgedEventId:      request.GetNewEventId(),
					AcknowledgedEventVersion: request.GetNewEventVersion(),
					LeaseExpirationTime:      timestamppb.New(renewedLease),
				}, nil
			},
		),
		localAdmin.EXPECT().UpdateLocalExecutionState(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				_ context.Context,
				request *adminservice.UpdateLocalExecutionStateRequest,
				_ ...grpc.CallOption,
			) (*adminservice.UpdateLocalExecutionStateResponse, error) {
				require.Equal(t, adminservice.UpdateLocalExecutionStateRequest_STATE_RUNNABLE, request.GetState())
				return &adminservice.UpdateLocalExecutionStateResponse{}, nil
			},
		),
	)

	ctx, cancel := context.WithCancel(context.Background())
	workflowClient := workflowTaskPollerFunc(
		func(
			pollContext context.Context,
			_ *workflowservice.PollWorkflowTaskQueueRequest,
			_ ...grpc.CallOption,
		) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
			cancel()
			return nil, pollContext.Err()
		},
	)
	runtime := newTestBridgeRuntime(t, store, workflowClient, upstreamAdmin, localAdmin)
	done, err := runtime.Start(ctx)
	require.NoError(t, err)
	require.NoError(t, <-done)

	records, err := store.LoadExecutions()
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.WithinDuration(t, renewedLease, records[0].LeaseExpiration, time.Millisecond)
}

func TestBridgeRuntimeDropsExpiredUnimportedAcquisition(t *testing.T) {
	ctrl := gomock.NewController(t)
	upstreamAdmin := adminservicemock.NewMockAdminServiceClient(ctrl)
	localAdmin := adminservicemock.NewMockAdminServiceClient(ctrl)
	store := newTestBridgeStateStore(t)
	record := validBridgeExecutionRecord()
	record.NamespaceID = "upstream-namespace-id"
	record.Phase = BridgeExecutionPhaseImporting
	record.LeaseExpiration = time.Now().Add(-time.Second)
	require.NoError(t, store.SaveExecution(record))
	localAdmin.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		nil, serviceerror.NewNotFound("execution not imported"),
	)

	ctx, cancel := context.WithCancel(context.Background())
	workflowClient := workflowTaskPollerFunc(
		func(
			pollContext context.Context,
			_ *workflowservice.PollWorkflowTaskQueueRequest,
			_ ...grpc.CallOption,
		) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
			cancel()
			return nil, pollContext.Err()
		},
	)
	runtime := newTestBridgeRuntime(t, store, workflowClient, upstreamAdmin, localAdmin)
	done, err := runtime.Start(ctx)
	require.NoError(t, err)
	require.NoError(t, <-done)
	records, err := store.LoadExecutions()
	require.NoError(t, err)
	require.Empty(t, records)
}

func newTestBridgeRuntime(
	t *testing.T,
	store *BridgeStateStore,
	workflowClient WorkflowTaskPoller,
	upstreamAdmin adminservice.AdminServiceClient,
	localAdmin adminservice.AdminServiceClient,
) *BridgeRuntime {
	t.Helper()
	runtime, err := NewBridgeRuntime(BridgeRuntimeOptions{
		Configuration:       validRuntimeConfiguration(),
		StateStore:          store,
		UpstreamNamespaceID: "upstream-namespace-id",
		LocalNamespaceID:    "local-namespace-id",
		UpstreamWorkflow:    workflowClient,
		UpstreamAdmin:       upstreamAdmin,
		LocalAdmin:          localAdmin,
	})
	require.NoError(t, err)
	return runtime
}

func newTestBridgeStateStore(t *testing.T) *BridgeStateStore {
	t.Helper()
	store, err := OpenBridgeStateStore(privateTempDir(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func validRuntimeConfiguration() BridgeConfiguration {
	configuration := validBridgeConfiguration()
	configuration.Options.SyncIntervalMilliseconds = 1_000
	configuration.Registrations.TaskQueue = "task-queue"
	configuration.Registrations.WorkflowTypes = []string{"workflow-type"}
	configuration.Registrations.ActivityTypes = []string{"activity-type"}
	return configuration
}

func testAcquisition(
	execution *commonpb.WorkflowExecution,
	leaseExpiration time.Time,
) *workflowservice.PollWorkflowTaskQueueResponse {
	return &workflowservice.PollWorkflowTaskQueueResponse{
		WorkflowExecution: execution,
		WorkflowType:      &commonpb.WorkflowType{Name: "workflow-type"},
		LocalExecutionInfo: &workflowservice.LocalExecutionTaskInfo{
			OwnershipToken:               bytes.Repeat([]byte{1}, 32),
			FencingEpoch:                 1,
			LeaseExpirationTime:          timestamppb.New(leaseExpiration),
			LastSynchronizedEventId:      2,
			LastSynchronizedEventVersion: 1,
		},
	}
}

type workflowTaskPollerFunc func(
	context.Context,
	*workflowservice.PollWorkflowTaskQueueRequest,
	...grpc.CallOption,
) (*workflowservice.PollWorkflowTaskQueueResponse, error)

func (f workflowTaskPollerFunc) PollWorkflowTaskQueue(
	ctx context.Context,
	request *workflowservice.PollWorkflowTaskQueueRequest,
	opts ...grpc.CallOption,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	return f(ctx, request, opts...)
}

var unusedWorkflowTaskPoller = workflowTaskPollerFunc(func(
	context.Context,
	*workflowservice.PollWorkflowTaskQueueRequest,
	...grpc.CallOption,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	return &workflowservice.PollWorkflowTaskQueueResponse{}, nil
})
