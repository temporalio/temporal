package localexecution

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHistoryClosesWorkflow(t *testing.T) {
	serializer := serialization.NewSerializer()
	replicator := &HistoryReplicator{eventSerializer: serializer}

	for _, eventType := range []enumspb.EventType{
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
	} {
		batch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{{EventId: 3, EventType: eventType}})
		require.NoError(t, err)
		require.True(t, replicator.historyClosesWorkflow([]*commonpb.DataBlob{batch}))
	}

	openBatch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId:   3,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
	}})
	require.NoError(t, err)
	require.False(t, replicator.historyClosesWorkflow([]*commonpb.DataBlob{openBatch}))
	require.False(t, replicator.historyClosesWorkflow(nil))
}

func TestHistoryAfterCursorTrimsReturnedPrefix(t *testing.T) {
	serializer := serialization.NewSerializer()
	firstBatch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{
		{EventId: 1},
		{EventId: 2},
	})
	require.NoError(t, err)
	secondBatch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{
		{EventId: 3},
		{EventId: 4},
	})
	require.NoError(t, err)
	replicator := &HistoryReplicator{
		cursor:          SyncCursor{EventID: 3},
		eventSerializer: serializer,
	}

	delta, err := replicator.historyAfterCursor([]*commonpb.DataBlob{firstBatch, secondBatch})
	require.NoError(t, err)
	require.Len(t, delta, 1)
	events, err := serializer.DeserializeEvents(delta[0])
	require.NoError(t, err)
	require.Equal(t, []int64{4}, []int64{events[0].GetEventId()})
}

func TestSyncRetriesTheSamePreparedRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	source := adminservicemock.NewMockAdminServiceClient(ctrl)
	target := adminservicemock.NewMockAdminServiceClient(ctrl)
	execution := &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{{EventId: 2, Version: 1}},
	}
	source.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{VersionHistory: versionHistory},
		nil,
	).Times(1)

	var firstRequest *adminservice.SyncLocalExecutionRequest
	target.EXPECT().SyncLocalExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.SyncLocalExecutionRequest, _ ...grpc.CallOption) (*adminservice.SyncLocalExecutionResponse, error) {
			firstRequest = request
			return nil, serviceerror.NewUnavailable("connection lost after send")
		},
	)
	target.EXPECT().SyncLocalExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.SyncLocalExecutionRequest, _ ...grpc.CallOption) (*adminservice.SyncLocalExecutionResponse, error) {
			require.Same(t, firstRequest, request)
			return &adminservice.SyncLocalExecutionResponse{
				SyncId:                   request.GetSyncId(),
				AcknowledgedEventId:      request.GetNewEventId(),
				AcknowledgedEventVersion: request.GetNewEventVersion(),
				LeaseExpirationTime:      timestamppb.New(time.Now().Add(time.Minute)),
			}, nil
		},
	)

	replicator := newTestReplicator(t, source, target)
	_, err := replicator.Sync(context.Background(), execution)
	require.Error(t, err)
	result, err := replicator.Sync(context.Background(), execution)
	require.NoError(t, err)
	require.Equal(t, int64(2), result.LastEventID)
}

func TestRunControlledPausesRetriesAndResumes(t *testing.T) {
	ctrl := gomock.NewController(t)
	source := adminservicemock.NewMockAdminServiceClient(ctrl)
	target := adminservicemock.NewMockAdminServiceClient(ctrl)
	local := adminservicemock.NewMockAdminServiceClient(ctrl)
	execution := &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"}
	versionHistory := &historyspb.VersionHistory{
		Items: []*historyspb.VersionHistoryItem{{EventId: 2, Version: 1}},
	}
	source.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{VersionHistory: versionHistory},
		nil,
	)
	target.EXPECT().SyncLocalExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		serviceerror.NewUnavailable("partitioned"),
	)
	target.EXPECT().SyncLocalExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.SyncLocalExecutionRequest, _ ...grpc.CallOption) (*adminservice.SyncLocalExecutionResponse, error) {
			return &adminservice.SyncLocalExecutionResponse{
				SyncId:                   request.GetSyncId(),
				AcknowledgedEventId:      request.GetNewEventId(),
				AcknowledgedEventVersion: request.GetNewEventVersion(),
				LeaseExpirationTime:      timestamppb.New(time.Now().Add(time.Minute)),
			}, nil
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	local.EXPECT().UpdateLocalExecutionState(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.UpdateLocalExecutionStateRequest, _ ...grpc.CallOption) (*adminservice.UpdateLocalExecutionStateResponse, error) {
			require.Equal(t, adminservice.UpdateLocalExecutionStateRequest_STATE_PAUSED, request.GetState())
			return &adminservice.UpdateLocalExecutionStateResponse{}, nil
		},
	)
	local.EXPECT().UpdateLocalExecutionState(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.UpdateLocalExecutionStateRequest, _ ...grpc.CallOption) (*adminservice.UpdateLocalExecutionStateResponse, error) {
			require.Equal(t, adminservice.UpdateLocalExecutionStateRequest_STATE_RUNNABLE, request.GetState())
			cancel()
			return &adminservice.UpdateLocalExecutionStateResponse{}, nil
		},
	)

	controller, err := NewExecutionStateController("namespace", "bridge", 1, local)
	require.NoError(t, err)
	replicator := newTestReplicator(t, source, target)
	err = replicator.RunControlled(ctx, execution, SynchronizationLoopOptions{
		Interval:          20 * time.Millisecond,
		LeaseExpiration:   time.Now().Add(60 * time.Millisecond),
		StateController:   controller,
		RetryInitialDelay: time.Millisecond,
	}, nil)
	require.NoError(t, err)
}

func TestRunControlledInvalidatesAtLeaseExpiration(t *testing.T) {
	ctrl := gomock.NewController(t)
	source := adminservicemock.NewMockAdminServiceClient(ctrl)
	target := adminservicemock.NewMockAdminServiceClient(ctrl)
	local := adminservicemock.NewMockAdminServiceClient(ctrl)
	execution := &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"}
	source.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{
			VersionHistory: &historyspb.VersionHistory{
				Items: []*historyspb.VersionHistoryItem{{EventId: 2, Version: 1}},
			},
		},
		nil,
	)
	target.EXPECT().SyncLocalExecution(gomock.Any(), gomock.Any()).Return(
		nil,
		serviceerror.NewUnavailable("partitioned"),
	).AnyTimes()
	states := make([]adminservice.UpdateLocalExecutionStateRequest_State, 0, 2)
	local.EXPECT().UpdateLocalExecutionState(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.UpdateLocalExecutionStateRequest, _ ...grpc.CallOption) (*adminservice.UpdateLocalExecutionStateResponse, error) {
			states = append(states, request.GetState())
			return &adminservice.UpdateLocalExecutionStateResponse{}, nil
		},
	).Times(2)

	controller, err := NewExecutionStateController("namespace", "bridge", 1, local)
	require.NoError(t, err)
	replicator := newTestReplicator(t, source, target)
	err = replicator.RunControlled(context.Background(), execution, SynchronizationLoopOptions{
		Interval:          10 * time.Millisecond,
		LeaseExpiration:   time.Now().Add(30 * time.Millisecond),
		StateController:   controller,
		RetryInitialDelay: time.Millisecond,
		RetryMaximumDelay: 5 * time.Millisecond,
	}, nil)
	require.ErrorIs(t, err, ErrLocalExecutionOwnershipLost)
	require.Equal(t, []adminservice.UpdateLocalExecutionStateRequest_State{
		adminservice.UpdateLocalExecutionStateRequest_STATE_PAUSED,
		adminservice.UpdateLocalExecutionStateRequest_STATE_OWNERSHIP_LOST,
	}, states)
}

func newTestReplicator(
	t *testing.T,
	source adminservice.AdminServiceClient,
	target adminservice.AdminServiceClient,
) *HistoryReplicator {
	t.Helper()
	replicator, err := NewHistoryReplicator(
		HistoryEndpoint{NamespaceID: "local-namespace-id", AdminClient: source},
		ReplicationTarget{
			Namespace:      "namespace",
			LocalServerID:  "bridge",
			OwnershipToken: []byte("token"),
			FencingEpoch:   1,
			AdminClient:    target,
		},
		SyncCursor{EventID: 2, Version: 1},
	)
	require.NoError(t, err)
	return replicator
}
