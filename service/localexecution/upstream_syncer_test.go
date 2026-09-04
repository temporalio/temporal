package localexecution

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestSyncDelegatesWholeDeltaToHistory(t *testing.T) {
	serializer := serialization.NewSerializer()
	request := validSyncRequest(t, serializer)
	requestBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	require.NoError(t, err)
	requestHash := sha256.Sum256(requestBytes)
	historyClient := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	historyClient.EXPECT().SyncLocalExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, historyRequest *historyservice.SyncLocalExecutionRequest, _ ...grpc.CallOption) (*historyservice.SyncLocalExecutionResponse, error) {
			require.Equal(t, "namespace-id", historyRequest.GetNamespaceId())
			require.Same(t, request, historyRequest.GetRequest())
			require.Equal(t, requestHash[:], historyRequest.GetSyncRequestHash())
			return &historyservice.SyncLocalExecutionResponse{
				Response: &adminservice.SyncLocalExecutionResponse{
					SyncId:                   request.GetSyncId(),
					AcknowledgedEventId:      request.GetNewEventId(),
					AcknowledgedEventVersion: request.GetNewEventVersion(),
				},
			}, nil
		},
	)

	response, err := NewUpstreamSyncer(historyClient, serializer).Sync(t.Context(), "namespace-id", request)
	require.NoError(t, err)
	require.Equal(t, request.SyncId, response.SyncId)
	require.Equal(t, request.NewEventId, response.AcknowledgedEventId)
	require.Equal(t, request.NewEventVersion, response.AcknowledgedEventVersion)
}

func TestValidateSyncLocalExecutionRequest(t *testing.T) {
	serializer := serialization.NewSerializer()
	syncer := NewUpstreamSyncer(nil, serializer)

	require.NoError(t, syncer.validateRequest(validSyncRequest(t, serializer)))

	tests := []struct {
		name   string
		mutate func(*testing.T, *adminservice.SyncLocalExecutionRequest)
	}{
		{
			name: "unknown protocol",
			mutate: func(_ *testing.T, request *adminservice.SyncLocalExecutionRequest) {
				request.ProtocolVersion = 2
			},
		},
		{
			name: "missing execution",
			mutate: func(_ *testing.T, request *adminservice.SyncLocalExecutionRequest) {
				request.Execution = nil
			},
		},
		{
			name: "cursor regression",
			mutate: func(_ *testing.T, request *adminservice.SyncLocalExecutionRequest) {
				request.NewEventId = 1
			},
		},
		{
			name: "previous cursor absent from version history",
			mutate: func(_ *testing.T, request *adminservice.SyncLocalExecutionRequest) {
				request.PreviousEventVersion = 2
			},
		},
		{
			name: "version history does not end at new cursor",
			mutate: func(_ *testing.T, request *adminservice.SyncLocalExecutionRequest) {
				request.NewEventId = 5
			},
		},
		{
			name: "history gap",
			mutate: func(_ *testing.T, request *adminservice.SyncLocalExecutionRequest) {
				request.HistoryBatches = request.HistoryBatches[1:]
			},
		},
		{
			name: "event version mismatch",
			mutate: func(t *testing.T, request *adminservice.SyncLocalExecutionRequest) {
				wrongVersion, err := serializer.SerializeEvents([]*historypb.HistoryEvent{
					{EventId: 4, Version: 2},
				})
				require.NoError(t, err)
				request.HistoryBatches[1] = wrongVersion
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validSyncRequest(t, serializer)
			test.mutate(t, request)
			err := syncer.validateRequest(request)
			require.Error(t, err)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}

func TestValidateEmptySyncLocalExecutionRequest(t *testing.T) {
	serializer := serialization.NewSerializer()
	syncer := NewUpstreamSyncer(nil, serializer)
	request := validSyncRequest(t, serializer)
	request.NewEventId = request.PreviousEventId
	request.NewEventVersion = request.PreviousEventVersion
	request.HistoryBatches = nil
	request.VersionHistory.Items[0].EventId = request.PreviousEventId

	require.NoError(t, syncer.validateRequest(request))
}

func validSyncRequest(
	t *testing.T,
	serializer serialization.Serializer,
) *adminservice.SyncLocalExecutionRequest {
	t.Helper()

	firstBatch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{
		{EventId: 3, Version: 1},
	})
	require.NoError(t, err)
	secondBatch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{
		{EventId: 4, Version: 1},
	})
	require.NoError(t, err)

	return &adminservice.SyncLocalExecutionRequest{
		Namespace: "namespace",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow-id",
			RunId:      "01a0682c-2982-7bb3-b638-929e1c62fbd5",
		},
		ProtocolVersion:      ProtocolVersion,
		LocalServerId:        "local-server-id",
		SyncId:               "sync-id",
		PreviousEventId:      2,
		PreviousEventVersion: 1,
		NewEventId:           4,
		NewEventVersion:      1,
		HistoryBatches:       []*commonpb.DataBlob{firstBatch, secondBatch},
		VersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{{EventId: 4, Version: 1}},
		},
	}
}
