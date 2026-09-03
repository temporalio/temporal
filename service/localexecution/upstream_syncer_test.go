package localexecution

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestReleasedSyncRepeatIsIdempotentAndFingerprintProtected(t *testing.T) {
	serializer := serialization.NewSerializer()
	request := validSyncRequest(t, serializer)
	request.Release = true
	requestBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	require.NoError(t, err)
	requestHash := sha256.Sum256(requestBytes)
	localInfo := &persistencespb.LocalExecutionInfo{
		State:               persistencespb.LocalExecutionInfo_STATE_UNOWNED,
		LastSyncId:          request.SyncId,
		LastSyncRequestHash: requestHash[:],
	}
	historyClient := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(
		mutableStateAtWithLocalInfo(request.NewEventId, request.NewEventVersion, localInfo), nil,
	).Times(2)
	syncer := NewUpstreamSyncer(historyClient, serializer)

	response, err := syncer.Sync(t.Context(), "namespace-id", request)
	require.NoError(t, err)
	require.Equal(t, request.NewEventId, response.GetAcknowledgedEventId())
	require.Nil(t, response.GetLeaseExpirationTime())

	conflict := proto.Clone(request).(*adminservice.SyncLocalExecutionRequest)
	conflict.LocalServerId = "different-local-server"
	_, err = syncer.Sync(t.Context(), "namespace-id", conflict)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
}

func TestOwnedSyncRenewsBeforeAndAfterReplication(t *testing.T) {
	serializer := serialization.NewSerializer()
	request := validSyncRequest(t, serializer)
	request.OwnershipToken = []byte("ownership-token")
	request.FencingEpoch = 7
	tokenHash := sha256.Sum256(request.OwnershipToken)
	localInfo := &persistencespb.LocalExecutionInfo{
		State:               persistencespb.LocalExecutionInfo_STATE_OWNED,
		FencingEpoch:        request.FencingEpoch,
		LocalServerId:       request.LocalServerId,
		OwnershipTokenHash:  tokenHash[:],
		LeaseExpirationTime: timestamppb.New(time.Now().Add(time.Minute)),
	}
	historyClient := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))

	gomock.InOrder(
		historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mutableStateAtWithLocalInfo(2, 1, localInfo), nil),
		historyClient.EXPECT().RenewLocalExecutionLease(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, renewal *historyservice.RenewLocalExecutionLeaseRequest, _ ...grpc.CallOption) (*historyservice.RenewLocalExecutionLeaseResponse, error) {
				require.Equal(t, int64(2), renewal.GetPreviousEventId())
				require.Equal(t, int64(2), renewal.GetNewEventId())
				require.Equal(t, request.SyncId+"/lease", renewal.GetSyncId())
				require.NotEmpty(t, renewal.GetSyncRequestHash())
				return &historyservice.RenewLocalExecutionLeaseResponse{LeaseExpirationTime: localInfo.LeaseExpirationTime}, nil
			},
		),
		historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), gomock.Any()).Return(&historyservice.ReplicateEventsV2Response{}, nil),
		historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mutableStateAt(3, 1), nil),
		historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), gomock.Any()).Return(&historyservice.ReplicateEventsV2Response{}, nil),
		historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mutableStateAt(4, 1), nil),
		historyClient.EXPECT().RenewLocalExecutionLease(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, renewal *historyservice.RenewLocalExecutionLeaseRequest, _ ...grpc.CallOption) (*historyservice.RenewLocalExecutionLeaseResponse, error) {
				require.Equal(t, int64(2), renewal.GetPreviousEventId())
				require.Equal(t, int64(4), renewal.GetNewEventId())
				require.Equal(t, request.SyncId, renewal.GetSyncId())
				require.NotEmpty(t, renewal.GetSyncRequestHash())
				return &historyservice.RenewLocalExecutionLeaseResponse{LeaseExpirationTime: localInfo.LeaseExpirationTime}, nil
			},
		),
	)

	response, err := NewUpstreamSyncer(historyClient, serializer).Sync(t.Context(), "namespace-id", request)
	require.NoError(t, err)
	require.Equal(t, request.NewEventId, response.GetAcknowledgedEventId())
	require.Equal(t, localInfo.LeaseExpirationTime, response.GetLeaseExpirationTime())
}

func TestSyncWaitsForEachHistoryBatch(t *testing.T) {
	serializer := serialization.NewSerializer()
	request := validSyncRequest(t, serializer)
	historyClient := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))

	gomock.InOrder(
		historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mutableStateAt(2, 1), nil),
		historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, replication *historyservice.ReplicateEventsV2Request, _ ...grpc.CallOption) (*historyservice.ReplicateEventsV2Response, error) {
				require.Equal(t, request.HistoryBatches[0], replication.Events)
				return &historyservice.ReplicateEventsV2Response{}, nil
			},
		),
		historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mutableStateAt(3, 1), nil),
		historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, replication *historyservice.ReplicateEventsV2Request, _ ...grpc.CallOption) (*historyservice.ReplicateEventsV2Response, error) {
				require.Equal(t, request.HistoryBatches[1], replication.Events)
				return &historyservice.ReplicateEventsV2Response{}, nil
			},
		),
		historyClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mutableStateAt(4, 1), nil),
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

func mutableStateAt(eventID int64, version int64) *historyservice.GetMutableStateResponse {
	return mutableStateAtWithLocalInfo(eventID, version, nil)
}

func mutableStateAtWithLocalInfo(
	eventID int64,
	version int64,
	localInfo *persistencespb.LocalExecutionInfo,
) *historyservice.GetMutableStateResponse {
	return &historyservice.GetMutableStateResponse{
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{Items: []*historyspb.VersionHistoryItem{{EventId: eventID, Version: version}}},
			},
		},
		LocalExecutionInfo: localInfo,
	}
}
