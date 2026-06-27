package history

import (
	"context"
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestDescribeHistoryHost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	controller := shard.NewMockController(ctrl)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	hostInfoProvider := membership.NewMockHostInfoProvider(ctrl)
	h := Handler{
		config: &configs.Config{
			NumberOfShards: 10,
		},
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            log.NewNoopLogger(),
		controller:        controller,
		namespaceRegistry: namespaceRegistry,
		hostInfoProvider:  hostInfoProvider,
	}

	mockShard1 := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	controller.EXPECT().GetShardByID(int32(1)).Return(mockShard1, serviceerror.NewShardOwnershipLost("", ""))

	_, err := h.DescribeHistoryHost(context.Background(), &historyservice.DescribeHistoryHostRequest{
		ShardId: 1,
	})
	assert.Error(t, err)
	var sol *serviceerror.ShardOwnershipLost
	assert.True(t, errors.As(err, &sol))

	mockShard2 := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 2,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	controller.EXPECT().GetShardByID(int32(2)).Return(mockShard2, nil)
	controller.EXPECT().ShardIDs().Return([]int32{2})
	namespaceRegistry.EXPECT().GetRegistrySize().Return(int64(0), int64(0))
	hostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("0.0.0.0"))
	_, err = h.DescribeHistoryHost(context.Background(), &historyservice.DescribeHistoryHostRequest{
		ShardId: 2,
	})
	assert.NoError(t, err)
}

func TestScheduledEventIDFromStateMachineRef(t *testing.T) {
	testCases := []struct {
		name    string
		ref     *persistencespb.StateMachineRef
		wantID  int64
		wantErr bool
	}{
		{
			name:   "recovers id from last key",
			ref:    &persistencespb.StateMachineRef{Path: []*persistencespb.StateMachineKey{{Type: "nexusoperations.Operation", Id: "42"}}},
			wantID: 42,
		},
		{
			name: "uses the last (deepest) key",
			ref: &persistencespb.StateMachineRef{Path: []*persistencespb.StateMachineKey{
				{Type: "nexusoperations.Operation", Id: "7"},
				{Type: "nexusoperations.Cancelation", Id: "99"},
			}},
			wantID: 99,
		},
		{
			name:    "empty path is an error",
			ref:     &persistencespb.StateMachineRef{},
			wantErr: true,
		},
		{
			name:    "non-numeric id is an error",
			ref:     &persistencespb.StateMachineRef{Path: []*persistencespb.StateMachineKey{{Type: "x", Id: "not-a-number"}}},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			id, err := scheduledEventIDFromStateMachineRef(tc.ref)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantID, id)
		})
	}
}

func TestChasmNexusCompletionFromHSMRequest(t *testing.T) {
	testCases := []struct {
		name    string
		request *historyservice.CompleteNexusOperationRequest
		verify  func(t *testing.T, completion *persistencespb.ChasmNexusCompletion)
	}{
		{
			name: "success carries the payload",
			request: &historyservice.CompleteNexusOperationRequest{
				State:          string(nexus.OperationStateSucceeded),
				OperationToken: "op-token",
				Completion:     &tokenspb.NexusOperationCompletion{RequestId: "req-1"},
				Outcome:        &historyservice.CompleteNexusOperationRequest_Success{Success: &commonpb.Payload{Data: []byte("ok")}},
			},
			verify: func(t *testing.T, completion *persistencespb.ChasmNexusCompletion) {
				require.Equal(t, "req-1", completion.GetRequestId())
				require.Equal(t, "op-token", completion.GetOperationToken())
				require.NotNil(t, completion.GetSuccess())
				require.Equal(t, []byte("ok"), completion.GetSuccess().GetData())
			},
		},
		{
			name: "failure is converted to a temporal failure",
			request: &historyservice.CompleteNexusOperationRequest{
				State:      string(nexus.OperationStateFailed),
				Completion: &tokenspb.NexusOperationCompletion{RequestId: "req-2"},
				Outcome:    &historyservice.CompleteNexusOperationRequest_Failure{Failure: &nexuspb.Failure{Message: "boom"}},
			},
			verify: func(t *testing.T, completion *persistencespb.ChasmNexusCompletion) {
				require.NotNil(t, completion.GetFailure())
				require.Equal(t, "boom", completion.GetFailure().GetMessage())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			completion, err := chasmNexusCompletionFromHSMRequest(tc.request)
			require.NoError(t, err)
			tc.verify(t, completion)
		})
	}
}

func TestScheduledEventIDFromComponentPath(t *testing.T) {
	testCases := []struct {
		name    string
		path    []string
		wantID  int64
		wantErr bool
	}{
		{name: "recovers id from Operations path", path: []string{"Operations", "42"}, wantID: 42},
		{name: "uses the last segment", path: []string{"Operations", "7", "13"}, wantID: 13},
		{name: "empty path is an error", path: nil, wantErr: true},
		{name: "non-numeric segment is an error", path: []string{"Operations", "nope"}, wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			id, err := scheduledEventIDFromComponentPath(tc.path)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantID, id)
		})
	}
}

func TestNexusOperationErrorFromChasmRequest(t *testing.T) {
	testCases := []struct {
		name      string
		request   *historyservice.CompleteNexusOperationChasmRequest
		wantOpErr bool
	}{
		{
			name: "success yields no operation error",
			request: &historyservice.CompleteNexusOperationChasmRequest{
				Completion: &tokenspb.NexusOperationCompletion{RequestId: "r"},
				Outcome:    &historyservice.CompleteNexusOperationChasmRequest_Success{Success: &commonpb.Payload{}},
			},
			wantOpErr: false,
		},
		{
			name: "failure yields an operation error",
			request: &historyservice.CompleteNexusOperationChasmRequest{
				Completion: &tokenspb.NexusOperationCompletion{RequestId: "r"},
				Outcome:    &historyservice.CompleteNexusOperationChasmRequest_Failure{Failure: &failurepb.Failure{Message: "boom"}},
			},
			wantOpErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opErr, err := nexusOperationErrorFromChasmRequest(tc.request)
			require.NoError(t, err)
			if tc.wantOpErr {
				require.NotNil(t, opErr)
			} else {
				require.Nil(t, opErr)
			}
		})
	}
}
