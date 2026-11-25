package frontend

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testcontext"
	"go.uber.org/mock/gomock"
)

func TestResetNamespaceReplicationAckForFreshConnect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		persistedBefore *persistence.GetClusterMetadataResponse
		enabled         bool
		wantDelete      bool
	}{
		{
			name:       "new enabled connection",
			enabled:    true,
			wantDelete: true,
		},
		{
			name: "previously disabled connection",
			persistedBefore: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{},
			},
			enabled:    true,
			wantDelete: true,
		},
		{
			name: "already enabled connection",
			persistedBefore: &persistence.GetClusterMetadataResponse{
				ClusterMetadata: &persistencespb.ClusterMetadata{IsConnectionEnabled: true},
			},
			enabled: true,
		},
		{
			name: "disabled connection",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			queue := persistence.NewMockNamespaceReplicationQueue(ctrl)
			if test.wantDelete {
				queue.EXPECT().DeleteAckLevel(gomock.Any(), "remote").Return(nil)
			}

			err := resetNamespaceReplicationAckForFreshConnect(
				testcontext.For(t),
				queue,
				test.persistedBefore,
				"remote",
				test.enabled,
			)
			require.NoError(t, err)
		})
	}
}

func TestResetNamespaceReplicationAckForFreshConnectError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	queue := persistence.NewMockNamespaceReplicationQueue(ctrl)
	wantErr := errors.New("delete ack level failed")
	queue.EXPECT().DeleteAckLevel(gomock.Any(), "remote").Return(wantErr)

	err := resetNamespaceReplicationAckForFreshConnect(
		testcontext.For(t),
		queue,
		nil,
		"remote",
		true,
	)
	require.ErrorIs(t, err, wantErr)
}

func TestGetNamespaceReplicationMessagesValidatesCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterName string
		clusters    map[string]cluster.ClusterInformation
		wantErr     bool
	}{
		{name: "empty cluster name", wantErr: true},
		{name: "unknown cluster", clusterName: "unknown", wantErr: true},
		{
			name:        "disabled cluster",
			clusterName: "remote",
			clusters: map[string]cluster.ClusterInformation{
				"remote": {Enabled: false},
			},
			wantErr: true,
		},
		{
			name:        "current cluster",
			clusterName: "current",
			clusters: map[string]cluster.ClusterInformation{
				"current": {Enabled: true},
			},
			wantErr: true,
		},
		{
			name:        "enabled remote cluster",
			clusterName: "remote",
			clusters: map[string]cluster.ClusterInformation{
				"remote": {Enabled: true},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			queue := persistence.NewMockNamespaceReplicationQueue(ctrl)
			metadata := cluster.NewMockMetadata(ctrl)
			handler := &AdminHandler{
				logger:                    log.NewNoopLogger(),
				namespaceReplicationQueue: queue,
				clusterMetadata:           metadata,
			}
			if test.clusterName != "" {
				metadata.EXPECT().GetAllClusterInfo().Return(test.clusters)
				metadata.EXPECT().GetCurrentClusterName().Return("current")
			}
			if !test.wantErr {
				queue.EXPECT().GetReplicationMessages(gomock.Any(), int64(5), getNamespaceReplicationMessageBatchSize).
					Return(nil, int64(5), nil)
			}

			response, err := handler.GetNamespaceReplicationMessages(
				testcontext.For(t),
				&adminservice.GetNamespaceReplicationMessagesRequest{
					ClusterName:            test.clusterName,
					LastRetrievedMessageId: 5,
					LastProcessedMessageId: defaultLastMessageID,
				},
			)
			if test.wantErr {
				require.Error(t, err)
				if test.clusterName != "" {
					var failedPrecondition *serviceerror.FailedPrecondition
					require.ErrorAs(t, err, &failedPrecondition)
				}
				require.Nil(t, response)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int64(5), response.GetMessages().GetLastRetrievedMessageId())
		})
	}
}

func (s *adminHandlerSuite) TestAddOrUpdateRemoteClusterFreshConnectResetsAckLevel() {
	rpcAddress := uuid.NewString()
	clusterName := uuid.NewString()
	clusterID := uuid.NewString()
	namespaceReplicationQueue := persistence.NewMockNamespaceReplicationQueue(s.controller)
	s.handler.namespaceReplicationQueue = namespaceReplicationQueue

	s.mockMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(10)).Times(2)
	s.mockMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{})
	s.mockClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockAdminClient,
	)
	s.mockAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 10,
			InitialFailoverVersion:   1,
			IsGlobalNamespaceEnabled: true,
		}, nil,
	)
	s.mockClusterMetadataManager.EXPECT().GetClusterMetadata(
		gomock.Any(),
		&persistence.GetClusterMetadataRequest{ClusterName: clusterName},
	).Return(nil, serviceerror.NewNotFound("expected empty result"))
	namespaceReplicationQueue.EXPECT().DeleteAckLevel(gomock.Any(), clusterName).Return(nil)
	s.mockClusterMetadataManager.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).Return(true, nil)

	_, err := s.handler.AddOrUpdateRemoteCluster(
		testcontext.For(s.T()),
		&adminservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               rpcAddress,
			EnableRemoteClusterConnection: true,
		},
	)
	s.NoError(err)
}

func (s *operatorHandlerSuite) TestAddOrUpdateRemoteClusterFreshConnectResetsAckLevel() {
	rpcAddress := uuid.NewString()
	clusterName := uuid.NewString()
	clusterID := uuid.NewString()
	namespaceReplicationQueue := persistence.NewMockNamespaceReplicationQueue(s.controller)
	s.handler.namespaceReplicationQueue = namespaceReplicationQueue

	s.mockResource.ClusterMetadata.EXPECT().GetFailoverVersionIncrement().Return(int64(10)).Times(2)
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{})
	s.mockResource.ClientFactory.EXPECT().NewRemoteAdminClientWithTimeout(rpcAddress, gomock.Any(), gomock.Any()).Return(
		s.mockResource.RemoteAdminClient,
	)
	s.mockResource.RemoteAdminClient.EXPECT().DescribeCluster(gomock.Any(), &adminservice.DescribeClusterRequest{}).Return(
		&adminservice.DescribeClusterResponse{
			ClusterId:                clusterID,
			ClusterName:              clusterName,
			HistoryShardCount:        4,
			FailoverVersionIncrement: 10,
			InitialFailoverVersion:   1,
			IsGlobalNamespaceEnabled: true,
		}, nil,
	)
	s.mockResource.ClusterMetadataMgr.EXPECT().GetClusterMetadata(
		gomock.Any(),
		&persistence.GetClusterMetadataRequest{ClusterName: clusterName},
	).Return(nil, serviceerror.NewNotFound("expected empty result"))
	namespaceReplicationQueue.EXPECT().DeleteAckLevel(gomock.Any(), clusterName).Return(nil)
	s.mockResource.ClusterMetadataMgr.EXPECT().SaveClusterMetadata(gomock.Any(), gomock.Any()).Return(true, nil)

	_, err := s.handler.AddOrUpdateRemoteCluster(
		testcontext.For(s.T()),
		&operatorservice.AddOrUpdateRemoteClusterRequest{
			FrontendAddress:               rpcAddress,
			EnableRemoteClusterConnection: true,
		},
	)
	s.NoError(err)
}
