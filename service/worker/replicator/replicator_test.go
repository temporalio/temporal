package replicator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testcontext"
	"go.uber.org/mock/gomock"
)

func TestCleanupAckedMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		ackLevels            map[string]int64
		clusters             map[string]cluster.ClusterInformation
		currentCluster       string
		deletedMessageID     int64
		wantDeletedAckLevels []string
		wantDeleteMessages   bool
		wantDeleteBefore     int64
		wantDeletedMessageID int64
	}{
		{
			name:                 "no connected clusters",
			deletedMessageID:     5,
			wantDeletedMessageID: 5,
		},
		{
			name: "delete disconnected ack levels",
			ackLevels: map[string]int64{
				"active":  3,
				"s-sc037": 24,
				"dev5":    32,
			},
			currentCluster:       "source",
			deletedMessageID:     5,
			wantDeletedAckLevels: []string{"active", "s-sc037", "dev5"},
			wantDeletedMessageID: 5,
		},
		{
			name: "delete disabled and current ack levels",
			ackLevels: map[string]int64{
				"enabled":  10,
				"disabled": 20,
				"current":  30,
			},
			clusters: map[string]cluster.ClusterInformation{
				"enabled":  {Enabled: true},
				"disabled": {Enabled: false},
				"current":  {Enabled: true},
			},
			currentCluster:       "current",
			deletedMessageID:     5,
			wantDeletedAckLevels: []string{"disabled", "current"},
			wantDeleteMessages:   true,
			wantDeleteBefore:     10,
			wantDeletedMessageID: 10,
		},
		{
			name:      "missing ack level blocks cleanup",
			ackLevels: map[string]int64{"cluster-a": 10},
			clusters: map[string]cluster.ClusterInformation{
				"cluster-a": {Enabled: true},
				"cluster-b": {Enabled: true},
			},
			currentCluster:       "current",
			deletedMessageID:     5,
			wantDeletedMessageID: 5,
		},
		{
			name: "delete to connected low watermark",
			ackLevels: map[string]int64{
				"cluster-a": 3,
				"cluster-b": 10,
			},
			clusters: map[string]cluster.ClusterInformation{
				"cluster-a": {Enabled: true},
				"cluster-b": {Enabled: true},
			},
			currentCluster:       "current",
			deletedMessageID:     1,
			wantDeleteMessages:   true,
			wantDeleteBefore:     3,
			wantDeletedMessageID: 3,
		},
		{
			name:      "low watermark does not advance",
			ackLevels: map[string]int64{"cluster-a": 3},
			clusters: map[string]cluster.ClusterInformation{
				"cluster-a": {Enabled: true},
			},
			currentCluster:       "current",
			deletedMessageID:     5,
			wantDeletedMessageID: 5,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			nsQueue := persistence.NewMockNamespaceReplicationQueue(ctrl)
			clusterMetadata := cluster.NewMockMetadata(ctrl)
			replicator := &Replicator{
				namespaceReplicationQueue: nsQueue,
				clusterMetadata:           clusterMetadata,
			}

			nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(test.ackLevels, nil)
			clusterMetadata.EXPECT().GetAllClusterInfo().Return(test.clusters)
			clusterMetadata.EXPECT().GetCurrentClusterName().Return(test.currentCluster)
			for _, clusterName := range test.wantDeletedAckLevels {
				nsQueue.EXPECT().DeleteAckLevel(gomock.Any(), clusterName).Return(nil)
			}
			if test.wantDeleteMessages {
				nsQueue.EXPECT().DeleteMessagesBefore(gomock.Any(), test.wantDeleteBefore).Return(nil)
			}

			deletedMessageID, err := replicator.cleanupAckedMessages(
				testcontext.For(t),
				test.deletedMessageID,
			)
			require.NoError(t, err)
			require.Equal(t, test.wantDeletedMessageID, deletedMessageID)
		})
	}
}

func TestCleanupAckedMessagesGetAckLevelsError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	nsQueue := persistence.NewMockNamespaceReplicationQueue(ctrl)
	replicator := &Replicator{
		namespaceReplicationQueue: nsQueue,
		clusterMetadata:           cluster.NewMockMetadata(ctrl),
	}
	wantErr := errors.New("get ack levels failed")
	nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(nil, wantErr)

	deletedMessageID, err := replicator.cleanupAckedMessages(testcontext.For(t), 5)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, int64(5), deletedMessageID)
}

func TestCleanupAckedMessagesDeleteAckLevelError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	nsQueue := persistence.NewMockNamespaceReplicationQueue(ctrl)
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	replicator := &Replicator{
		namespaceReplicationQueue: nsQueue,
		clusterMetadata:           clusterMetadata,
	}
	wantErr := errors.New("delete ack level failed")
	nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(map[string]int64{"stale": 10}, nil)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(nil)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("current")
	nsQueue.EXPECT().DeleteAckLevel(gomock.Any(), "stale").Return(wantErr)

	deletedMessageID, err := replicator.cleanupAckedMessages(testcontext.For(t), 5)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, int64(5), deletedMessageID)
}
