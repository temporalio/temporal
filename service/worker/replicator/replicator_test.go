package replicator

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

func TestCleanupAckedMessages_GetAckLevelsError(t *testing.T) {
	control := gomock.NewController(t)
	nsQueue := persistence.NewMockNamespaceReplicationQueue(control)
	clusterMetadata := cluster.NewMockMetadata(control)
	replicator := &Replicator{
		namespaceReplicationQueue: nsQueue,
		clusterMetadata:           clusterMetadata,
	}

	nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(nil, fmt.Errorf("test"))
	nsQueue.EXPECT().DeleteMessagesBefore(gomock.Any(), gomock.Any()).Times(0)
	_, err := replicator.cleanupAckedMessages(context.Background(), 1)

	assert.Error(t, err)
}

func TestCleanupAckedMessages_EmptyGetAckLevels(t *testing.T) {
	control := gomock.NewController(t)
	nsQueue := persistence.NewMockNamespaceReplicationQueue(control)
	clusterMetadata := cluster.NewMockMetadata(control)
	replicator := &Replicator{
		namespaceReplicationQueue: nsQueue,
		clusterMetadata:           clusterMetadata,
	}

	nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(nil, nil)
	nsQueue.EXPECT().DeleteMessagesBefore(gomock.Any(), int64(1)).Times(0)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(nil)
	mid, err := replicator.cleanupAckedMessages(context.Background(), 1)

	assert.Equal(t, int64(1), mid)
	assert.NoError(t, err)

}

func TestCleanupAckedMessages_Noop(t *testing.T) {
	control := gomock.NewController(t)
	nsQueue := persistence.NewMockNamespaceReplicationQueue(control)
	clusterMetadata := cluster.NewMockMetadata(control)
	replicator := &Replicator{
		namespaceReplicationQueue: nsQueue,
		clusterMetadata:           clusterMetadata,
	}

	nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(map[string]int64{
		"a": 2,
		"b": 3,
		"c": 5,
		"d": 10,
		"e": 1,
	}, nil)
	nsQueue.EXPECT().DeleteMessagesBefore(gomock.Any(), int64(1)).Times(0)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"b": {},
		"c": {},
		"d": {},
		"e": {},
	})
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("e").AnyTimes()
	mid, err := replicator.cleanupAckedMessages(context.Background(), 5)

	assert.Equal(t, int64(5), mid)
	assert.NoError(t, err)
}

func TestCleanupAckedMessages_DeleteMessage(t *testing.T) {
	control := gomock.NewController(t)
	nsQueue := persistence.NewMockNamespaceReplicationQueue(control)
	clusterMetadata := cluster.NewMockMetadata(control)
	replicator := &Replicator{
		namespaceReplicationQueue: nsQueue,
		clusterMetadata:           clusterMetadata,
	}

	nsQueue.EXPECT().GetAckLevels(gomock.Any()).Return(map[string]int64{
		"a": 2,
		"b": 3,
		"c": 5,
		"d": 10,
		"e": 1,
	}, nil)
	nsQueue.EXPECT().DeleteMessagesBefore(gomock.Any(), int64(10)).Times(1)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"d": {},
		"e": {},
	})
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("e").AnyTimes()
	mid, err := replicator.cleanupAckedMessages(context.Background(), 5)

	assert.Equal(t, int64(10), mid)
	assert.NoError(t, err)
}
