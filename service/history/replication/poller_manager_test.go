package replication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
	"go.uber.org/mock/gomock"
)

func TestGetPollingShardIds(t *testing.T) {
	testCases := []struct {
		shardID          int32
		remoteShardCount int32
		localShardCount  int32
		expectedShardIDs []int32
	}{
		{
			1,
			4,
			4,
			[]int32{1},
		},
		{
			1,
			2,
			4,
			[]int32{1},
		},
		{
			3,
			2,
			4,
			nil,
		},
		{
			1,
			16,
			4,
			[]int32{1, 5, 9, 13},
		},
		{
			4,
			16,
			4,
			[]int32{4, 8, 12, 16},
		},
		{
			4,
			17,
			4,
			[]int32{4, 8, 12, 16},
		},
		{
			1,
			17,
			4,
			[]int32{1, 5, 9, 13, 17},
		},
	}
	for idx, tt := range testCases {
		t.Run(fmt.Sprintf("Testcase %d", idx), func(t *testing.T) {
			shardIDs := generateShardIDs(tt.shardID, tt.localShardCount, tt.remoteShardCount)
			assert.Equal(t, tt.expectedShardIDs, shardIDs)
		})
	}
}

const (
	pollerMgrCurrentCluster = "active"
	pollerMgrSourceCluster  = "standby"
)

func pollerMgrNewMetadata(t *testing.T, info map[string]cluster.ClusterInformation) *cluster.MockMetadata {
	ctrl := gomock.NewController(t)
	md := cluster.NewMockMetadata(ctrl)
	md.EXPECT().GetCurrentClusterName().Return(pollerMgrCurrentCluster).AnyTimes()
	md.EXPECT().GetAllClusterInfo().Return(info).AnyTimes()
	return md
}

func TestNewPollerManager(t *testing.T) {
	md := pollerMgrNewMetadata(t, cluster.TestAllClusterInfo)
	p := newPollerManager(1, md)
	require.NotNil(t, p)
	require.Equal(t, int32(1), p.currentShardId)
}

func TestGetSourceClusterShardIDs_Success(t *testing.T) {
	info := map[string]cluster.ClusterInformation{
		pollerMgrCurrentCluster: {ShardCount: 4},
		pollerMgrSourceCluster:  {ShardCount: 16},
	}
	md := pollerMgrNewMetadata(t, info)
	p := newPollerManager(1, md)
	ids, err := p.getSourceClusterShardIDs(pollerMgrSourceCluster)
	require.NoError(t, err)
	require.Equal(t, []int32{1, 5, 9, 13}, ids)
}

func TestGetSourceClusterShardIDs_CurrentClusterMissing(t *testing.T) {
	info := map[string]cluster.ClusterInformation{
		pollerMgrSourceCluster: {ShardCount: 16},
	}
	md := pollerMgrNewMetadata(t, info)
	p := newPollerManager(1, md)
	ids, err := p.getSourceClusterShardIDs(pollerMgrSourceCluster)
	require.Error(t, err)
	require.Nil(t, ids)
}

func TestGetSourceClusterShardIDs_SourceClusterMissing(t *testing.T) {
	info := map[string]cluster.ClusterInformation{
		pollerMgrCurrentCluster: {ShardCount: 4},
	}
	md := pollerMgrNewMetadata(t, info)
	p := newPollerManager(1, md)
	ids, err := p.getSourceClusterShardIDs(pollerMgrSourceCluster)
	require.Error(t, err)
	require.Nil(t, ids)
}

func TestGetSourceClusterShardIDs_NotMultiples(t *testing.T) {
	info := map[string]cluster.ClusterInformation{
		pollerMgrCurrentCluster: {ShardCount: 4},
		pollerMgrSourceCluster:  {ShardCount: 6},
	}
	md := pollerMgrNewMetadata(t, info)
	p := newPollerManager(1, md)
	ids, err := p.getSourceClusterShardIDs(pollerMgrSourceCluster)
	require.Error(t, err)
	require.Nil(t, ids)
}
