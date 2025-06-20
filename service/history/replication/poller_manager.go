package replication

import (
	"errors"
	"fmt"

	"go.temporal.io/server/common/cluster"
)

type (
	pollerManager interface {
		getSourceClusterShardIDs(sourceClusterName string) ([]int32, error)
	}

	pollerManagerImpl struct {
		currentShardId  int32
		clusterMetadata cluster.Metadata
	}
)

var _ pollerManager = (*pollerManagerImpl)(nil)

func newPollerManager(
	currentShardId int32,
	clusterMetadata cluster.Metadata,
) *pollerManagerImpl {
	return &pollerManagerImpl{
		currentShardId:  currentShardId,
		clusterMetadata: clusterMetadata,
	}
}

func (p pollerManagerImpl) getSourceClusterShardIDs(sourceClusterName string) ([]int32, error) {
	currentCluster := p.clusterMetadata.GetCurrentClusterName()
	allClusters := p.clusterMetadata.GetAllClusterInfo()
	currentClusterInfo, ok := allClusters[currentCluster]
	if !ok {
		return nil, errors.New("cannot get current cluster info from cluster metadata cache")
	}
	remoteClusterInfo, ok := allClusters[sourceClusterName]
	if !ok {
		return nil, errors.New(fmt.Sprintf("cannot get source cluster %s info from cluster metadata cache", sourceClusterName))
	}

	// The remote shard count and local shard count must be multiples.
	large, small := remoteClusterInfo.ShardCount, currentClusterInfo.ShardCount
	if small > large {
		large, small = small, large
	}
	if large%small != 0 {
		return nil, errors.New(fmt.Sprintf("remote shard count %d and local shard count %d are not multiples.", remoteClusterInfo.ShardCount, currentClusterInfo.ShardCount))
	}
	return generateShardIDs(p.currentShardId, currentClusterInfo.ShardCount, remoteClusterInfo.ShardCount), nil
}

// NOTE generateShardIDs is different than common.MapShardID
// common.MapShardID guarantee to return the corresponding shard IDs for give shard ID
// this function however is only a helper function for polling & redirecting replication task,
func generateShardIDs(localShardId int32, localShardCount int32, remoteShardCount int32) []int32 {
	var pollingShards []int32
	if remoteShardCount <= localShardCount {
		if localShardId <= remoteShardCount {
			pollingShards = append(pollingShards, localShardId)
		}
		return pollingShards
	}
	// remoteShardCount > localShardCount, replication poller will poll from multiple remote shard.
	for i := localShardId; i <= remoteShardCount; i += localShardCount {
		pollingShards = append(pollingShards, i)
	}
	return pollingShards
}
