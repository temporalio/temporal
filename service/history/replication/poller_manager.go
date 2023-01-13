// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package replication

import (
	"fmt"

	"go.temporal.io/server/common/cluster"
)

type (
	pollerManager interface {
		getSourceClusterShardIDs(sourceClusterName string) []int32
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

func (p pollerManagerImpl) getSourceClusterShardIDs(sourceClusterName string) []int32 {
	currentCluster := p.clusterMetadata.GetCurrentClusterName()
	allClusters := p.clusterMetadata.GetAllClusterInfo()
	currentClusterInfo, ok := allClusters[currentCluster]
	if !ok {
		panic("Cannot get current cluster info from cluster metadata cache")
	}
	remoteClusterInfo, ok := allClusters[sourceClusterName]
	if !ok {
		panic(fmt.Sprintf("Cannot get source cluster %s info from cluster metadata cache", sourceClusterName))
	}
	return generateShardIDs(p.currentShardId, currentClusterInfo.ShardCount, remoteClusterInfo.ShardCount)
}

func generateShardIDs(localShardId int32, localShardCount int32, remoteShardCount int32) []int32 {
	var pollingShards []int32
	if remoteShardCount <= localShardCount {
		if localShardId <= remoteShardCount {
			pollingShards = append(pollingShards, localShardId)
		}
		return pollingShards
	}

	// remoteShardCount > localShardCount, replication poller will poll from multiple remote shard.
	// The remote shard count and local shard count must be multiples.
	if remoteShardCount%localShardCount != 0 {
		panic(fmt.Sprintf("Remote shard count %d and local shard count %d are not multiples.", remoteShardCount, localShardCount))
	}
	for i := localShardId; i <= remoteShardCount; i += localShardCount {
		pollingShards = append(pollingShards, i)
	}
	return pollingShards
}
