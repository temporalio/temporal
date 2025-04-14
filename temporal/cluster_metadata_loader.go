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

package temporal

import (
	"context"
	"fmt"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
)

// ClusterMetadataLoader loads cluster metadata from the database and merges it with the static config.
// TODO: move this to the [cluster] package. It is here temporarily to avoid a circular dependency.
type ClusterMetadataLoader struct {
	manager persistence.ClusterMetadataManager
	logger  log.Logger
}

// NewClusterMetadataLoader creates a new [ClusterMetadataLoader] that loads cluster metadata from the database.
func NewClusterMetadataLoader(manager persistence.ClusterMetadataManager, logger log.Logger) *ClusterMetadataLoader {
	return &ClusterMetadataLoader{
		manager: manager,
		logger:  logger,
	}
}

// LoadAndMergeWithStaticConfig loads cluster metadata from the database and merges it with the static config.
func (c *ClusterMetadataLoader) LoadAndMergeWithStaticConfig(ctx context.Context, svc *config.Config) error {
	iter := cluster.GetAllClustersIter(ctx, c.manager)

	for iter.HasNext() {
		item, err := iter.Next()
		if err != nil {
			return err
		}
		newMetadata := cluster.ClusterInformationFromDB(item)
		c.mergeMetadataFromDBWithStaticConfig(svc, item.ClusterName, newMetadata)
	}
	return nil
}

func (c *ClusterMetadataLoader) mergeMetadataFromDBWithStaticConfig(svc *config.Config, clusterName string, newMetadata *cluster.ClusterInformation) {
	c.backfillShardCount(svc, newMetadata)
	if currentMetadata, ok := svc.ClusterMetadata.ClusterInformation[clusterName]; ok {
		c.reconcileMetadata(svc, clusterName, currentMetadata, newMetadata)
	}
	svc.ClusterMetadata.ClusterInformation[clusterName] = *newMetadata
}

// reconcileMetadata merges the current metadata with the new metadata, modifying the new metadata in place.
func (c *ClusterMetadataLoader) reconcileMetadata(
	svc *config.Config,
	clusterName string,
	currentMetadata cluster.ClusterInformation,
	newMetadata *cluster.ClusterInformation,
) {
	if clusterName != svc.ClusterMetadata.CurrentClusterName {
		c.logger.Warn(
			"ClusterInformation in static config is deprecated. Please use TCTL tool to configure remote cluster connections",
			tag.Key("clusterInformation"),
			tag.IgnoredValue(currentMetadata),
			tag.Value(newMetadata))
		return
	}
	newMetadata.RPCAddress = currentMetadata.RPCAddress
	c.logger.Info(fmt.Sprintf("Use rpc address %v for cluster %v.", newMetadata.RPCAddress, clusterName))
}

// backfillShardCount is to add backward compatibility to the svc based cluster connection. It sets the shard count for
// newMetadata to the number of shards in the current cluster, if the shard count is not set in the database.
func (c *ClusterMetadataLoader) backfillShardCount(svc *config.Config, newMetadata *cluster.ClusterInformation) {
	if newMetadata.ShardCount == 0 {
		newMetadata.ShardCount = svc.Persistence.NumHistoryShards
	}
}
