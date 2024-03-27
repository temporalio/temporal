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
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
)

type ClusterMetadataLoader struct {
	manager persistence.ClusterMetadataManager
	logger  log.Logger
}

func NewClusterMetadataLoader(manager persistence.ClusterMetadataManager, logger log.Logger) *ClusterMetadataLoader {
	return &ClusterMetadataLoader{
		manager: manager,
		logger:  logger,
	}
}

// TODO: move this to cluster.fx
func (c *ClusterMetadataLoader) LoadClusterInformationFromStore(ctx context.Context, svc *config.Config) error {
	var iter collection.Iterator[*persistence.GetClusterMetadataResponse]
	iter = collection.NewPagingIterator(func(paginationToken []byte) ([]*persistence.GetClusterMetadataResponse, []byte, error) {
		request := &persistence.ListClusterMetadataRequest{
			PageSize:      100,
			NextPageToken: paginationToken,
		}
		resp, err := c.manager.ListClusterMetadata(ctx, request)
		if err != nil {
			return nil, nil, err
		}
		return resp.ClusterMetadata, resp.NextPageToken, nil
	})

	for iter.HasNext() {
		item, err := iter.Next()
		if err != nil {
			return err
		}
		c.merge(svc, item)
	}
	return nil
}

func (c *ClusterMetadataLoader) merge(svc *config.Config, metadata *persistence.GetClusterMetadataResponse) {
	shardCount := metadata.HistoryShardCount
	if shardCount == 0 {
		// This is to add backward compatibility to the svc based cluster connection.
		shardCount = svc.Persistence.NumHistoryShards
	}
	newMetadata := cluster.ClusterInformationFromDB(metadata)
	if staticClusterMetadata, ok := svc.ClusterMetadata.ClusterInformation[metadata.ClusterName]; ok {
		if metadata.ClusterName != svc.ClusterMetadata.CurrentClusterName {
			c.logger.Warn(
				"ClusterInformation in ClusterMetadata svc is deprecated. Please use TCTL tool to configure remote cluster connections",
				tag.Key("clusterInformation"),
				tag.IgnoredValue(staticClusterMetadata),
				tag.Value(newMetadata))
		} else {
			newMetadata.RPCAddress = staticClusterMetadata.RPCAddress
			c.logger.Info(fmt.Sprintf("Use rpc address %v for cluster %v.", newMetadata.RPCAddress, metadata.ClusterName))
			// The logic for HTTP addresses is different from that of RPC addresses because HTTP addresses are optional.
			if newMetadata.HTTPAddress == "" {
				// Only use HTTP address from static config if there isn't one in the database.
				newMetadata.HTTPAddress = staticClusterMetadata.HTTPAddress
			} else {
				// Otherwise, defer to the HTTP address in the database, and log a message that the static one is ignored.
				c.logger.Warn(fmt.Sprintf(
					"Static http address %v is ignored for cluster %v, using %v from database.",
					staticClusterMetadata.HTTPAddress, metadata.ClusterName, newMetadata.HTTPAddress,
				))
			}
		}
	}
	svc.ClusterMetadata.ClusterInformation[metadata.ClusterName] = *newMetadata
}
