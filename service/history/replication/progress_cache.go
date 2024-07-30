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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination progress_cache_mock.go

package replication

import (
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	ProgressCache interface {
		Get(
			shardContext shard.Context,
			namespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			clusterID int32,
			eventVersionHistory *historyspb.VersionHistory,
		) (*ReplicationProgress, bool)
		Put(
			shardContext shard.Context,
			namespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			clusterID int32,
			eventVersionHistory *historyspb.VersionHistory,
			versionedTransition *persistencespb.VersionedTransition,
		) error
	}

	progressCacheImpl struct {
		cacheLock sync.RWMutex
		cache     cache.Cache
	}

	ReplicationProgress struct {
		versionedTransition      *persistencespb.VersionedTransition
		eventVersionHistoryItems []*historyspb.VersionHistoryItem
	}

	cacheItem struct {
		versionedTransition *persistencespb.VersionedTransition
		eventVersionHistory map[string][]*historyspb.VersionHistoryItem
	}

	Key struct {
		WorkflowKey definition.WorkflowKey
		ShardUUID   string
		ClusterID   int32
	}
)

func NewProgressCache(
	config *configs.Config,
	logger log.Logger,
	handler metrics.Handler,
) ProgressCache {
	maxSize := config.ReplicationProgressCacheMaxSize()
	opts := &cache.Options{
		TTL: config.ReplicationProgressCacheTTL(),
	}
	return &progressCacheImpl{
		cache: cache.NewWithMetrics(maxSize, opts, handler.WithTags(metrics.CacheTypeTag(metrics.MutableStateCacheTypeTagValue))),
	}
}

func (c *progressCacheImpl) Get(
	shardContext shard.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	clusterID int32,
	eventVersionHistory *historyspb.VersionHistory,
) (*ReplicationProgress, bool) {
	cacheKey := makeCacheKey(shardContext, namespaceID, execution, clusterID)
	c.cacheLock.RLock()
	defer c.cacheLock.RUnlock()

	item, cacheHit := c.cache.Get(cacheKey).(*cacheItem)
	if cacheHit {
		historyItems, cacheHit := item.eventVersionHistory[makeMapKey(eventVersionHistory)]
		if cacheHit {
			return &ReplicationProgress{
				versionedTransition:      item.versionedTransition,
				eventVersionHistoryItems: historyItems,
			}, true
		}
	}
	return nil, false
}

func (c *progressCacheImpl) Put(
	shardContext shard.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	clusterID int32,
	eventVersionHistory *historyspb.VersionHistory,
	versionedTransition *persistencespb.VersionedTransition,
) error {
	cacheKey := makeCacheKey(shardContext, namespaceID, execution, clusterID)
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	mapKey := makeMapKey(eventVersionHistory)
	item, cacheHit := c.cache.Get(cacheKey).(*cacheItem)
	if !cacheHit {
		item = &cacheItem{
			versionedTransition: versionedTransition,
			eventVersionHistory: map[string][]*historyspb.VersionHistoryItem{
				mapKey: versionhistory.CopyVersionHistory(eventVersionHistory).GetItems(),
			},
		}
		c.cache.Put(cacheKey, item)
		return nil
	}

	if workflow.CompareVersionedTransition(versionedTransition, item.versionedTransition) < 0 {
		return nil
	}
	item.versionedTransition = versionedTransition
	item.eventVersionHistory[mapKey] = versionhistory.CopyVersionHistory(eventVersionHistory).GetItems()
	return nil
}

func makeCacheKey(
	shardContext shard.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	clusterID int32,
) Key {
	return Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId()),
		ShardUUID:   shardContext.GetOwner(),
		ClusterID:   clusterID,
	}
}

func makeMapKey(eventVersionHistory *historyspb.VersionHistory) string {
	return string(eventVersionHistory.GetBranchToken())
}
