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

package matching

import (
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/tqid"
	"time"

	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/metrics"
)

const (
	taskQueueStatsCacheMaxSize = 10000
)

/*
In-memory Cache for storing task queue stats from multiple child partitions
*/

// todo Shivam - cache will have PartitionType -> stats
type taskQueueStatsCache struct {
	cache          cache.Cache
	metricsHandler metrics.Handler
}

func newTaskQueueStatsCache(handler metrics.Handler, taskQueueStatsCacheTTL time.Duration) taskQueueStatsCache {
	return taskQueueStatsCache{
		cache:          cache.New(taskQueueStatsCacheMaxSize, &cache.Options{TTL: taskQueueStatsCacheTTL}),
		metricsHandler: handler,
	}
}

// Get retrieves the backlog stats for the partition
// NOTE: Should only be called by the root Partition
func (tqCache *taskQueueStatsCache) Get() *taskqueuepb.TaskQueueStats {
	return tqCache.Get()
}

// Put updates the backlog stats for the partition
// NOTE: Should only be called by the root Partition
func (tqCache *taskQueueStatsCache) Put(partitionKey tqid.PartitionKey, taskQueueStats *taskqueuepb.TaskQueueStats) {
	tqCache.cache.Put(partitionKey, taskQueueStats)
	return
}
