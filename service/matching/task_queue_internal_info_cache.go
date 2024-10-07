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
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/tqid"
)

const (
	taskQueueInternalInfoCacheMaxSize = 10000
)

/*
In-memory Cache for storing task queue child partitions' internal info (Stats, Pollers)
Stores key-value pairs as: PartitionKey -> physicalInfoByBuildId
*/

type taskQueueInternalInfoCache struct {
	cache cache.Cache
}

func newTaskQueueInternalInfoCache(opts *cache.Options) taskQueueInternalInfoCache {
	return taskQueueInternalInfoCache{
		cache: cache.New(taskQueueInternalInfoCacheMaxSize, opts),
	}
}

// Get retrieves a map containing PhysicalTaskQueueInfo per task queue type, for each buildID
// NOTE: Should only be called by the root Partition
func (tqCache *taskQueueInternalInfoCache) Get(partitionKey tqid.PartitionKey) map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo {
	stored := tqCache.cache.Get(partitionKey)

	// Type assert the stored value to the expected map type
	if physicalInfo, ok := stored.(map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo); ok {
		return physicalInfo
	}
	return nil
}

// Put updates the PhysicalTaskQueueInfo per task queue type, for each buildID
// NOTE: Should only be called by the root Partition
func (tqCache *taskQueueInternalInfoCache) Put(partitionKey tqid.PartitionKey, physicalInfoByBuildId map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo) {
	tqCache.cache.Put(partitionKey, physicalInfoByBuildId)
	return
}
