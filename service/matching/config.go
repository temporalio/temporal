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
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/dynamicconfig"
)

type (
	// Config represents configuration for matching service
	Config struct {
		PersistenceMaxQPS       dynamicconfig.IntPropertyFn
		PersistenceGlobalMaxQPS dynamicconfig.IntPropertyFn
		SyncMatchWaitDuration   dynamicconfig.DurationPropertyFnWithTaskQueueInfoFilters
		RPS                     dynamicconfig.IntPropertyFn
		ShutdownDrainDuration   dynamicconfig.DurationPropertyFn

		// taskQueueManager configuration

		RangeSize                    int64
		GetTasksBatchSize            dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		UpdateAckInterval            dynamicconfig.DurationPropertyFnWithTaskQueueInfoFilters
		IdleTaskqueueCheckInterval   dynamicconfig.DurationPropertyFn
		MaxTaskqueueIdleTime         dynamicconfig.DurationPropertyFnWithTaskQueueInfoFilters
		NumTaskqueueWritePartitions  dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		NumTaskqueueReadPartitions   dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		ForwarderMaxOutstandingPolls dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		ForwarderMaxOutstandingTasks dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		ForwarderMaxRatePerSecond    dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		ForwarderMaxChildrenPerNode  dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		ResilientSyncMatch           dynamicconfig.BoolPropertyFn

		// Time to hold a poll request before returning an empty response if there are no tasks
		LongPollExpirationInterval dynamicconfig.DurationPropertyFnWithTaskQueueInfoFilters
		MinTaskThrottlingBurstSize dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		MaxTaskDeleteBatchSize     dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters

		// taskWriter configuration
		OutstandingTaskAppendsThreshold dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		MaxTaskBatchSize                dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters

		ThrottledLogRPS dynamicconfig.IntPropertyFn

		AdminNamespaceToPartitionDispatchRate          dynamicconfig.FloatPropertyFnWithNamespaceFilter
		AdminNamespaceTaskqueueToPartitionDispatchRate dynamicconfig.FloatPropertyFnWithTaskQueueInfoFilters
	}

	forwarderConfig struct {
		ForwarderMaxOutstandingPolls func() int
		ForwarderMaxOutstandingTasks func() int
		ForwarderMaxRatePerSecond    func() int
		ForwarderMaxChildrenPerNode  func() int
	}

	taskQueueConfig struct {
		forwarderConfig
		SyncMatchWaitDuration func() time.Duration
		// Time to hold a poll request before returning an empty response if there are no tasks
		LongPollExpirationInterval func() time.Duration
		RangeSize                  int64
		GetTasksBatchSize          func() int
		UpdateAckInterval          func() time.Duration
		MaxTaskqueueIdleTime       func() time.Duration
		MinTaskThrottlingBurstSize func() int
		MaxTaskDeleteBatchSize     func() int
		// taskWriter configuration
		OutstandingTaskAppendsThreshold func() int
		MaxTaskBatchSize                func() int
		NumWritePartitions              func() int
		NumReadPartitions               func() int

		// partition qps = AdminNamespaceToPartitionDispatchRate(namespace)
		AdminNamespaceToPartitionDispatchRate func() float64
		// partition qps = AdminNamespaceTaskQueueToPartitionDispatchRate(namespace, task_queue)
		AdminNamespaceTaskQueueToPartitionDispatchRate func() float64

		// ResilientSyncMatch enables or disables sync-matching while
		// persistence is unavailable
		ResilientSyncMatch func() bool
	}
)

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection) *Config {
	return &Config{
		PersistenceMaxQPS:               dc.GetIntProperty(dynamicconfig.MatchingPersistenceMaxQPS, 3000),
		PersistenceGlobalMaxQPS:         dc.GetIntProperty(dynamicconfig.MatchingPersistenceGlobalMaxQPS, 0),
		SyncMatchWaitDuration:           dc.GetDurationPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingSyncMatchWaitDuration, 200*time.Millisecond),
		RPS:                             dc.GetIntProperty(dynamicconfig.MatchingRPS, 1200),
		RangeSize:                       100000,
		GetTasksBatchSize:               dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingGetTasksBatchSize, 1000),
		UpdateAckInterval:               dc.GetDurationPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingUpdateAckInterval, 1*time.Minute),
		IdleTaskqueueCheckInterval:      dc.GetDurationProperty(dynamicconfig.MatchingIdleTaskqueueCheckInterval, 30*time.Second),
		MaxTaskqueueIdleTime:            dc.GetDurationPropertyFilteredByTaskQueueInfo(dynamicconfig.MaxTaskqueueIdleTime, 5*time.Minute),
		LongPollExpirationInterval:      dc.GetDurationPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingLongPollExpirationInterval, time.Minute),
		MinTaskThrottlingBurstSize:      dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingMinTaskThrottlingBurstSize, 1),
		MaxTaskDeleteBatchSize:          dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingMaxTaskDeleteBatchSize, 100),
		OutstandingTaskAppendsThreshold: dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingOutstandingTaskAppendsThreshold, 250),
		MaxTaskBatchSize:                dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingMaxTaskBatchSize, 100),
		ThrottledLogRPS:                 dc.GetIntProperty(dynamicconfig.MatchingThrottledLogRPS, 20),
		NumTaskqueueWritePartitions:     dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingNumTaskqueueWritePartitions, dynamicconfig.DefaultNumTaskQueuePartitions),
		NumTaskqueueReadPartitions:      dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingNumTaskqueueReadPartitions, dynamicconfig.DefaultNumTaskQueuePartitions),
		ForwarderMaxOutstandingPolls:    dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingForwarderMaxOutstandingPolls, 1),
		ForwarderMaxOutstandingTasks:    dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingForwarderMaxOutstandingTasks, 1),
		ForwarderMaxRatePerSecond:       dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingForwarderMaxRatePerSecond, 10),
		ForwarderMaxChildrenPerNode:     dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingForwarderMaxChildrenPerNode, 20),
		ResilientSyncMatch:              dc.GetBoolProperty(dynamicconfig.ResilientSyncMatch, false),
		ShutdownDrainDuration:           dc.GetDurationProperty(dynamicconfig.MatchingShutdownDrainDuration, 0),

		AdminNamespaceToPartitionDispatchRate:          dc.GetFloatPropertyFilteredByNamespace(dynamicconfig.AdminMatchingNamespaceToPartitionDispatchRate, 10000),
		AdminNamespaceTaskqueueToPartitionDispatchRate: dc.GetFloatPropertyFilteredByTaskQueueInfo(dynamicconfig.AdminMatchingNamespaceTaskqueueToPartitionDispatchRate, 1000),
	}
}

func newTaskQueueConfig(id *taskQueueID, config *Config, namespaceCache cache.NamespaceCache) (*taskQueueConfig, error) {
	namespaceEntry, err := namespaceCache.GetNamespaceByID(id.namespaceID)
	if err != nil {
		return nil, err
	}

	namespace := namespaceEntry.GetInfo().Name
	taskQueueName := id.name
	taskType := id.taskType

	writePartition := func() int {
		return common.MaxInt(1, config.NumTaskqueueWritePartitions(namespace, taskQueueName, taskType))
	}
	readPartition := func() int {
		return common.MaxInt(1, config.NumTaskqueueReadPartitions(namespace, taskQueueName, taskType))
	}

	return &taskQueueConfig{
		RangeSize: config.RangeSize,
		GetTasksBatchSize: func() int {
			return config.GetTasksBatchSize(namespace, taskQueueName, taskType)
		},
		UpdateAckInterval: func() time.Duration {
			return config.UpdateAckInterval(namespace, taskQueueName, taskType)
		},
		MaxTaskqueueIdleTime: func() time.Duration {
			return config.MaxTaskqueueIdleTime(namespace, taskQueueName, taskType)
		},
		MinTaskThrottlingBurstSize: func() int {
			return config.MinTaskThrottlingBurstSize(namespace, taskQueueName, taskType)
		},
		SyncMatchWaitDuration: func() time.Duration {
			return config.SyncMatchWaitDuration(namespace, taskQueueName, taskType)
		},
		LongPollExpirationInterval: func() time.Duration {
			return config.LongPollExpirationInterval(namespace, taskQueueName, taskType)
		},
		MaxTaskDeleteBatchSize: func() int {
			return config.MaxTaskDeleteBatchSize(namespace, taskQueueName, taskType)
		},
		OutstandingTaskAppendsThreshold: func() int {
			return config.OutstandingTaskAppendsThreshold(namespace, taskQueueName, taskType)
		},
		MaxTaskBatchSize: func() int {
			return config.MaxTaskBatchSize(namespace, taskQueueName, taskType)
		},
		NumWritePartitions: writePartition,
		NumReadPartitions:  readPartition,
		AdminNamespaceToPartitionDispatchRate: func() float64 {
			return config.AdminNamespaceToPartitionDispatchRate(namespace)
		},
		AdminNamespaceTaskQueueToPartitionDispatchRate: func() float64 {
			return config.AdminNamespaceTaskqueueToPartitionDispatchRate(namespace, taskQueueName, taskType)
		},
		forwarderConfig: forwarderConfig{
			ForwarderMaxOutstandingPolls: func() int {
				return config.ForwarderMaxOutstandingPolls(namespace, taskQueueName, taskType)
			},
			ForwarderMaxOutstandingTasks: func() int {
				return config.ForwarderMaxOutstandingTasks(namespace, taskQueueName, taskType)
			},
			ForwarderMaxRatePerSecond: func() int {
				return config.ForwarderMaxRatePerSecond(namespace, taskQueueName, taskType)
			},
			ForwarderMaxChildrenPerNode: func() int {
				return common.MaxInt(1, config.ForwarderMaxChildrenPerNode(namespace, taskQueueName, taskType))
			},
		},
		ResilientSyncMatch: func() bool {
			return config.ResilientSyncMatch()
		},
	}, nil
}
