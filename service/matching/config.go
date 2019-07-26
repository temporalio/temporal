// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	// Config represents configuration for cadence-matching service
	Config struct {
		PersistenceMaxQPS dynamicconfig.IntPropertyFn
		EnableSyncMatch   dynamicconfig.BoolPropertyFnWithTaskListInfoFilters
		RPS               dynamicconfig.IntPropertyFn

		// taskListManager configuration
		RangeSize                    int64
		GetTasksBatchSize            dynamicconfig.IntPropertyFnWithTaskListInfoFilters
		UpdateAckInterval            dynamicconfig.DurationPropertyFnWithTaskListInfoFilters
		IdleTasklistCheckInterval    dynamicconfig.DurationPropertyFnWithTaskListInfoFilters
		MaxTasklistIdleTime          dynamicconfig.DurationPropertyFnWithTaskListInfoFilters
		NumTasklistWritePartitions   dynamicconfig.IntPropertyFnWithTaskListInfoFilters
		NumTasklistReadPartitions    dynamicconfig.IntPropertyFnWithTaskListInfoFilters
		ForwarderMaxOutstandingPolls dynamicconfig.IntPropertyFnWithTaskListInfoFilters
		ForwarderMaxOutstandingTasks dynamicconfig.IntPropertyFnWithTaskListInfoFilters
		ForwarderMaxRatePerSecond    dynamicconfig.IntPropertyFnWithTaskListInfoFilters
		ForwarderMaxChildrenPerNode  dynamicconfig.IntPropertyFnWithTaskListInfoFilters

		// Time to hold a poll request before returning an empty response if there are no tasks
		LongPollExpirationInterval dynamicconfig.DurationPropertyFnWithTaskListInfoFilters
		MinTaskThrottlingBurstSize dynamicconfig.IntPropertyFnWithTaskListInfoFilters
		MaxTaskDeleteBatchSize     dynamicconfig.IntPropertyFnWithTaskListInfoFilters

		// taskWriter configuration
		OutstandingTaskAppendsThreshold dynamicconfig.IntPropertyFnWithTaskListInfoFilters
		MaxTaskBatchSize                dynamicconfig.IntPropertyFnWithTaskListInfoFilters

		ThrottledLogRPS dynamicconfig.IntPropertyFn
	}

	forwarderConfig struct {
		ForwarderMaxOutstandingPolls func() int
		ForwarderMaxOutstandingTasks func() int
		ForwarderMaxRatePerSecond    func() int
		ForwarderMaxChildrenPerNode  func() int
	}

	taskListConfig struct {
		forwarderConfig
		EnableSyncMatch func() bool
		// Time to hold a poll request before returning an empty response if there are no tasks
		LongPollExpirationInterval func() time.Duration
		RangeSize                  int64
		GetTasksBatchSize          func() int
		UpdateAckInterval          func() time.Duration
		IdleTasklistCheckInterval  func() time.Duration
		MaxTasklistIdleTime        func() time.Duration
		MinTaskThrottlingBurstSize func() int
		MaxTaskDeleteBatchSize     func() int
		// taskWriter configuration
		OutstandingTaskAppendsThreshold func() int
		MaxTaskBatchSize                func() int
		NumWritePartitions              func() int
		NumReadPartitions               func() int
	}
)

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection) *Config {
	return &Config{
		PersistenceMaxQPS:               dc.GetIntProperty(dynamicconfig.MatchingPersistenceMaxQPS, 3000),
		EnableSyncMatch:                 dc.GetBoolPropertyFilteredByTaskListInfo(dynamicconfig.MatchingEnableSyncMatch, true),
		RPS:                             dc.GetIntProperty(dynamicconfig.MatchingRPS, 1200),
		RangeSize:                       100000,
		GetTasksBatchSize:               dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingGetTasksBatchSize, 1000),
		UpdateAckInterval:               dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MatchingUpdateAckInterval, 1*time.Minute),
		IdleTasklistCheckInterval:       dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MatchingIdleTasklistCheckInterval, 5*time.Minute),
		MaxTasklistIdleTime:             dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MaxTasklistIdleTime, 5*time.Minute),
		LongPollExpirationInterval:      dc.GetDurationPropertyFilteredByTaskListInfo(dynamicconfig.MatchingLongPollExpirationInterval, time.Minute),
		MinTaskThrottlingBurstSize:      dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingMinTaskThrottlingBurstSize, 1),
		MaxTaskDeleteBatchSize:          dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingMaxTaskDeleteBatchSize, 100),
		OutstandingTaskAppendsThreshold: dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingOutstandingTaskAppendsThreshold, 250),
		MaxTaskBatchSize:                dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingMaxTaskBatchSize, 100),
		ThrottledLogRPS:                 dc.GetIntProperty(dynamicconfig.MatchingThrottledLogRPS, 20),
		NumTasklistWritePartitions:      dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingNumTasklistWritePartitions, 1),
		NumTasklistReadPartitions:       dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingNumTasklistReadPartitions, 1),
		ForwarderMaxOutstandingPolls:    dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingForwarderMaxOutstandingPolls, 1),
		ForwarderMaxOutstandingTasks:    dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingForwarderMaxOutstandingTasks, 1),
		ForwarderMaxRatePerSecond:       dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingForwarderMaxRatePerSecond, 10),
		ForwarderMaxChildrenPerNode:     dc.GetIntPropertyFilteredByTaskListInfo(dynamicconfig.MatchingForwarderMaxChildrenPerNode, 20),
	}
}

func newTaskListConfig(id *taskListID, config *Config, domainCache cache.DomainCache) (*taskListConfig, error) {
	domainEntry, err := domainCache.GetDomainByID(id.domainID)
	if err != nil {
		return nil, err
	}

	domain := domainEntry.GetInfo().Name
	taskListName := id.name
	taskType := id.taskType
	return &taskListConfig{
		RangeSize: config.RangeSize,
		GetTasksBatchSize: func() int {
			return config.GetTasksBatchSize(domain, taskListName, taskType)
		},
		UpdateAckInterval: func() time.Duration {
			return config.UpdateAckInterval(domain, taskListName, taskType)
		},
		IdleTasklistCheckInterval: func() time.Duration {
			return config.IdleTasklistCheckInterval(domain, taskListName, taskType)
		},
		MaxTasklistIdleTime: func() time.Duration {
			return config.MaxTasklistIdleTime(domain, taskListName, taskType)
		},
		MinTaskThrottlingBurstSize: func() int {
			return config.MinTaskThrottlingBurstSize(domain, taskListName, taskType)
		},
		EnableSyncMatch: func() bool {
			return config.EnableSyncMatch(domain, taskListName, taskType)
		},
		LongPollExpirationInterval: func() time.Duration {
			return config.LongPollExpirationInterval(domain, taskListName, taskType)
		},
		MaxTaskDeleteBatchSize: func() int {
			return config.MaxTaskDeleteBatchSize(domain, taskListName, taskType)
		},
		OutstandingTaskAppendsThreshold: func() int {
			return config.OutstandingTaskAppendsThreshold(domain, taskListName, taskType)
		},
		MaxTaskBatchSize: func() int {
			return config.MaxTaskBatchSize(domain, taskListName, taskType)
		},
		NumWritePartitions: func() int {
			return common.MaxInt(1, config.NumTasklistWritePartitions(domain, taskListName, taskType))
		},
		NumReadPartitions: func() int {
			return common.MaxInt(1, config.NumTasklistReadPartitions(domain, taskListName, taskType))
		},
		forwarderConfig: forwarderConfig{
			ForwarderMaxOutstandingPolls: func() int {
				return config.ForwarderMaxOutstandingPolls(domain, taskListName, taskType)
			},
			ForwarderMaxOutstandingTasks: func() int {
				return config.ForwarderMaxOutstandingTasks(domain, taskListName, taskType)
			},
			ForwarderMaxRatePerSecond: func() int {
				return config.ForwarderMaxRatePerSecond(domain, taskListName, taskType)
			},
			ForwarderMaxChildrenPerNode: func() int {
				return common.MaxInt(1, config.ForwarderMaxChildrenPerNode(domain, taskListName, taskType))
			},
		},
	}, nil
}
