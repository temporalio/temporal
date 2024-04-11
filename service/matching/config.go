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
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/tqid"
)

type (
	// Config represents configuration for matching service
	Config struct {
		PersistenceMaxQPS                     dynamicconfig.IntPropertyFn
		PersistenceGlobalMaxQPS               dynamicconfig.IntPropertyFn
		PersistenceNamespaceMaxQPS            dynamicconfig.IntPropertyFnWithNamespaceFilter
		PersistenceGlobalNamespaceMaxQPS      dynamicconfig.IntPropertyFnWithNamespaceFilter
		PersistencePerShardNamespaceMaxQPS    dynamicconfig.IntPropertyFnWithNamespaceFilter
		EnablePersistencePriorityRateLimiting dynamicconfig.BoolPropertyFn
		PersistenceDynamicRateLimitingParams  dynamicconfig.MapPropertyFn
		SyncMatchWaitDuration                 dynamicconfig.DurationPropertyFnWithTaskQueueInfoFilters
		TestDisableSyncMatch                  dynamicconfig.BoolPropertyFn
		RPS                                   dynamicconfig.IntPropertyFn
		OperatorRPSRatio                      dynamicconfig.FloatPropertyFn
		ShutdownDrainDuration                 dynamicconfig.DurationPropertyFn
		HistoryMaxPageSize                    dynamicconfig.IntPropertyFnWithNamespaceFilter

		// task queue configuration

		RangeSize                                int64
		GetTasksBatchSize                        dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		UpdateAckInterval                        dynamicconfig.DurationPropertyFnWithTaskQueueInfoFilters
		MaxTaskQueueIdleTime                     dynamicconfig.DurationPropertyFnWithTaskQueueInfoFilters
		NumTaskqueueWritePartitions              dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		NumTaskqueueReadPartitions               dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		ForwarderMaxOutstandingPolls             dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		ForwarderMaxOutstandingTasks             dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		ForwarderMaxRatePerSecond                dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		ForwarderMaxChildrenPerNode              dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		VersionCompatibleSetLimitPerQueue        dynamicconfig.IntPropertyFnWithNamespaceFilter
		VersionBuildIdLimitPerQueue              dynamicconfig.IntPropertyFnWithNamespaceFilter
		AssignmentRuleLimitPerQueue              dynamicconfig.IntPropertyFnWithNamespaceFilter
		RedirectRuleLimitPerQueue                dynamicconfig.IntPropertyFnWithNamespaceFilter
		RedirectRuleChainLimitPerQueue           dynamicconfig.IntPropertyFnWithNamespaceFilter
		DeletedRuleRetentionTime                 dynamicconfig.DurationPropertyFnWithNamespaceFilter
		ReachabilityBuildIdVisibilityGracePeriod dynamicconfig.DurationPropertyFnWithNamespaceFilter
		TaskQueueLimitPerBuildId                 dynamicconfig.IntPropertyFnWithNamespaceFilter
		GetUserDataLongPollTimeout               dynamicconfig.DurationPropertyFn
		BacklogNegligibleAge                     dynamicconfig.DurationPropertyFnWithTaskQueueInfoFilters
		MaxWaitForPollerBeforeFwd                dynamicconfig.DurationPropertyFnWithTaskQueueInfoFilters

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

		VisibilityPersistenceMaxReadQPS   dynamicconfig.IntPropertyFn
		VisibilityPersistenceMaxWriteQPS  dynamicconfig.IntPropertyFn
		EnableReadFromSecondaryVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter
		VisibilityDisableOrderByClause    dynamicconfig.BoolPropertyFnWithNamespaceFilter
		VisibilityEnableManualPagination  dynamicconfig.BoolPropertyFnWithNamespaceFilter

		LoadUserData dynamicconfig.BoolPropertyFnWithTaskQueueInfoFilters

		// FrontendAccessHistoryFraction is an interim flag across 2 minor releases and will be removed once fully enabled.
		FrontendAccessHistoryFraction dynamicconfig.FloatPropertyFn
	}

	forwarderConfig struct {
		ForwarderMaxOutstandingPolls func() int
		ForwarderMaxOutstandingTasks func() int
		ForwarderMaxRatePerSecond    func() int
		ForwarderMaxChildrenPerNode  func() int
	}

	taskQueueConfig struct {
		forwarderConfig
		SyncMatchWaitDuration     func() time.Duration
		BacklogNegligibleAge      func() time.Duration
		MaxWaitForPollerBeforeFwd func() time.Duration
		TestDisableSyncMatch      func() bool
		// Time to hold a poll request before returning an empty response if there are no tasks
		LongPollExpirationInterval func() time.Duration
		RangeSize                  int64
		GetTasksBatchSize          func() int
		UpdateAckInterval          func() time.Duration
		MaxTaskQueueIdleTime       func() time.Duration
		MinTaskThrottlingBurstSize func() int
		MaxTaskDeleteBatchSize     func() int

		GetUserDataLongPollTimeout dynamicconfig.DurationPropertyFn
		GetUserDataMinWaitTime     time.Duration

		// taskWriter configuration
		OutstandingTaskAppendsThreshold func() int
		MaxTaskBatchSize                func() int
		NumWritePartitions              func() int
		NumReadPartitions               func() int

		// partition qps = AdminNamespaceToPartitionDispatchRate(namespace)
		AdminNamespaceToPartitionDispatchRate func() float64
		// partition qps = AdminNamespaceTaskQueueToPartitionDispatchRate(namespace, task_queue)
		AdminNamespaceTaskQueueToPartitionDispatchRate func() float64

		// Retry policy for fetching user data from root partition. Should retry forever.
		GetUserDataRetryPolicy backoff.RetryPolicy
	}
)

// NewConfig returns new service config with default values
func NewConfig(
	dc *dynamicconfig.Collection,
	visibilityStoreConfigExist bool,
	enableReadFromES bool,
) *Config {
	defaultUpdateAckInterval := []dynamicconfig.ConstrainedValue{
		// Use a longer default interval for the per-namespace internal worker queues.
		{
			Constraints: dynamicconfig.Constraints{
				TaskQueueName: primitives.PerNSWorkerTaskQueue,
			},
			Value: 5 * time.Minute,
		},
		// Default for everything else.
		{
			Value: 1 * time.Minute,
		},
	}
	return &Config{
		PersistenceMaxQPS:                        dc.GetIntProperty(dynamicconfig.MatchingPersistenceMaxQPS, 3000),
		PersistenceGlobalMaxQPS:                  dc.GetIntProperty(dynamicconfig.MatchingPersistenceGlobalMaxQPS, 0),
		PersistenceNamespaceMaxQPS:               dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MatchingPersistenceNamespaceMaxQPS, 0),
		PersistenceGlobalNamespaceMaxQPS:         dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MatchingPersistenceGlobalNamespaceMaxQPS, 0),
		PersistencePerShardNamespaceMaxQPS:       dynamicconfig.DefaultPerShardNamespaceRPSMax,
		EnablePersistencePriorityRateLimiting:    dc.GetBoolProperty(dynamicconfig.MatchingEnablePersistencePriorityRateLimiting, true),
		PersistenceDynamicRateLimitingParams:     dc.GetMapProperty(dynamicconfig.MatchingPersistenceDynamicRateLimitingParams, dynamicconfig.DefaultDynamicRateLimitingParams),
		SyncMatchWaitDuration:                    dc.GetDurationPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingSyncMatchWaitDuration, 200*time.Millisecond),
		TestDisableSyncMatch:                     dc.GetBoolProperty(dynamicconfig.TestMatchingDisableSyncMatch, false),
		LoadUserData:                             dc.GetBoolPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingLoadUserData, true),
		HistoryMaxPageSize:                       dc.GetIntPropertyFilteredByNamespace(dynamicconfig.MatchingHistoryMaxPageSize, common.GetHistoryMaxPageSize),
		RPS:                                      dc.GetIntProperty(dynamicconfig.MatchingRPS, 1200),
		OperatorRPSRatio:                         dc.GetFloat64Property(dynamicconfig.OperatorRPSRatio, common.DefaultOperatorRPSRatio),
		RangeSize:                                100000,
		GetTasksBatchSize:                        dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingGetTasksBatchSize, 1000),
		UpdateAckInterval:                        dc.GetDurationPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingUpdateAckInterval, defaultUpdateAckInterval),
		MaxTaskQueueIdleTime:                     dc.GetDurationPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingMaxTaskQueueIdleTime, 5*time.Minute),
		LongPollExpirationInterval:               dc.GetDurationPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingLongPollExpirationInterval, time.Minute),
		MinTaskThrottlingBurstSize:               dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingMinTaskThrottlingBurstSize, 1),
		MaxTaskDeleteBatchSize:                   dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingMaxTaskDeleteBatchSize, 100),
		OutstandingTaskAppendsThreshold:          dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingOutstandingTaskAppendsThreshold, 250),
		MaxTaskBatchSize:                         dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingMaxTaskBatchSize, 100),
		ThrottledLogRPS:                          dc.GetIntProperty(dynamicconfig.MatchingThrottledLogRPS, 20),
		NumTaskqueueWritePartitions:              dc.GetTaskQueuePartitionsProperty(dynamicconfig.MatchingNumTaskqueueWritePartitions),
		NumTaskqueueReadPartitions:               dc.GetTaskQueuePartitionsProperty(dynamicconfig.MatchingNumTaskqueueReadPartitions),
		ForwarderMaxOutstandingPolls:             dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingForwarderMaxOutstandingPolls, 1),
		ForwarderMaxOutstandingTasks:             dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingForwarderMaxOutstandingTasks, 1),
		ForwarderMaxRatePerSecond:                dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingForwarderMaxRatePerSecond, 10),
		ForwarderMaxChildrenPerNode:              dc.GetIntPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingForwarderMaxChildrenPerNode, 20),
		ShutdownDrainDuration:                    dc.GetDurationProperty(dynamicconfig.MatchingShutdownDrainDuration, 0*time.Second),
		VersionCompatibleSetLimitPerQueue:        dc.GetIntPropertyFilteredByNamespace(dynamicconfig.VersionCompatibleSetLimitPerQueue, 10),
		VersionBuildIdLimitPerQueue:              dc.GetIntPropertyFilteredByNamespace(dynamicconfig.VersionBuildIdLimitPerQueue, 100),
		AssignmentRuleLimitPerQueue:              dc.GetIntPropertyFilteredByNamespace(dynamicconfig.AssignmentRuleLimitPerQueue, 100),
		RedirectRuleLimitPerQueue:                dc.GetIntPropertyFilteredByNamespace(dynamicconfig.RedirectRuleLimitPerQueue, 500),
		RedirectRuleChainLimitPerQueue:           dc.GetIntPropertyFilteredByNamespace(dynamicconfig.RedirectRuleChainLimitPerQueue, 50),
		DeletedRuleRetentionTime:                 dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.MatchingDeletedRuleRetentionTime, 14*24*time.Hour),
		ReachabilityBuildIdVisibilityGracePeriod: dc.GetDurationPropertyFilteredByNamespace(dynamicconfig.ReachabilityBuildIdVisibilityGracePeriod, 3*time.Minute),
		TaskQueueLimitPerBuildId:                 dc.GetIntPropertyFilteredByNamespace(dynamicconfig.TaskQueuesPerBuildIdLimit, 20),
		GetUserDataLongPollTimeout:               dc.GetDurationProperty(dynamicconfig.MatchingGetUserDataLongPollTimeout, 5*time.Minute-10*time.Second),
		BacklogNegligibleAge:                     dc.GetDurationPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingBacklogNegligibleAge, 24*365*10*time.Hour),
		MaxWaitForPollerBeforeFwd:                dc.GetDurationPropertyFilteredByTaskQueueInfo(dynamicconfig.MatchingMaxWaitForPollerBeforeFwd, 200*time.Millisecond),

		AdminNamespaceToPartitionDispatchRate:          dc.GetFloatPropertyFilteredByNamespace(dynamicconfig.AdminMatchingNamespaceToPartitionDispatchRate, 10000),
		AdminNamespaceTaskqueueToPartitionDispatchRate: dc.GetFloatPropertyFilteredByTaskQueueInfo(dynamicconfig.AdminMatchingNamespaceTaskqueueToPartitionDispatchRate, 1000),

		VisibilityPersistenceMaxReadQPS:   visibility.GetVisibilityPersistenceMaxReadQPS(dc, enableReadFromES),
		VisibilityPersistenceMaxWriteQPS:  visibility.GetVisibilityPersistenceMaxWriteQPS(dc, enableReadFromES),
		EnableReadFromSecondaryVisibility: visibility.GetEnableReadFromSecondaryVisibilityConfig(dc, visibilityStoreConfigExist, enableReadFromES),
		VisibilityDisableOrderByClause:    dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityDisableOrderByClause, true),
		VisibilityEnableManualPagination:  dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.VisibilityEnableManualPagination, true),

		FrontendAccessHistoryFraction: dc.GetFloat64Property(dynamicconfig.FrontendAccessHistoryFraction, 0.0),
	}
}

func newTaskQueueConfig(tq *tqid.TaskQueue, config *Config, ns namespace.Name) *taskQueueConfig {
	taskQueueName := tq.Name()
	taskType := tq.TaskType()

	return &taskQueueConfig{
		RangeSize: config.RangeSize,
		GetTasksBatchSize: func() int {
			return config.GetTasksBatchSize(ns.String(), taskQueueName, taskType)
		},
		UpdateAckInterval: func() time.Duration {
			return config.UpdateAckInterval(ns.String(), taskQueueName, taskType)
		},
		MaxTaskQueueIdleTime: func() time.Duration {
			return config.MaxTaskQueueIdleTime(ns.String(), taskQueueName, taskType)
		},
		MinTaskThrottlingBurstSize: func() int {
			return config.MinTaskThrottlingBurstSize(ns.String(), taskQueueName, taskType)
		},
		SyncMatchWaitDuration: func() time.Duration {
			return config.SyncMatchWaitDuration(ns.String(), taskQueueName, taskType)
		},
		BacklogNegligibleAge: func() time.Duration {
			return config.BacklogNegligibleAge(ns.String(), taskQueueName, taskType)
		},
		MaxWaitForPollerBeforeFwd: func() time.Duration {
			return config.MaxWaitForPollerBeforeFwd(ns.String(), taskQueueName, taskType)
		},
		TestDisableSyncMatch: config.TestDisableSyncMatch,
		LongPollExpirationInterval: func() time.Duration {
			return config.LongPollExpirationInterval(ns.String(), taskQueueName, taskType)
		},
		MaxTaskDeleteBatchSize: func() int {
			return config.MaxTaskDeleteBatchSize(ns.String(), taskQueueName, taskType)
		},
		GetUserDataLongPollTimeout: config.GetUserDataLongPollTimeout,
		GetUserDataMinWaitTime:     1 * time.Second,
		OutstandingTaskAppendsThreshold: func() int {
			return config.OutstandingTaskAppendsThreshold(ns.String(), taskQueueName, taskType)
		},
		MaxTaskBatchSize: func() int {
			return config.MaxTaskBatchSize(ns.String(), taskQueueName, taskType)
		},
		NumWritePartitions: func() int {
			return max(1, config.NumTaskqueueWritePartitions(ns.String(), taskQueueName, taskType))
		},
		NumReadPartitions: func() int {
			return max(1, config.NumTaskqueueReadPartitions(ns.String(), taskQueueName, taskType))
		},
		AdminNamespaceToPartitionDispatchRate: func() float64 {
			return config.AdminNamespaceToPartitionDispatchRate(ns.String())
		},
		AdminNamespaceTaskQueueToPartitionDispatchRate: func() float64 {
			return config.AdminNamespaceTaskqueueToPartitionDispatchRate(ns.String(), taskQueueName, taskType)
		},
		forwarderConfig: forwarderConfig{
			ForwarderMaxOutstandingPolls: func() int {
				return config.ForwarderMaxOutstandingPolls(ns.String(), taskQueueName, taskType)
			},
			ForwarderMaxOutstandingTasks: func() int {
				return config.ForwarderMaxOutstandingTasks(ns.String(), taskQueueName, taskType)
			},
			ForwarderMaxRatePerSecond: func() int {
				return config.ForwarderMaxRatePerSecond(ns.String(), taskQueueName, taskType)
			},
			ForwarderMaxChildrenPerNode: func() int {
				return max(1, config.ForwarderMaxChildrenPerNode(ns.String(), taskQueueName, taskType))
			},
		},
		GetUserDataRetryPolicy: backoff.NewExponentialRetryPolicy(1 * time.Second).WithMaximumInterval(5 * time.Minute),
	}
}
