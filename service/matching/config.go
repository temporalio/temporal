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

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/tqid"
)

type (
	// Config represents configuration for matching service
	Config struct {
		PersistenceMaxQPS                    dynamicconfig.IntPropertyFn
		PersistenceGlobalMaxQPS              dynamicconfig.IntPropertyFn
		PersistenceNamespaceMaxQPS           dynamicconfig.IntPropertyFnWithNamespaceFilter
		PersistenceGlobalNamespaceMaxQPS     dynamicconfig.IntPropertyFnWithNamespaceFilter
		PersistencePerShardNamespaceMaxQPS   dynamicconfig.IntPropertyFnWithNamespaceFilter
		PersistenceDynamicRateLimitingParams dynamicconfig.MapPropertyFn
		PersistenceQPSBurstRatio             dynamicconfig.FloatPropertyFn
		SyncMatchWaitDuration                dynamicconfig.DurationPropertyFnWithTaskQueueFilter
		TestDisableSyncMatch                 dynamicconfig.BoolPropertyFn
		RPS                                  dynamicconfig.IntPropertyFn
		OperatorRPSRatio                     dynamicconfig.FloatPropertyFn
		AlignMembershipChange                dynamicconfig.DurationPropertyFn
		ShutdownDrainDuration                dynamicconfig.DurationPropertyFn
		HistoryMaxPageSize                   dynamicconfig.IntPropertyFnWithNamespaceFilter

		// task queue configuration

		RangeSize                                int64
		GetTasksBatchSize                        dynamicconfig.IntPropertyFnWithTaskQueueFilter
		UpdateAckInterval                        dynamicconfig.DurationPropertyFnWithTaskQueueFilter
		MaxTaskQueueIdleTime                     dynamicconfig.DurationPropertyFnWithTaskQueueFilter
		NumTaskqueueWritePartitions              dynamicconfig.IntPropertyFnWithTaskQueueFilter
		NumTaskqueueReadPartitions               dynamicconfig.IntPropertyFnWithTaskQueueFilter
		ForwarderMaxOutstandingPolls             dynamicconfig.IntPropertyFnWithTaskQueueFilter
		ForwarderMaxOutstandingTasks             dynamicconfig.IntPropertyFnWithTaskQueueFilter
		ForwarderMaxRatePerSecond                dynamicconfig.IntPropertyFnWithTaskQueueFilter
		ForwarderMaxChildrenPerNode              dynamicconfig.IntPropertyFnWithTaskQueueFilter
		VersionCompatibleSetLimitPerQueue        dynamicconfig.IntPropertyFnWithNamespaceFilter
		VersionBuildIdLimitPerQueue              dynamicconfig.IntPropertyFnWithNamespaceFilter
		AssignmentRuleLimitPerQueue              dynamicconfig.IntPropertyFnWithNamespaceFilter
		RedirectRuleLimitPerQueue                dynamicconfig.IntPropertyFnWithNamespaceFilter
		RedirectRuleMaxUpstreamBuildIDsPerQueue  dynamicconfig.IntPropertyFnWithNamespaceFilter
		DeletedRuleRetentionTime                 dynamicconfig.DurationPropertyFnWithNamespaceFilter
		ReachabilityBuildIdVisibilityGracePeriod dynamicconfig.DurationPropertyFnWithNamespaceFilter
		ReachabilityCacheOpenWFsTTL              dynamicconfig.DurationPropertyFn
		ReachabilityCacheClosedWFsTTL            dynamicconfig.DurationPropertyFn
		TaskQueueLimitPerBuildId                 dynamicconfig.IntPropertyFnWithNamespaceFilter
		GetUserDataLongPollTimeout               dynamicconfig.DurationPropertyFn
		BacklogNegligibleAge                     dynamicconfig.DurationPropertyFnWithTaskQueueFilter
		MaxWaitForPollerBeforeFwd                dynamicconfig.DurationPropertyFnWithTaskQueueFilter
		QueryPollerUnavailableWindow             dynamicconfig.DurationPropertyFn
		QueryWorkflowTaskTimeoutLogRate          dynamicconfig.FloatPropertyFnWithTaskQueueFilter
		MembershipUnloadDelay                    dynamicconfig.DurationPropertyFn

		// Time to hold a poll request before returning an empty response if there are no tasks
		LongPollExpirationInterval dynamicconfig.DurationPropertyFnWithTaskQueueFilter
		MinTaskThrottlingBurstSize dynamicconfig.IntPropertyFnWithTaskQueueFilter
		MaxTaskDeleteBatchSize     dynamicconfig.IntPropertyFnWithTaskQueueFilter

		// taskWriter configuration
		OutstandingTaskAppendsThreshold dynamicconfig.IntPropertyFnWithTaskQueueFilter
		MaxTaskBatchSize                dynamicconfig.IntPropertyFnWithTaskQueueFilter

		ThrottledLogRPS dynamicconfig.IntPropertyFn

		AdminNamespaceToPartitionDispatchRate          dynamicconfig.FloatPropertyFnWithNamespaceFilter
		AdminNamespaceTaskqueueToPartitionDispatchRate dynamicconfig.FloatPropertyFnWithTaskQueueFilter

		VisibilityPersistenceMaxReadQPS   dynamicconfig.IntPropertyFn
		VisibilityPersistenceMaxWriteQPS  dynamicconfig.IntPropertyFn
		EnableReadFromSecondaryVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter
		VisibilityDisableOrderByClause    dynamicconfig.BoolPropertyFnWithNamespaceFilter
		VisibilityEnableManualPagination  dynamicconfig.BoolPropertyFnWithNamespaceFilter

		LoadUserData dynamicconfig.BoolPropertyFnWithTaskQueueFilter

		ListNexusEndpointsLongPollTimeout dynamicconfig.DurationPropertyFn

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
		SyncMatchWaitDuration        func() time.Duration
		BacklogNegligibleAge         func() time.Duration
		MaxWaitForPollerBeforeFwd    func() time.Duration
		QueryPollerUnavailableWindow func() time.Duration
		TestDisableSyncMatch         func() bool
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
) *Config {
	return &Config{
		PersistenceMaxQPS:                        dynamicconfig.MatchingPersistenceMaxQPS.Get(dc),
		PersistenceGlobalMaxQPS:                  dynamicconfig.MatchingPersistenceGlobalMaxQPS.Get(dc),
		PersistenceNamespaceMaxQPS:               dynamicconfig.MatchingPersistenceNamespaceMaxQPS.Get(dc),
		PersistenceGlobalNamespaceMaxQPS:         dynamicconfig.MatchingPersistenceGlobalNamespaceMaxQPS.Get(dc),
		PersistencePerShardNamespaceMaxQPS:       dynamicconfig.DefaultPerShardNamespaceRPSMax,
		PersistenceDynamicRateLimitingParams:     dynamicconfig.MatchingPersistenceDynamicRateLimitingParams.Get(dc),
		PersistenceQPSBurstRatio:                 dynamicconfig.PersistenceQPSBurstRatio.Get(dc),
		SyncMatchWaitDuration:                    dynamicconfig.MatchingSyncMatchWaitDuration.Get(dc),
		TestDisableSyncMatch:                     dynamicconfig.TestMatchingDisableSyncMatch.Get(dc),
		LoadUserData:                             dynamicconfig.MatchingLoadUserData.Get(dc),
		HistoryMaxPageSize:                       dynamicconfig.MatchingHistoryMaxPageSize.Get(dc),
		RPS:                                      dynamicconfig.MatchingRPS.Get(dc),
		OperatorRPSRatio:                         dynamicconfig.OperatorRPSRatio.Get(dc),
		RangeSize:                                100000,
		GetTasksBatchSize:                        dynamicconfig.MatchingGetTasksBatchSize.Get(dc),
		UpdateAckInterval:                        dynamicconfig.MatchingUpdateAckInterval.Get(dc),
		MaxTaskQueueIdleTime:                     dynamicconfig.MatchingMaxTaskQueueIdleTime.Get(dc),
		LongPollExpirationInterval:               dynamicconfig.MatchingLongPollExpirationInterval.Get(dc),
		MinTaskThrottlingBurstSize:               dynamicconfig.MatchingMinTaskThrottlingBurstSize.Get(dc),
		MaxTaskDeleteBatchSize:                   dynamicconfig.MatchingMaxTaskDeleteBatchSize.Get(dc),
		OutstandingTaskAppendsThreshold:          dynamicconfig.MatchingOutstandingTaskAppendsThreshold.Get(dc),
		MaxTaskBatchSize:                         dynamicconfig.MatchingMaxTaskBatchSize.Get(dc),
		ThrottledLogRPS:                          dynamicconfig.MatchingThrottledLogRPS.Get(dc),
		NumTaskqueueWritePartitions:              dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(dc),
		NumTaskqueueReadPartitions:               dynamicconfig.MatchingNumTaskqueueReadPartitions.Get(dc),
		ForwarderMaxOutstandingPolls:             dynamicconfig.MatchingForwarderMaxOutstandingPolls.Get(dc),
		ForwarderMaxOutstandingTasks:             dynamicconfig.MatchingForwarderMaxOutstandingTasks.Get(dc),
		ForwarderMaxRatePerSecond:                dynamicconfig.MatchingForwarderMaxRatePerSecond.Get(dc),
		ForwarderMaxChildrenPerNode:              dynamicconfig.MatchingForwarderMaxChildrenPerNode.Get(dc),
		AlignMembershipChange:                    dynamicconfig.MatchingAlignMembershipChange.Get(dc),
		ShutdownDrainDuration:                    dynamicconfig.MatchingShutdownDrainDuration.Get(dc),
		VersionCompatibleSetLimitPerQueue:        dynamicconfig.VersionCompatibleSetLimitPerQueue.Get(dc),
		VersionBuildIdLimitPerQueue:              dynamicconfig.VersionBuildIdLimitPerQueue.Get(dc),
		AssignmentRuleLimitPerQueue:              dynamicconfig.AssignmentRuleLimitPerQueue.Get(dc),
		RedirectRuleLimitPerQueue:                dynamicconfig.RedirectRuleLimitPerQueue.Get(dc),
		RedirectRuleMaxUpstreamBuildIDsPerQueue:  dynamicconfig.RedirectRuleMaxUpstreamBuildIDsPerQueue.Get(dc),
		DeletedRuleRetentionTime:                 dynamicconfig.MatchingDeletedRuleRetentionTime.Get(dc),
		ReachabilityBuildIdVisibilityGracePeriod: dynamicconfig.ReachabilityBuildIdVisibilityGracePeriod.Get(dc),
		ReachabilityCacheOpenWFsTTL:              dynamicconfig.ReachabilityCacheOpenWFsTTL.Get(dc),
		ReachabilityCacheClosedWFsTTL:            dynamicconfig.ReachabilityCacheClosedWFsTTL.Get(dc),
		TaskQueueLimitPerBuildId:                 dynamicconfig.TaskQueuesPerBuildIdLimit.Get(dc),
		GetUserDataLongPollTimeout:               dynamicconfig.MatchingGetUserDataLongPollTimeout.Get(dc), // Use -10 seconds so that we send back empty response instead of timeout
		BacklogNegligibleAge:                     dynamicconfig.MatchingBacklogNegligibleAge.Get(dc),
		MaxWaitForPollerBeforeFwd:                dynamicconfig.MatchingMaxWaitForPollerBeforeFwd.Get(dc),
		QueryPollerUnavailableWindow:             dynamicconfig.QueryPollerUnavailableWindow.Get(dc),
		QueryWorkflowTaskTimeoutLogRate:          dynamicconfig.MatchingQueryWorkflowTaskTimeoutLogRate.Get(dc),
		MembershipUnloadDelay:                    dynamicconfig.MatchingMembershipUnloadDelay.Get(dc),

		AdminNamespaceToPartitionDispatchRate:          dynamicconfig.AdminMatchingNamespaceToPartitionDispatchRate.Get(dc),
		AdminNamespaceTaskqueueToPartitionDispatchRate: dynamicconfig.AdminMatchingNamespaceTaskqueueToPartitionDispatchRate.Get(dc),

		VisibilityPersistenceMaxReadQPS:   visibility.GetVisibilityPersistenceMaxReadQPS(dc),
		VisibilityPersistenceMaxWriteQPS:  visibility.GetVisibilityPersistenceMaxWriteQPS(dc),
		EnableReadFromSecondaryVisibility: visibility.GetEnableReadFromSecondaryVisibilityConfig(dc),
		VisibilityDisableOrderByClause:    dynamicconfig.VisibilityDisableOrderByClause.Get(dc),
		VisibilityEnableManualPagination:  dynamicconfig.VisibilityEnableManualPagination.Get(dc),

		ListNexusEndpointsLongPollTimeout: dynamicconfig.MatchingListNexusEndpointsLongPollTimeout.Get(dc),

		FrontendAccessHistoryFraction: dynamicconfig.FrontendAccessHistoryFraction.Get(dc),
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
		QueryPollerUnavailableWindow: config.QueryPollerUnavailableWindow,
		TestDisableSyncMatch:         config.TestDisableSyncMatch,
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
