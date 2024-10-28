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

package history

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

const (
	visibilityQueuePersistenceMaxRPSRatio = 0.15
)

type (
	visibilityQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams

		VisibilityMgr manager.VisibilityManager
	}

	visibilityQueueFactory struct {
		visibilityQueueFactoryParams
		QueueFactoryBase
	}
)

func NewVisibilityQueueFactory(
	params visibilityQueueFactoryParams,
) QueueFactory {
	return &visibilityQueueFactory{
		visibilityQueueFactoryParams: params,
		QueueFactoryBase: QueueFactoryBase{
			HostScheduler: queues.NewScheduler(
				params.ClusterMetadata.GetCurrentClusterName(),
				queues.SchedulerOptions{
					WorkerCount:             params.Config.VisibilityProcessorSchedulerWorkerCount,
					ActiveNamespaceWeights:  params.Config.VisibilityProcessorSchedulerActiveRoundRobinWeights,
					StandbyNamespaceWeights: params.Config.VisibilityProcessorSchedulerStandbyRoundRobinWeights,
				},
				params.NamespaceRegistry,
				params.Logger,
			),
			HostPriorityAssigner: queues.NewPriorityAssigner(),
			HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
				NewHostRateLimiterRateFn(
					params.Config.VisibilityProcessorMaxPollHostRPS,
					params.Config.PersistenceMaxQPS,
					visibilityQueuePersistenceMaxRPSRatio,
				),
				int64(params.Config.VisibilityQueueMaxReaderCount()),
			),
		},
	}
}

func (f *visibilityQueueFactory) CreateQueue(
	shard shard.Context,
	workflowCache wcache.Cache,
) queues.Queue {
	logger := log.With(shard.GetLogger(), tag.ComponentVisibilityQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationVisibilityQueueProcessorScope))

	var shardScheduler = f.HostScheduler
	if f.Config.TaskSchedulerEnableRateLimiter() {
		shardScheduler = queues.NewRateLimitedScheduler(
			f.HostScheduler,
			queues.RateLimitedSchedulerOptions{
				EnableShadowMode: f.Config.TaskSchedulerEnableRateLimiterShadowMode,
				StartupDelay:     f.Config.TaskSchedulerRateLimiterStartupDelay,
			},
			f.ClusterMetadata.GetCurrentClusterName(),
			f.NamespaceRegistry,
			f.SchedulerRateLimiter,
			f.TimeSource,
			logger,
			metricsHandler,
		)
	}

	rescheduler := queues.NewRescheduler(
		shardScheduler,
		shard.GetTimeSource(),
		logger,
		metricsHandler,
	)

	executor := newVisibilityQueueTaskExecutor(
		shard,
		workflowCache,
		f.VisibilityMgr,
		logger,
		f.MetricsHandler,
		f.Config.VisibilityProcessorEnsureCloseBeforeDelete,
		f.Config.VisibilityProcessorEnableCloseWorkflowCleanup,
		f.Config.VisibilityProcessorRelocateAttributesMinBlobSize,
	)
	if f.ExecutorWrapper != nil {
		executor = f.ExecutorWrapper.Wrap(executor)
	}

	factory := queues.NewExecutableFactory(
		executor,
		shardScheduler,
		rescheduler,
		f.HostPriorityAssigner,
		shard.GetTimeSource(),
		shard.GetNamespaceRegistry(),
		shard.GetClusterMetadata(),
		logger,
		metricsHandler,
		f.DLQWriter,
		f.Config.TaskDLQEnabled,
		f.Config.TaskDLQUnexpectedErrorAttempts,
		f.Config.TaskDLQErrorPattern,
	)
	return queues.NewImmediateQueue(
		shard,
		tasks.CategoryVisibility,
		shardScheduler,
		rescheduler,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.VisibilityTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.VisibilityProcessorPollBackoffInterval,
				MaxPredicateSize:     f.Config.QueueMaxPredicateSize,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.VisibilityProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.VisibilityProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.VisibilityProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.VisibilityProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.VisibilityProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.VisibilityQueueMaxReaderCount,
		},
		f.HostReaderRateLimiter,
		queues.GrouperNamespaceID{},
		logger,
		metricsHandler,
		factory,
	)
}
