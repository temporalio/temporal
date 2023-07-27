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
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/worker/archiver"
	"go.uber.org/fx"
)

const (
	timerQueuePersistenceMaxRPSRatio = 0.3
)

type (
	timerQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams
		ArchivalClient    archiver.Client
		VisibilityManager manager.VisibilityManager
	}

	timerQueueFactory struct {
		timerQueueFactoryParams
		QueueFactoryBase
	}
)

func NewTimerQueueFactory(
	params timerQueueFactoryParams,
) QueueFactory {
	return &timerQueueFactory{
		timerQueueFactoryParams: params,
		QueueFactoryBase: QueueFactoryBase{
			HostScheduler: queues.NewNamespacePriorityScheduler(
				params.ClusterMetadata.GetCurrentClusterName(),
				queues.NamespacePrioritySchedulerOptions{
					WorkerCount:                 params.Config.TimerProcessorSchedulerWorkerCount,
					ActiveNamespaceWeights:      params.Config.TimerProcessorSchedulerActiveRoundRobinWeights,
					StandbyNamespaceWeights:     params.Config.TimerProcessorSchedulerStandbyRoundRobinWeights,
					EnableRateLimiter:           params.Config.TaskSchedulerEnableRateLimiter,
					EnableRateLimiterShadowMode: params.Config.TaskSchedulerEnableRateLimiterShadowMode,
					DispatchThrottleDuration:    params.Config.TaskSchedulerThrottleDuration,
				},
				params.NamespaceRegistry,
				params.SchedulerRateLimiter,
				params.TimeSource,
				params.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationTimerQueueProcessorScope)),
				params.Logger,
			),
			HostPriorityAssigner: queues.NewPriorityAssigner(),
			HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
				NewHostRateLimiterRateFn(
					params.Config.TimerProcessorMaxPollHostRPS,
					params.Config.PersistenceMaxQPS,
					timerQueuePersistenceMaxRPSRatio,
				),
				int64(params.Config.QueueMaxReaderCount()),
			),
		},
	}
}

func (f *timerQueueFactory) CreateQueue(
	shard shard.Context,
	workflowCache wcache.Cache,
) queues.Queue {
	logger := log.With(shard.GetLogger(), tag.ComponentTimerQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationTimerQueueProcessorScope))
	currentClusterName := f.ClusterMetadata.GetCurrentClusterName()

	deleteManager := f.getNewDeleteManager(shard, workflowCache)

	executor := f.ExecutorFactory.CreateExecutorWrapper(
		currentClusterName,
		f.ExecutorFactory.CreateTimerActiveExecutor(
			shard,
			workflowCache,
			deleteManager,
			logger),
		f.ExecutorFactory.CreateTimerStandbyExecutor(
			shard,
			workflowCache,
			deleteManager,
			f.ExecutorFactory.GetNewDefaultStandbyNDCHistoryResender(shard, logger),
			logger,
			currentClusterName),
		logger,
	)

	rescheduler := queues.NewRescheduler(
		f.HostScheduler,
		shard.GetTimeSource(),
		logger,
		metricsHandler,
	)

	return queues.NewScheduledQueue(
		shard,
		tasks.CategoryTimer,
		f.HostScheduler,
		rescheduler,
		f.HostPriorityAssigner,
		executor,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.TimerTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.TimerProcessorPollBackoffInterval,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.TimerProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.TimerProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.TimerProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.TimerProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.TimerProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.QueueMaxReaderCount,
		},
		f.HostReaderRateLimiter,
		logger,
		metricsHandler,
	)
}

func (f *timerQueueFactory) getNewDeleteManager(shardCtx shard.Context, workflowCache wcache.Cache) deletemanager.DeleteManager {
	return deletemanager.NewDeleteManager(
		shardCtx,
		workflowCache,
		f.Config,
		f.ArchivalClient,
		shardCtx.GetTimeSource(),
		f.VisibilityManager,
	)
}
