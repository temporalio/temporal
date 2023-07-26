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
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	timerQueuePersistenceMaxRPSRatio = 0.3
)

type (
	timerQueueFactory struct {
		QueueFactoryBaseParams
		QueueFactoryBase
	}
)

func NewTimerQueueFactory(
	params QueueFactoryBaseParams,
) QueueFactory {
	return &timerQueueFactory{
		QueueFactoryBaseParams: params,
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

	executor := f.ExecutorFactory.CreateTimerExecutor(
		shard,
		workflowCache,
		logger,
		currentClusterName,
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
