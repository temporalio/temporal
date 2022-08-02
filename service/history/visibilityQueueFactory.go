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
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/fx"
)

type (
	visibilityQueueFactoryParams struct {
		fx.In

		SchedulerParams

		VisibilityMgr  manager.VisibilityManager
		MetricsHandler metrics.MetricsHandler
	}

	visibilityQueueFactory struct {
		visibilityQueueFactoryParams
		queueFactoryBase
	}
)

func NewVisibilityQueueFactory(
	params visibilityQueueFactoryParams,
) queues.Factory {
	var scheduler queues.Scheduler
	if params.Config.VisibilityProcessorEnablePriorityTaskScheduler() {
		scheduler = queues.NewScheduler(
			queues.NewPriorityAssigner(
				params.ClusterMetadata.GetCurrentClusterName(),
				params.NamespaceRegistry,
				queues.PriorityAssignerOptions{
					HighPriorityRPS:       params.Config.VisibilityTaskHighPriorityRPS,
					CriticalRetryAttempts: params.Config.VisibilityTaskMaxRetryCount,
				},
				params.MetricsHandler,
			),
			queues.SchedulerOptions{
				ParallelProcessorOptions: ctasks.ParallelProcessorOptions{
					WorkerCount: params.Config.VisibilityProcessorSchedulerWorkerCount,
					QueueSize:   params.Config.VisibilityProcessorSchedulerQueueSize(),
				},
				InterleavedWeightedRoundRobinSchedulerOptions: ctasks.InterleavedWeightedRoundRobinSchedulerOptions{
					PriorityToWeight: configs.ConvertDynamicConfigValueToWeights(params.Config.VisibilityProcessorSchedulerRoundRobinWeights(), params.Logger),
				},
			},
			params.MetricsHandler,
			params.Logger,
		)
	}
	return &visibilityQueueFactory{
		visibilityQueueFactoryParams: params,
		queueFactoryBase: queueFactoryBase{
			scheduler: scheduler,
			hostRateLimiter: newQueueHostRateLimiter(
				params.Config.VisibilityProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
			),
		},
	}
}

func (f *visibilityQueueFactory) CreateQueue(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Queue {
	if f.scheduler != nil && f.Config.VisibilityProcessorEnableMultiCursor() {
		executor := newVisibilityQueueTaskExecutor(
			shard,
			workflowCache,
			f.VisibilityMgr,
			f.Logger,
			f.MetricsHandler,
		)

		return queues.NewImmediateQueue(
			shard,
			tasks.CategoryVisibility,
			f.scheduler,
			executor,
			&queues.Options{
				ReaderOptions: queues.ReaderOptions{
					BatchSize:            f.Config.VisibilityTaskBatchSize,
					MaxPendingTasksCount: f.Config.VisibilityProcessorMaxReschedulerSize,
					PollBackoffInterval:  f.Config.VisibilityProcessorPollBackoffInterval,
				},
				MaxPollInterval:                     f.Config.VisibilityProcessorMaxPollInterval,
				MaxPollIntervalJitterCoefficient:    f.Config.VisibilityProcessorMaxPollIntervalJitterCoefficient,
				CheckpointInterval:                  f.Config.VisibilityProcessorUpdateAckInterval,
				CheckpointIntervalJitterCoefficient: f.Config.VisibilityProcessorUpdateAckIntervalJitterCoefficient,
				TaskMaxRetryCount:                   f.Config.VisibilityTaskMaxRetryCount,
			},
			newQueueProcessorRateLimiter(
				f.hostRateLimiter,
				f.Config.VisibilityProcessorMaxPollRPS,
			),
			f.Logger,
			f.MetricsHandler,
		)
	}

	return newVisibilityQueueProcessor(
		shard,
		workflowCache,
		f.scheduler,
		f.VisibilityMgr,
		f.MetricsHandler,
		f.hostRateLimiter,
	)
}
