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
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/archiver"
	"go.uber.org/fx"
)

type (
	timerQueueFactoryParams struct {
		fx.In

		SchedulerParams

		ClientBean     client.Bean
		ArchivalClient archiver.Client
		MatchingClient resource.MatchingClient
		MetricsHandler metrics.MetricsHandler
	}

	timerQueueFactory struct {
		timerQueueFactoryParams
		queueFactoryBase
	}
)

func NewTimerQueueFactory(
	params timerQueueFactoryParams,
) queues.Factory {
	var scheduler queues.Scheduler
	if params.Config.TimerProcessorEnablePriorityTaskScheduler() {
		scheduler = queues.NewScheduler(
			queues.NewPriorityAssigner(
				params.ClusterMetadata.GetCurrentClusterName(),
				params.NamespaceRegistry,
				queues.PriorityAssignerOptions{
					HighPriorityRPS:       params.Config.TimerTaskHighPriorityRPS,
					CriticalRetryAttempts: params.Config.TimerTaskMaxRetryCount,
				},
				params.MetricsHandler,
			),
			queues.SchedulerOptions{
				ParallelProcessorOptions: ctasks.ParallelProcessorOptions{
					WorkerCount: params.Config.TimerProcessorSchedulerWorkerCount,
					QueueSize:   params.Config.TimerProcessorSchedulerQueueSize(),
				},
				InterleavedWeightedRoundRobinSchedulerOptions: ctasks.InterleavedWeightedRoundRobinSchedulerOptions{
					PriorityToWeight: configs.ConvertDynamicConfigValueToWeights(params.Config.TimerProcessorSchedulerRoundRobinWeights(), params.Logger),
				},
			},
			params.MetricsHandler,
			params.Logger,
		)
	}
	return &timerQueueFactory{
		timerQueueFactoryParams: params,
		queueFactoryBase: queueFactoryBase{
			scheduler: scheduler,
			hostRateLimiter: newQueueHostRateLimiter(
				params.Config.TimerProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
			),
		},
	}
}

func (f *timerQueueFactory) CreateQueue(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Queue {
	if f.scheduler != nil && f.Config.TimerProcessorEnableMultiCursor() {
		logger := log.With(shard.GetLogger(), tag.ComponentTimerQueue)

		currentClusterName := f.ClusterMetadata.GetCurrentClusterName()
		workflowDeleteManager := workflow.NewDeleteManager(
			shard,
			workflowCache,
			f.Config,
			f.ArchivalClient,
			shard.GetTimeSource(),
		)

		activeExecutor := newTimerQueueActiveTaskExecutor(
			shard,
			workflowCache,
			workflowDeleteManager,
			nil,
			logger,
			f.MetricsHandler,
			f.Config,
			f.MatchingClient,
		)

		standbyExecutor := newTimerQueueStandbyTaskExecutor(
			shard,
			workflowCache,
			workflowDeleteManager,
			xdc.NewNDCHistoryResender(
				shard.GetNamespaceRegistry(),
				f.ClientBean,
				func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
					engine, err := shard.GetEngine(ctx)
					if err != nil {
						return err
					}
					return engine.ReplicateEventsV2(ctx, request)
				},
				shard.GetPayloadSerializer(),
				f.Config.StandbyTaskReReplicationContextTimeout,
				logger,
			),
			f.MatchingClient,
			logger,
			f.MetricsHandler,
			// note: the cluster name is for calculating time for standby tasks,
			// here we are basically using current cluster time
			// this field will be deprecated soon, currently exists so that
			// we have the option of revert to old behavior
			currentClusterName,
			f.Config,
		)

		executor := queues.NewExecutorWrapper(
			currentClusterName,
			f.NamespaceRegistry,
			activeExecutor,
			standbyExecutor,
			logger,
		)

		return queues.NewScheduledQueue(
			shard,
			tasks.CategoryTimer,
			f.scheduler,
			executor,
			&queues.Options{
				ReaderOptions: queues.ReaderOptions{
					BatchSize:            f.Config.TimerTaskBatchSize,
					MaxPendingTasksCount: f.Config.TimerProcessorMaxReschedulerSize,
					PollBackoffInterval:  f.Config.TimerProcessorPollBackoffInterval,
				},
				MonitorOptions: queues.MonitorOptions{
					ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				},
				MaxPollInterval:                     f.Config.TimerProcessorMaxPollInterval,
				MaxPollIntervalJitterCoefficient:    f.Config.TimerProcessorMaxPollIntervalJitterCoefficient,
				CheckpointInterval:                  f.Config.TimerProcessorUpdateAckInterval,
				CheckpointIntervalJitterCoefficient: f.Config.TimerProcessorUpdateAckIntervalJitterCoefficient,
				MaxReaderCount:                      f.Config.QueueMaxReaderCount,
				TaskMaxRetryCount:                   f.Config.TimerTaskMaxRetryCount,
			},
			newQueueProcessorRateLimiter(
				f.hostRateLimiter,
				f.Config.TimerProcessorMaxPollRPS,
			),
			logger,
			f.MetricsHandler.WithTags(metrics.OperationTag(queues.OperationTimerQueueProcessor)),
		)
	}

	return newTimerQueueProcessor(
		shard,
		workflowCache,
		f.scheduler,
		f.ClientBean,
		f.ArchivalClient,
		f.MatchingClient,
		f.MetricsHandler,
		f.hostRateLimiter,
	)
}
