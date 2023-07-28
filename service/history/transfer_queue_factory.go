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

	"go.uber.org/fx"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/worker/archiver"
)

const (
	transferQueuePersistenceMaxRPSRatio = 0.3
)

type (
	transferQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams

		ClientBean        client.Bean
		ArchivalClient    archiver.Client
		SdkClientFactory  sdk.ClientFactory
		MatchingClient    resource.MatchingClient
		HistoryClient     historyservice.HistoryServiceClient
		VisibilityManager manager.VisibilityManager
	}

	transferQueueFactory struct {
		transferQueueFactoryParams
		QueueFactoryBase
	}
)

func NewTransferQueueFactory(
	params transferQueueFactoryParams,
) QueueFactory {
	return &transferQueueFactory{
		transferQueueFactoryParams: params,
		QueueFactoryBase: QueueFactoryBase{
			HostScheduler: queues.NewNamespacePriorityScheduler(
				params.ClusterMetadata.GetCurrentClusterName(),
				queues.NamespacePrioritySchedulerOptions{
					WorkerCount:                 params.Config.TransferProcessorSchedulerWorkerCount,
					ActiveNamespaceWeights:      params.Config.TransferProcessorSchedulerActiveRoundRobinWeights,
					StandbyNamespaceWeights:     params.Config.TransferProcessorSchedulerStandbyRoundRobinWeights,
					EnableRateLimiter:           params.Config.TaskSchedulerEnableRateLimiter,
					EnableRateLimiterShadowMode: params.Config.TaskSchedulerEnableRateLimiterShadowMode,
					DispatchThrottleDuration:    params.Config.TaskSchedulerThrottleDuration,
				},
				params.NamespaceRegistry,
				params.SchedulerRateLimiter,
				params.TimeSource,
				params.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationTransferQueueProcessorScope)),
				params.Logger,
			),
			HostPriorityAssigner: queues.NewPriorityAssigner(),
			HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
				NewHostRateLimiterRateFn(
					params.Config.TransferProcessorMaxPollHostRPS,
					params.Config.PersistenceMaxQPS,
					transferQueuePersistenceMaxRPSRatio,
				),
				int64(params.Config.QueueMaxReaderCount()),
			),
		},
	}
}

func (f *transferQueueFactory) CreateQueue(
	shard shard.Context,
	workflowCache wcache.Cache,
) queues.Queue {
	logger := log.With(shard.GetLogger(), tag.ComponentTransferQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationTransferQueueProcessorScope))

	rescheduler := queues.NewRescheduler(
		f.HostScheduler,
		shard.GetTimeSource(),
		logger,
		metricsHandler,
	)

	currentClusterName := f.ClusterMetadata.GetCurrentClusterName()
	activeExecutor := newTransferQueueActiveTaskExecutor(
		shard,
		workflowCache,
		f.ArchivalClient,
		f.SdkClientFactory,
		logger,
		f.MetricsHandler,
		f.Config,
		f.MatchingClient,
		f.VisibilityManager,
	)

	standbyExecutor := newTransferQueueStandbyTaskExecutor(
		shard,
		workflowCache,
		f.ArchivalClient,
		xdc.NewNDCHistoryResender(
			f.NamespaceRegistry,
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
		logger,
		f.MetricsHandler,
		currentClusterName,
		f.MatchingClient,
		f.VisibilityManager,
	)

	executor := queues.NewActiveStandbyExecutor(
		currentClusterName,
		f.NamespaceRegistry,
		activeExecutor,
		standbyExecutor,
		logger,
	)
	if f.ExecutorWrapper != nil {
		executor = f.ExecutorWrapper.Wrap(executor)
	}

	return queues.NewImmediateQueue(
		shard,
		tasks.CategoryTransfer,
		f.HostScheduler,
		rescheduler,
		f.HostPriorityAssigner,
		executor,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.TransferTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.TransferProcessorPollBackoffInterval,
			},
			MonitorOptions: queues.MonitorOptions{
				PendingTasksCriticalCount:   f.Config.QueuePendingTaskCriticalCount,
				ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				SliceCountCriticalThreshold: f.Config.QueueCriticalSlicesCount,
			},
			MaxPollRPS:                          f.Config.TransferProcessorMaxPollRPS,
			MaxPollInterval:                     f.Config.TransferProcessorMaxPollInterval,
			MaxPollIntervalJitterCoefficient:    f.Config.TransferProcessorMaxPollIntervalJitterCoefficient,
			CheckpointInterval:                  f.Config.TransferProcessorUpdateAckInterval,
			CheckpointIntervalJitterCoefficient: f.Config.TransferProcessorUpdateAckIntervalJitterCoefficient,
			MaxReaderCount:                      f.Config.QueueMaxReaderCount,
		},
		f.HostReaderRateLimiter,
		logger,
		metricsHandler,
	)
}
