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
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/archiver"
	"go.uber.org/fx"
)

type (
	transferQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams

		ClientBean       client.Bean
		ArchivalClient   archiver.Client
		SdkClientFactory sdk.ClientFactory
		MatchingClient   resource.MatchingClient
		HistoryClient    historyservice.HistoryServiceClient
	}

	transferQueueFactory struct {
		transferQueueFactoryParams
		QueueFactoryBase
	}
)

func NewTransferQueueFactory(
	params transferQueueFactoryParams,
) queues.Factory {
	var hostScheduler queues.Scheduler
	if params.Config.TransferProcessorEnablePriorityTaskScheduler() {
		hostScheduler = queues.NewNamespacePriorityScheduler(
			queues.NamespacePrioritySchedulerOptions{
				WorkerCount:      params.Config.TransferProcessorSchedulerWorkerCount,
				NamespaceWeights: params.Config.TransferProcessorSchedulerRoundRobinWeights,
			},
			params.NamespaceRegistry,
			params.Logger,
		)
	}
	return &transferQueueFactory{
		transferQueueFactoryParams: params,
		QueueFactoryBase: QueueFactoryBase{
			HostScheduler: hostScheduler,
			HostPriorityAssigner: queues.NewPriorityAssigner(
				params.ClusterMetadata.GetCurrentClusterName(),
				params.NamespaceRegistry,
				queues.PriorityAssignerOptions{
					CriticalRetryAttempts: params.Config.TransferTaskMaxRetryCount(),
				},
			),
			HostRateLimiter: NewQueueHostRateLimiter(
				params.Config.TransferProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
			),
		},
	}
}

func (f *transferQueueFactory) CreateQueue(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Queue {
	if f.HostScheduler != nil && f.Config.TransferProcessorEnableMultiCursor() {
		logger := log.With(shard.GetLogger(), tag.ComponentTransferQueue)

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
		)

		executor := queues.NewExecutorWrapper(
			currentClusterName,
			f.NamespaceRegistry,
			activeExecutor,
			standbyExecutor,
			logger,
		)

		return queues.NewImmediateQueue(
			shard,
			tasks.CategoryTransfer,
			f.HostScheduler,
			f.HostPriorityAssigner,
			executor,
			&queues.Options{
				ReaderOptions: queues.ReaderOptions{
					BatchSize:            f.Config.TransferTaskBatchSize,
					MaxPendingTasksCount: f.Config.TransferProcessorMaxReschedulerSize,
					PollBackoffInterval:  f.Config.TransferProcessorPollBackoffInterval,
				},
				MonitorOptions: queues.MonitorOptions{
					ReaderStuckCriticalAttempts: f.Config.QueueReaderStuckCriticalAttempts,
				},
				MaxPollInterval:                     f.Config.TransferProcessorMaxPollInterval,
				MaxPollIntervalJitterCoefficient:    f.Config.TransferProcessorMaxPollIntervalJitterCoefficient,
				CheckpointInterval:                  f.Config.TransferProcessorUpdateAckInterval,
				CheckpointIntervalJitterCoefficient: f.Config.TransferProcessorUpdateAckIntervalJitterCoefficient,
				MaxReaderCount:                      f.Config.QueueMaxReaderCount,
				TaskMaxRetryCount:                   f.Config.TransferTaskMaxRetryCount,
			},
			newQueueProcessorRateLimiter(
				f.HostRateLimiter,
				f.Config.TransferProcessorMaxPollRPS,
			),
			logger,
			f.MetricsHandler.WithTags(metrics.OperationTag(queues.OperationTransferQueueProcessor)),
		)
	}

	return newTransferQueueProcessor(
		shard,
		workflowCache,
		f.HostScheduler,
		f.HostPriorityAssigner,
		f.ClientBean,
		f.ArchivalClient,
		f.SdkClientFactory,
		f.MatchingClient,
		f.HistoryClient,
		f.MetricsHandler,
		f.HostRateLimiter,
	)
}
