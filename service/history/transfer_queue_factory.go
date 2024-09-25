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

	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
)

const (
	transferQueuePersistenceMaxRPSRatio = 0.3
)

type (
	transferQueueFactoryParams struct {
		fx.In

		QueueFactoryBaseParams

		ClientBean        client.Bean
		SdkClientFactory  sdk.ClientFactory
		HistoryRawClient  resource.HistoryRawClient
		MatchingRawClient resource.MatchingRawClient
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
			HostScheduler: queues.NewScheduler(
				params.ClusterMetadata.GetCurrentClusterName(),
				queues.SchedulerOptions{
					WorkerCount:             params.Config.TransferProcessorSchedulerWorkerCount,
					ActiveNamespaceWeights:  params.Config.TransferProcessorSchedulerActiveRoundRobinWeights,
					StandbyNamespaceWeights: params.Config.TransferProcessorSchedulerStandbyRoundRobinWeights,
				},
				params.NamespaceRegistry,
				params.Logger,
			),
			HostPriorityAssigner: queues.NewPriorityAssigner(),
			HostReaderRateLimiter: queues.NewReaderPriorityRateLimiter(
				NewHostRateLimiterRateFn(
					params.Config.TransferProcessorMaxPollHostRPS,
					params.Config.PersistenceMaxQPS,
					transferQueuePersistenceMaxRPSRatio,
				),
				int64(params.Config.TransferQueueMaxReaderCount()),
			),
		},
	}
}

func (f *transferQueueFactory) CreateQueue(
	shardContext shard.Context,
	workflowCache wcache.Cache,
) queues.Queue {
	logger := log.With(shardContext.GetLogger(), tag.ComponentTransferQueue)
	metricsHandler := f.MetricsHandler.WithTags(metrics.OperationTag(metrics.OperationTransferQueueProcessorScope))

	currentClusterName := f.ClusterMetadata.GetCurrentClusterName()

	var shardScheduler = f.HostScheduler
	if f.Config.TaskSchedulerEnableRateLimiter() {
		shardScheduler = queues.NewRateLimitedScheduler(
			f.HostScheduler,
			queues.RateLimitedSchedulerOptions{
				EnableShadowMode: f.Config.TaskSchedulerEnableRateLimiterShadowMode,
				StartupDelay:     f.Config.TaskSchedulerRateLimiterStartupDelay,
			},
			currentClusterName,
			f.NamespaceRegistry,
			f.SchedulerRateLimiter,
			f.TimeSource,
			logger,
			metricsHandler,
		)
	}

	rescheduler := queues.NewRescheduler(
		shardScheduler,
		shardContext.GetTimeSource(),
		logger,
		metricsHandler,
	)

	activeExecutor := newTransferQueueActiveTaskExecutor(
		shardContext,
		workflowCache,
		f.SdkClientFactory,
		logger,
		f.MetricsHandler,
		f.Config,
		f.HistoryRawClient,
		f.MatchingRawClient,
		f.VisibilityManager,
	)
	eventImporter := eventhandler.NewEventImporter(
		f.RemoteHistoryFetcher,
		func(ctx context.Context, namespaceID namespace.ID, workflowID string) (shard.Engine, error) {
			return shardContext.GetEngine(ctx)
		},
		f.Serializer,
		logger,
	)
	resendHandler := eventhandler.NewResendHandler(
		f.NamespaceRegistry,
		f.ClientBean,
		f.Serializer,
		f.ClusterMetadata,
		func(ctx context.Context, namespaceID namespace.ID, workflowID string) (shard.Engine, error) {
			return shardContext.GetEngine(ctx)
		},
		f.RemoteHistoryFetcher,
		eventImporter,
		logger,
		f.Config,
	)

	standbyExecutor := newTransferQueueStandbyTaskExecutor(
		shardContext,
		workflowCache,
		xdc.NewNDCHistoryResender(
			f.NamespaceRegistry,
			f.ClientBean,
			func(
				ctx context.Context,
				sourceClusterName string,
				namespaceId namespace.ID,
				workflowId string,
				runId string,
				events [][]*historypb.HistoryEvent,
				versionHistory []*historyspb.VersionHistoryItem,
			) error {
				engine, err := shardContext.GetEngine(ctx)
				if err != nil {
					return err
				}
				return engine.ReplicateHistoryEvents(
					ctx,
					definition.WorkflowKey{
						NamespaceID: namespaceId.String(),
						WorkflowID:  workflowId,
						RunID:       runId,
					},
					nil,
					versionHistory,
					events,
					nil,
					"",
				)
			},
			shardContext.GetPayloadSerializer(),
			f.Config.StandbyTaskReReplicationContextTimeout,
			logger,
			f.Config,
		),
		resendHandler,
		logger,
		f.MetricsHandler,
		currentClusterName,
		f.HistoryRawClient,
		f.MatchingRawClient,
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

	factory := queues.NewExecutableFactory(
		executor,
		shardScheduler,
		rescheduler,
		f.HostPriorityAssigner,
		shardContext.GetTimeSource(),
		shardContext.GetNamespaceRegistry(),
		shardContext.GetClusterMetadata(),
		logger,
		metricsHandler,
		f.DLQWriter,
		f.Config.TaskDLQEnabled,
		f.Config.TaskDLQUnexpectedErrorAttempts,
		f.Config.TaskDLQInternalErrors,
		f.Config.TaskDLQErrorPattern,
	)
	return queues.NewImmediateQueue(
		shardContext,
		tasks.CategoryTransfer,
		shardScheduler,
		rescheduler,
		&queues.Options{
			ReaderOptions: queues.ReaderOptions{
				BatchSize:            f.Config.TransferTaskBatchSize,
				MaxPendingTasksCount: f.Config.QueuePendingTaskMaxCount,
				PollBackoffInterval:  f.Config.TransferProcessorPollBackoffInterval,
				MaxPredicateSize:     f.Config.QueueMaxPredicateSize,
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
			MaxReaderCount:                      f.Config.TransferQueueMaxReaderCount,
		},
		f.HostReaderRateLimiter,
		queues.GrouperNamespaceID{},
		logger,
		metricsHandler,
		factory,
	)
}
