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

package replication

import (
	"context"
	"math/rand"
	"strconv"

	"github.com/dgryski/go-farm"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.uber.org/fx"
)

type (
	ClusterChannelKey struct {
		ClusterName string
	}
)

var Module = fx.Provide(
	NewTaskFetcherFactory,
	func(m persistence.ExecutionManager) ExecutionManager {
		return m
	},
	NewExecutionManagerDLQWriter,
	replicationTaskConverterFactoryProvider,
	replicationTaskExecutorProvider,
	fx.Annotated{
		Name:   "HighPriorityTaskScheduler",
		Target: replicationStreamHighPrioritySchedulerProvider,
	},
	fx.Annotated{
		Name:   "LowPriorityTaskScheduler",
		Target: replicationStreamLowPrioritySchedulerProvider,
	},
	executableTaskConverterProvider,
	streamReceiverMonitorProvider,
	ndcHistoryResenderProvider,
	eagerNamespaceRefresherProvider,
	sequentialTaskQueueFactoryProvider,
	dlqWriterAdapterProvider,
	newDLQWriterToggle,
	historyPaginatedFetcherProvider,
	resendHandlerProvider,
	eventImporterProvider,
	historyEventsHandlerProvider,
)

func eagerNamespaceRefresherProvider(
	metadataManager persistence.MetadataManager,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	clientBean client.Bean,
	clusterMetadata cluster.Metadata,
	metricsHandler metrics.Handler,
) EagerNamespaceRefresher {
	return NewEagerNamespaceRefresher(
		metadataManager,
		namespaceRegistry,
		logger,
		clientBean,
		namespace.NewReplicationTaskExecutor(
			clusterMetadata.GetCurrentClusterName(),
			metadataManager,
			logger,
		),
		clusterMetadata.GetCurrentClusterName(),
		metricsHandler,
	)
}

func replicationTaskConverterFactoryProvider(
	config *configs.Config,
) SourceTaskConverterProvider {
	return func(
		historyEngine shard.Engine,
		shardContext shard.Context,
		clientClusterName string,
		serializer serialization.Serializer,
	) SourceTaskConverter {
		return NewSourceTaskConverter(
			historyEngine,
			shardContext.GetNamespaceRegistry(),
			serializer,
			config)
	}
}

func replicationTaskExecutorProvider() TaskExecutorProvider {
	return func(params TaskExecutorParams) TaskExecutor {
		return NewTaskExecutor(
			params.RemoteCluster,
			params.Shard,
			params.HistoryResender,
			params.DeleteManager,
			params.WorkflowCache,
		)
	}
}

func replicationStreamHighPrioritySchedulerProvider(
	config *configs.Config,
	logger log.Logger,
	queueFactory ctasks.SequentialTaskQueueFactory[TrackableExecutableTask],
	lc fx.Lifecycle,
) ctasks.Scheduler[TrackableExecutableTask] {
	// SequentialScheduler has panic wrapper when executing task,
	// if changing the executor, please make sure other executor has panic wrapper
	scheduler := ctasks.NewSequentialScheduler[TrackableExecutableTask](
		&ctasks.SequentialSchedulerOptions{
			QueueSize:   config.ReplicationProcessorSchedulerQueueSize(),
			WorkerCount: config.ReplicationProcessorSchedulerWorkerCount,
		},
		WorkflowKeyHashFn,
		queueFactory,
		logger,
	)
	taskChannelKeyFn := func(e TrackableExecutableTask) ClusterChannelKey {
		return ClusterChannelKey{
			ClusterName: e.SourceClusterName(),
		}
	}
	channelWeightFn := func(key ClusterChannelKey) int {
		return 1
	}
	// This creates a per cluster channel.
	// They share the same weight so it just does a round-robin on all clusters' tasks.
	rrScheduler := ctasks.NewInterleavedWeightedRoundRobinScheduler(
		ctasks.InterleavedWeightedRoundRobinSchedulerOptions[TrackableExecutableTask, ClusterChannelKey]{
			TaskChannelKeyFn: taskChannelKeyFn,
			ChannelWeightFn:  channelWeightFn,
		},
		scheduler,
		logger,
	)
	lc.Append(fx.StartStopHook(rrScheduler.Start, rrScheduler.Stop))
	return rrScheduler
}

func replicationStreamLowPrioritySchedulerProvider(
	config *configs.Config,
	logger log.Logger,
	lc fx.Lifecycle,
) ctasks.Scheduler[TrackableExecutableTask] {
	queueFactory := func(task TrackableExecutableTask) ctasks.SequentialTaskQueue[TrackableExecutableTask] {
		return NewSequentialTaskQueue(task)
	}
	taskQueueHashFunc := func(item interface{}) uint32 {
		workflowKey, ok := item.(definition.WorkflowKey)
		if !ok {
			return 0
		}

		idBytes := []byte(workflowKey.NamespaceID + "_" + workflowKey.WorkflowID + "_" + strconv.Itoa(rand.Intn(config.ReplicationLowPriorityTaskParallelism())))
		return farm.Fingerprint32(idBytes)
	}
	// SequentialScheduler has panic wrapper when executing task,
	// if changing the executor, please make sure other executor has panic wrapper
	scheduler := ctasks.NewSequentialScheduler[TrackableExecutableTask](
		&ctasks.SequentialSchedulerOptions{
			QueueSize:   config.ReplicationProcessorSchedulerQueueSize(),
			WorkerCount: config.ReplicationLowPriorityProcessorSchedulerWorkerCount,
		},
		taskQueueHashFunc,
		queueFactory,
		logger,
	)
	taskChannelKeyFn := func(e TrackableExecutableTask) ClusterChannelKey {
		return ClusterChannelKey{
			ClusterName: e.SourceClusterName(),
		}
	}
	channelWeightFn := func(key ClusterChannelKey) int {
		return 1
	}
	// This creates a per cluster channel.
	// They share the same weight so it just does a round-robin on all clusters' tasks.
	rrScheduler := ctasks.NewInterleavedWeightedRoundRobinScheduler(
		ctasks.InterleavedWeightedRoundRobinSchedulerOptions[TrackableExecutableTask, ClusterChannelKey]{
			TaskChannelKeyFn: taskChannelKeyFn,
			ChannelWeightFn:  channelWeightFn,
		},
		scheduler,
		logger,
	)
	lc.Append(fx.StartStopHook(rrScheduler.Start, rrScheduler.Stop))
	return rrScheduler
}

func sequentialTaskQueueFactoryProvider(
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *configs.Config,
) ctasks.SequentialTaskQueueFactory[TrackableExecutableTask] {
	return func(task TrackableExecutableTask) ctasks.SequentialTaskQueue[TrackableExecutableTask] {
		if config.EnableReplicationTaskBatching() {
			return NewSequentialBatchableTaskQueue(task, nil, logger, metricsHandler)
		}
		return NewSequentialTaskQueue(task)
	}
}

func executableTaskConverterProvider(
	processToolBox ProcessToolBox,
) ExecutableTaskConverter {
	return NewExecutableTaskConverter(processToolBox)
}

func streamReceiverMonitorProvider(
	processToolBox ProcessToolBox,
	taskConverter ExecutableTaskConverter,
) StreamReceiverMonitor {
	return NewStreamReceiverMonitor(
		processToolBox,
		taskConverter,
		processToolBox.Config.EnableReplicationStream(),
	)
}

func ndcHistoryResenderProvider(
	config *configs.Config,
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	serializer serialization.Serializer,
	logger log.Logger,
	shardController shard.Controller,
	historyReplicationEventHandler eventhandler.HistoryEventsHandler,
) xdc.NDCHistoryResender {
	return xdc.NewNDCHistoryResender(
		namespaceRegistry,
		clientBean,
		func(
			ctx context.Context,
			sourceClusterName string,
			namespaceId namespace.ID,
			workflowId string,
			runId string,
			events [][]*historypb.HistoryEvent,
			versionHistory []*historyspb.VersionHistoryItem,
		) error {
			if config.EnableReplicateLocalGeneratedEvent() {
				return historyReplicationEventHandler.HandleHistoryEvents(
					ctx,
					sourceClusterName,
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
			}

			shardContext, err := shardController.GetShardByNamespaceWorkflow(
				namespaceId,
				workflowId,
			)
			if err != nil {
				return err
			}
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
		serializer,
		config.StandbyTaskReReplicationContextTimeout,
		logger,
		config,
	)
}

func resendHandlerProvider(
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	serializer serialization.Serializer,
	clusterMetadata cluster.Metadata,
	shardController shard.Controller,
	config *configs.Config,
	remoteHistoryFetcher eventhandler.HistoryPaginatedFetcher,
	logger log.Logger,
	importer eventhandler.EventImporter,
) eventhandler.ResendHandler {
	return eventhandler.NewResendHandler(
		namespaceRegistry,
		clientBean,
		serializer,
		clusterMetadata,
		func(ctx context.Context, namespaceId namespace.ID, workflowId string) (shard.Engine, error) {
			shardContext, err := shardController.GetShardByNamespaceWorkflow(
				namespaceId,
				workflowId,
			)
			if err != nil {
				return nil, err
			}
			return shardContext.GetEngine(ctx)
		},
		remoteHistoryFetcher,
		importer,
		logger,
		config,
	)
}

func eventImporterProvider(
	historyFetcher eventhandler.HistoryPaginatedFetcher,
	shardController shard.Controller,
	serializer serialization.Serializer,
	logger log.Logger,
) eventhandler.EventImporter {
	return eventhandler.NewEventImporter(
		historyFetcher,
		func(ctx context.Context, namespaceId namespace.ID, workflowId string) (shard.Engine, error) {
			shardContext, err := shardController.GetShardByNamespaceWorkflow(
				namespaceId,
				workflowId,
			)
			if err != nil {
				return nil, err
			}
			return shardContext.GetEngine(ctx)
		},
		serializer,
		logger,
	)
}

func dlqWriterAdapterProvider(
	dlqWriter *queues.DLQWriter,
	taskSerializer serialization.Serializer,
	clusterMetadata cluster.Metadata,
) *DLQWriterAdapter {
	return NewDLQWriterAdapter(dlqWriter, taskSerializer, clusterMetadata.GetCurrentClusterName())
}

func historyEventsHandlerProvider(
	clusterMetadata cluster.Metadata,
	importer eventhandler.EventImporter,
	shardController shard.Controller,
	logger log.Logger,
) eventhandler.HistoryEventsHandler {
	return eventhandler.NewHistoryEventsHandler(
		clusterMetadata,
		importer,
		shardController,
		logger,
	)
}

func historyPaginatedFetcherProvider(
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	serializer serialization.Serializer,
	logger log.Logger,
) eventhandler.HistoryPaginatedFetcher {
	return eventhandler.NewHistoryPaginatedFetcher(
		namespaceRegistry,
		clientBean,
		serializer,
		logger,
	)
}
