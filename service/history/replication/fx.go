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

	"go.temporal.io/server/service/history/queues"
	"go.uber.org/fx"

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

var Module = fx.Provide(
	NewTaskFetcherFactory,
	func(m persistence.ExecutionManager) ExecutionManager {
		return m
	},
	NewExecutionManagerDLQWriter,
	replicationTaskConverterFactoryProvider,
	replicationTaskExecutorProvider,
	replicationStreamSchedulerProvider,
	executableTaskConverterProvider,
	streamReceiverMonitorProvider,
	ndcHistoryResenderProvider,
	eagerNamespaceRefresherProvider,
	sequentialTaskQueueFactoryProvider,
	dlqWriterAdapterProvider,
	historyPaginatedFetcherProvider,
	newDLQWriterToggle,
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
func historyPaginatedFetcherProvider(
	config *configs.Config,
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	serializer serialization.Serializer,
	logger log.Logger,
) HistoryPaginatedFetcher {
	return NewHistoryPaginatedFetcher(
		namespaceRegistry,
		clientBean,
		serializer,
		config.StandbyTaskReReplicationContextTimeout,
		logger,
	)
}

func replicationTaskConverterFactoryProvider() SourceTaskConverterProvider {
	return func(
		historyEngine shard.Engine,
		shardContext shard.Context,
		clientClusterShardCount int32,
		clientClusterName string,
		clientShardKey ClusterShardKey,
	) SourceTaskConverter {
		return NewSourceTaskConverter(
			historyEngine,
			shardContext.GetNamespaceRegistry(),
			clientClusterShardCount,
			clientClusterName,
			clientShardKey)
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

func replicationStreamSchedulerProvider(
	config *configs.Config,
	logger log.Logger,
	queueFactory ctasks.SequentialTaskQueueFactory[TrackableExecutableTask],
	lc fx.Lifecycle,
) ctasks.Scheduler[TrackableExecutableTask] {
	scheduler := ctasks.NewSequentialScheduler[TrackableExecutableTask](
		&ctasks.SequentialSchedulerOptions{
			QueueSize:   config.ReplicationProcessorSchedulerQueueSize(),
			WorkerCount: config.ReplicationProcessorSchedulerWorkerCount,
		},
		WorkflowKeyHashFn,
		queueFactory,
		logger,
	)
	lc.Append(fx.StartStopHook(scheduler.Start, scheduler.Stop))
	return scheduler
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
) xdc.NDCHistoryResender {
	return xdc.NewNDCHistoryResender(
		namespaceRegistry,
		clientBean,
		func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
			// use HistoryEventsHandler.HandleHistoryEvents(...) instead
			_, err := clientBean.GetHistoryClient().ReplicateEventsV2(ctx, request)
			return err
		},
		serializer,
		config.StandbyTaskReReplicationContextTimeout,
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
