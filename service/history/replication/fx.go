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

	"go.uber.org/fx"

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

var Module = fx.Options(
	fx.Provide(ReplicationTaskFetcherFactoryProvider),
	fx.Provide(ReplicationTaskConverterFactoryProvider),
	fx.Provide(ReplicationTaskExecutorProvider),
	fx.Provide(ReplicationStreamSchedulerProvider),
	fx.Provide(StreamReceiverMonitorProvider),
	fx.Invoke(ReplicationStreamSchedulerLifetimeHooks),
	fx.Provide(NDCHistoryResenderProvider),
)

func ReplicationTaskFetcherFactoryProvider(
	logger log.Logger,
	config *configs.Config,
	clusterMetadata cluster.Metadata,
	clientBean client.Bean,
) TaskFetcherFactory {
	return NewTaskFetcherFactory(
		logger,
		config,
		clusterMetadata,
		clientBean,
	)
}

func ReplicationTaskConverterFactoryProvider() SourceTaskConverterProvider {
	return func(historyEngine shard.Engine, shardContext shard.Context, clientClusterShardCount int32, clientClusterName string, clientShardKey ClusterShardKey) SourceTaskConverter {
		return NewSourceTaskConverter(
			historyEngine,
			shardContext.GetNamespaceRegistry(),
			clientClusterShardCount,
			clientClusterName,
			clientShardKey)
	}
}

func ReplicationTaskExecutorProvider() TaskExecutorProvider {
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

func ReplicationStreamSchedulerProvider(
	config *configs.Config,
	logger log.Logger,
) ctasks.Scheduler[TrackableExecutableTask] {
	return ctasks.NewSequentialScheduler[TrackableExecutableTask](
		&ctasks.SequentialSchedulerOptions{
			QueueSize:   config.ReplicationProcessorSchedulerQueueSize(),
			WorkerCount: config.ReplicationProcessorSchedulerWorkerCount,
		},
		TaskHashFn,
		NewSequentialTaskQueue,
		logger,
	)
}

func ReplicationStreamSchedulerLifetimeHooks(
	lc fx.Lifecycle,
	scheduler ctasks.Scheduler[TrackableExecutableTask],
) {
	lc.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				scheduler.Start()
				return nil
			},
			OnStop: func(context.Context) error {
				scheduler.Stop()
				return nil
			},
		},
	)
}

func StreamReceiverMonitorProvider(
	processToolBox ProcessToolBox,
) StreamReceiverMonitor {
	return NewStreamReceiverMonitor(
		processToolBox,
		processToolBox.Config.EnableReplicationStream(),
	)
}

func NDCHistoryResenderProvider(
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
			_, err := clientBean.GetHistoryClient().ReplicateEventsV2(ctx, request)
			return err
		},
		serializer,
		config.StandbyTaskReReplicationContextTimeout,
		logger,
	)
}
