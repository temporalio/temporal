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
	"go.uber.org/fx"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/archiver"
)

var QueueProcessorModule = fx.Options(
	fx.Provide(
		fx.Annotated{
			Group:  queues.ProcessorFactoryFxGroup,
			Target: NewTransferQueueProcessorFactory,
		},
		fx.Annotated{
			Group:  queues.ProcessorFactoryFxGroup,
			Target: NewTimerQueueProcessorFactory,
		},
		fx.Annotated{
			Group:  queues.ProcessorFactoryFxGroup,
			Target: NewVisibilityQueueProcessorFactory,
		},
		fx.Annotated{
			Group:  queues.ProcessorFactoryFxGroup,
			Target: NewReplicationQueueProcessorFactory,
		},
	),
)

type (
	SchedulerParams struct {
		fx.In

		NamespaceRegistry namespace.Registry
		ClusterMetadata   cluster.Metadata
		Config            *configs.Config
		MetricsClient     metrics.Client
		Logger            resource.SnTaggedLogger
	}

	transferQueueProcessorFactoryParams struct {
		fx.In

		SchedulerParams

		ArchivalClient   archiver.Client
		SdkClientFactory sdk.ClientFactory
		MatchingClient   resource.MatchingClient
		HistoryClient    historyservice.HistoryServiceClient
	}

	timerQueueProcessorFactoryParams struct {
		fx.In

		SchedulerParams

		ArchivalClient archiver.Client
		MatchingClient resource.MatchingClient
	}

	visibilityQueueProcessorFactoryParams struct {
		fx.In

		SchedulerParams

		VisibilityMgr manager.VisibilityManager
	}

	replicationQueueProcessorFactoryParams struct {
		fx.In

		Config             *configs.Config
		ArchivalClient     archiver.Client
		EventSerializer    serialization.Serializer
		TaskFetcherFactory replication.TaskFetcherFactory
	}

	transferQueueProcessorFactory struct {
		transferQueueProcessorFactoryParams

		scheduler queues.Scheduler
	}

	timerQueueProcessorFactory struct {
		timerQueueProcessorFactoryParams

		scheduler queues.Scheduler
	}

	visibilityQueueProcessorFactory struct {
		visibilityQueueProcessorFactoryParams

		scheduler queues.Scheduler
	}

	replicationQueueProcessorFactory struct {
		replicationQueueProcessorFactoryParams
	}
)

func NewTransferQueueProcessorFactory(
	params transferQueueProcessorFactoryParams,
) queues.ProcessorFactory {
	var scheduler queues.Scheduler
	if params.Config.TransferProcessorEnablePriorityTaskScheduler() {
		scheduler = queues.NewScheduler(
			queues.NewPriorityAssigner(
				params.ClusterMetadata.GetCurrentClusterName(),
				params.NamespaceRegistry,
				queues.PriorityAssignerOptions{
					HighPriorityRPS:       params.Config.TransferTaskHighPriorityRPS,
					CriticalRetryAttempts: params.Config.TransferTaskMaxRetryCount,
				},
				params.MetricsClient,
			),
			queues.SchedulerOptions{
				ParallelProcessorOptions: ctasks.ParallelProcessorOptions{
					WorkerCount: params.Config.TransferProcessorSchedulerWorkerCount(),
					QueueSize:   params.Config.TransferProcessorSchedulerQueueSize(),
				},
				InterleavedWeightedRoundRobinSchedulerOptions: ctasks.InterleavedWeightedRoundRobinSchedulerOptions{
					PriorityToWeight: configs.ConvertDynamicConfigValueToWeights(params.Config.TransferProcessorSchedulerRoundRobinWeights(), params.Logger),
				},
			},
			params.MetricsClient,
			params.Logger,
		)
	}
	return &transferQueueProcessorFactory{
		transferQueueProcessorFactoryParams: params,
		scheduler:                           scheduler,
	}
}

func (f *transferQueueProcessorFactory) CreateProcessor(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Processor {
	return newTransferQueueProcessor(
		shard,
		engine,
		workflowCache,
		f.scheduler,
		f.ArchivalClient,
		f.SdkClientFactory,
		f.MatchingClient,
		f.HistoryClient,
	)
}

func NewTimerQueueProcessorFactory(
	params timerQueueProcessorFactoryParams,
) queues.ProcessorFactory {
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
				params.MetricsClient,
			),
			queues.SchedulerOptions{
				ParallelProcessorOptions: ctasks.ParallelProcessorOptions{
					WorkerCount: params.Config.TimerProcessorSchedulerWorkerCount(),
					QueueSize:   params.Config.TimerProcessorSchedulerQueueSize(),
				},
				InterleavedWeightedRoundRobinSchedulerOptions: ctasks.InterleavedWeightedRoundRobinSchedulerOptions{
					PriorityToWeight: configs.ConvertDynamicConfigValueToWeights(params.Config.TimerProcessorSchedulerRoundRobinWeights(), params.Logger),
				},
			},
			params.MetricsClient,
			params.Logger,
		)
	}
	return &timerQueueProcessorFactory{
		timerQueueProcessorFactoryParams: params,
		scheduler:                        scheduler,
	}
}

func (f *timerQueueProcessorFactory) CreateProcessor(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Processor {
	return newTimerQueueProcessor(
		shard,
		engine,
		workflowCache,
		f.scheduler,
		f.ArchivalClient,
		f.MatchingClient,
	)
}

func NewVisibilityQueueProcessorFactory(
	params visibilityQueueProcessorFactoryParams,
) queues.ProcessorFactory {
	var scheduler queues.Scheduler
	if params.Config.TimerProcessorEnablePriorityTaskScheduler() {
		scheduler = queues.NewScheduler(
			queues.NewPriorityAssigner(
				params.ClusterMetadata.GetCurrentClusterName(),
				params.NamespaceRegistry,
				queues.PriorityAssignerOptions{
					HighPriorityRPS:       params.Config.VisibilityTaskHighPriorityRPS,
					CriticalRetryAttempts: params.Config.VisibilityTaskMaxRetryCount,
				},
				params.MetricsClient,
			),
			queues.SchedulerOptions{
				ParallelProcessorOptions: ctasks.ParallelProcessorOptions{
					WorkerCount: params.Config.VisibilityProcessorSchedulerWorkerCount(),
					QueueSize:   params.Config.VisibilityProcessorSchedulerQueueSize(),
				},
				InterleavedWeightedRoundRobinSchedulerOptions: ctasks.InterleavedWeightedRoundRobinSchedulerOptions{
					PriorityToWeight: configs.ConvertDynamicConfigValueToWeights(params.Config.VisibilityProcessorSchedulerRoundRobinWeights(), params.Logger),
				},
			},
			params.MetricsClient,
			params.Logger,
		)
	}
	return &visibilityQueueProcessorFactory{
		visibilityQueueProcessorFactoryParams: params,
		scheduler:                             scheduler,
	}
}

func (f *visibilityQueueProcessorFactory) CreateProcessor(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Processor {
	return newVisibilityQueueProcessor(
		shard,
		workflowCache,
		f.scheduler,
		f.VisibilityMgr,
	)
}

func NewReplicationQueueProcessorFactory(
	params replicationQueueProcessorFactoryParams,
) queues.ProcessorFactory {

	return &replicationQueueProcessorFactory{
		replicationQueueProcessorFactoryParams: params,
	}
}

func (f *replicationQueueProcessorFactory) CreateProcessor(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Processor {
	return replication.NewTaskProcessorFactory(
		f.ArchivalClient,
		f.Config,
		engine,
		f.EventSerializer,
		shard,
		f.TaskFetcherFactory,
		workflowCache,
	)
}
