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
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
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
	),
	fx.Invoke(QueueProcessorFactoryLifetimeHooks),
)

type (
	SchedulerParams struct {
		fx.In

		NamespaceRegistry namespace.Registry
		ClusterMetadata   cluster.Metadata
		Config            *configs.Config
		MetricProvider    metrics.MetricProvider
		Logger            resource.SnTaggedLogger
	}

	transferQueueProcessorFactoryParams struct {
		fx.In

		SchedulerParams

		ClientBean       client.Bean
		ArchivalClient   archiver.Client
		SdkClientFactory sdk.ClientFactory
		MatchingClient   resource.MatchingClient
		HistoryClient    historyservice.HistoryServiceClient
		MetricProvider   metrics.MetricProvider
	}

	timerQueueProcessorFactoryParams struct {
		fx.In

		SchedulerParams

		ClientBean     client.Bean
		ArchivalClient archiver.Client
		MatchingClient resource.MatchingClient
		MetricProvider metrics.MetricProvider
	}

	visibilityQueueProcessorFactoryParams struct {
		fx.In

		SchedulerParams

		VisibilityMgr  manager.VisibilityManager
		MetricProvider metrics.MetricProvider
	}

	queueProcessorFactoryBase struct {
		scheduler       queues.Scheduler
		hostRateLimiter quotas.RateLimiter
	}

	transferQueueProcessorFactory struct {
		transferQueueProcessorFactoryParams
		queueProcessorFactoryBase
	}

	timerQueueProcessorFactory struct {
		timerQueueProcessorFactoryParams
		queueProcessorFactoryBase
	}

	visibilityQueueProcessorFactory struct {
		visibilityQueueProcessorFactoryParams
		queueProcessorFactoryBase
	}

	QueueProcessorFactoriesLifetimeHookParams struct {
		fx.In

		Lifecycle fx.Lifecycle
		Factories []queues.ProcessorFactory `group:"queueProcessorFactory"`
	}
)

func QueueProcessorFactoryLifetimeHooks(
	params QueueProcessorFactoriesLifetimeHookParams,
) {
	params.Lifecycle.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				for _, factory := range params.Factories {
					factory.Start()
				}
				return nil
			},
			OnStop: func(context.Context) error {
				for _, factory := range params.Factories {
					factory.Stop()
				}
				return nil
			},
		},
	)
}

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
				params.MetricProvider,
			),
			queues.SchedulerOptions{
				ParallelProcessorOptions: ctasks.ParallelProcessorOptions{
					WorkerCount: params.Config.TransferProcessorSchedulerWorkerCount,
					QueueSize:   params.Config.TransferProcessorSchedulerQueueSize(),
				},
				InterleavedWeightedRoundRobinSchedulerOptions: ctasks.InterleavedWeightedRoundRobinSchedulerOptions{
					PriorityToWeight: configs.ConvertDynamicConfigValueToWeights(params.Config.TransferProcessorSchedulerRoundRobinWeights(), params.Logger),
				},
			},
			params.MetricProvider,
			params.Logger,
		)
	}
	return &transferQueueProcessorFactory{
		transferQueueProcessorFactoryParams: params,
		queueProcessorFactoryBase: queueProcessorFactoryBase{
			scheduler: scheduler,
			hostRateLimiter: newQueueProcessorHostRateLimiter(
				params.Config.TransferProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
			),
		},
	}
}

func (f *transferQueueProcessorFactory) CreateProcessor(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Processor {
	return newTransferQueueProcessor(
		shard,
		workflowCache,
		f.scheduler,
		f.ClientBean,
		f.ArchivalClient,
		f.SdkClientFactory,
		f.MatchingClient,
		f.HistoryClient,
		f.MetricProvider,
		f.hostRateLimiter,
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
				params.MetricProvider,
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
			params.MetricProvider,
			params.Logger,
		)
	}
	return &timerQueueProcessorFactory{
		timerQueueProcessorFactoryParams: params,
		queueProcessorFactoryBase: queueProcessorFactoryBase{
			scheduler: scheduler,
			hostRateLimiter: newQueueProcessorHostRateLimiter(
				params.Config.TimerProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
			),
		},
	}
}

func (f *timerQueueProcessorFactory) CreateProcessor(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Processor {
	return newTimerQueueProcessor(
		shard,
		workflowCache,
		f.scheduler,
		f.ClientBean,
		f.ArchivalClient,
		f.MatchingClient,
		f.MetricProvider,
		f.hostRateLimiter,
	)
}

func NewVisibilityQueueProcessorFactory(
	params visibilityQueueProcessorFactoryParams,
) queues.ProcessorFactory {
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
				params.MetricProvider,
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
			params.MetricProvider,
			params.Logger,
		)
	}
	return &visibilityQueueProcessorFactory{
		visibilityQueueProcessorFactoryParams: params,
		queueProcessorFactoryBase: queueProcessorFactoryBase{
			scheduler: scheduler,
			hostRateLimiter: newQueueProcessorHostRateLimiter(
				params.Config.VisibilityProcessorMaxPollHostRPS,
				params.Config.PersistenceMaxQPS,
			),
		},
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
		f.MetricProvider,
		f.hostRateLimiter,
	)
}

func (f *queueProcessorFactoryBase) Start() {
	if f.scheduler != nil {
		f.scheduler.Start()
	}
}

func (f *queueProcessorFactoryBase) Stop() {
	if f.scheduler != nil {
		f.scheduler.Stop()
	}
}

func newQueueProcessorHostRateLimiter(
	hostRPS dynamicconfig.IntPropertyFn,
	fallBackRPS dynamicconfig.IntPropertyFn,
) quotas.RateLimiter {
	return quotas.NewDefaultOutgoingRateLimiter(
		func() float64 {
			if maxPollHostRps := hostRPS(); maxPollHostRps > 0 {
				return float64(maxPollHostRps)
			}

			return float64(fallBackRPS())
		},
	)
}
