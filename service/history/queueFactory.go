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

var QueueModule = fx.Options(
	fx.Provide(
		fx.Annotated{
			Group:  queues.FactoryFxGroup,
			Target: NewTransferQueueFactory,
		},
		fx.Annotated{
			Group:  queues.FactoryFxGroup,
			Target: NewTimerQueueFactory,
		},
		fx.Annotated{
			Group:  queues.FactoryFxGroup,
			Target: NewVisibilityQueueFactory,
		},
	),
	fx.Invoke(QueueFactoryLifetimeHooks),
)

type (
	SchedulerParams struct {
		fx.In

		NamespaceRegistry namespace.Registry
		ClusterMetadata   cluster.Metadata
		Config            *configs.Config
		MetricsHandler    metrics.MetricsHandler
		Logger            resource.SnTaggedLogger
	}

	transferQueueFactoryParams struct {
		fx.In

		SchedulerParams

		ClientBean       client.Bean
		ArchivalClient   archiver.Client
		SdkClientFactory sdk.ClientFactory
		MatchingClient   resource.MatchingClient
		HistoryClient    historyservice.HistoryServiceClient
		MetricsHandler   metrics.MetricsHandler
	}

	timerQueueFactoryParams struct {
		fx.In

		SchedulerParams

		ClientBean     client.Bean
		ArchivalClient archiver.Client
		MatchingClient resource.MatchingClient
		MetricsHandler metrics.MetricsHandler
	}

	visibilityQueueFactoryParams struct {
		fx.In

		SchedulerParams

		VisibilityMgr  manager.VisibilityManager
		MetricsHandler metrics.MetricsHandler
	}

	queueFactoryBase struct {
		scheduler       queues.Scheduler
		hostRateLimiter quotas.RateLimiter
	}

	transferQueueFactory struct {
		transferQueueFactoryParams
		queueFactoryBase
	}

	timerQueueFactory struct {
		timerQueueFactoryParams
		queueFactoryBase
	}

	visibilityQueueFactory struct {
		visibilityQueueFactoryParams
		queueFactoryBase
	}

	QueueFactoriesLifetimeHookParams struct {
		fx.In

		Lifecycle fx.Lifecycle
		Factories []queues.Factory `group:"queueFactory"`
	}
)

func QueueFactoryLifetimeHooks(
	params QueueFactoriesLifetimeHookParams,
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

func NewTransferQueueFactory(
	params transferQueueFactoryParams,
) queues.Factory {
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
				params.MetricsHandler,
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
			params.MetricsHandler,
			params.Logger,
		)
	}
	return &transferQueueFactory{
		transferQueueFactoryParams: params,
		queueFactoryBase: queueFactoryBase{
			scheduler: scheduler,
			hostRateLimiter: newQueueHostRateLimiter(
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
	return newTransferQueueProcessor(
		shard,
		workflowCache,
		f.scheduler,
		f.ClientBean,
		f.ArchivalClient,
		f.SdkClientFactory,
		f.MatchingClient,
		f.HistoryClient,
		f.MetricsHandler,
		f.hostRateLimiter,
	)
}

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
	return newVisibilityQueueProcessor(
		shard,
		workflowCache,
		f.scheduler,
		f.VisibilityMgr,
		f.MetricsHandler,
		f.hostRateLimiter,
	)
}

func (f *queueFactoryBase) Start() {
	if f.scheduler != nil {
		f.scheduler.Start()
	}
}

func (f *queueFactoryBase) Stop() {
	if f.scheduler != nil {
		f.scheduler.Stop()
	}
}

func newQueueHostRateLimiter(
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
