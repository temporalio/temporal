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

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const QueueFactoryFxGroup = "queueFactory"

type (
	QueueFactory interface {
		Start()
		Stop()

		// TODO:
		// 1. Remove the cache parameter after workflow cache become a host level component
		// and it can be provided as a parameter when creating a QueueFactory instance.
		// Currently, workflow cache is shard level, but we can't get it from shard or engine interface,
		// as that will lead to a cycle dependency issue between shard and workflow package.
		// 2. Move this interface to queues package after 1 is done so that there's no cycle dependency
		// between workflow and queues package.
		CreateQueue(shard shard.Context, cache wcache.Cache) queues.Queue
	}

	QueueFactoryBaseParams struct {
		fx.In

		NamespaceRegistry    namespace.Registry
		ClusterMetadata      cluster.Metadata
		Config               *configs.Config
		TimeSource           clock.TimeSource
		MetricsHandler       metrics.Handler
		Logger               log.SnTaggedLogger
		SchedulerRateLimiter queues.SchedulerRateLimiter

		ExecutorWrapper   queues.ExecutorWrapper   `optional:"true"`
		ExecutableWrapper queues.ExecutableWrapper `optional:"true"`
	}

	QueueFactoryBase struct {
		HostScheduler         queues.Scheduler
		HostPriorityAssigner  queues.PriorityAssigner
		HostReaderRateLimiter quotas.RequestRateLimiter
	}

	QueueFactoriesLifetimeHookParams struct {
		fx.In

		Lifecycle fx.Lifecycle
		Factories []QueueFactory `group:"queueFactory"`
	}
)

var QueueModule = fx.Options(
	fx.Provide(QueueSchedulerRateLimiterProvider),
	fx.Provide(NewExecutableDLQWrapper),
	fx.Provide(
		fx.Annotated{
			Group:  QueueFactoryFxGroup,
			Target: NewTransferQueueFactory,
		},
		fx.Annotated{
			Group:  QueueFactoryFxGroup,
			Target: NewTimerQueueFactory,
		},
		fx.Annotated{
			Group:  QueueFactoryFxGroup,
			Target: NewVisibilityQueueFactory,
		},
		fx.Annotated{
			Group:  QueueFactoryFxGroup,
			Target: NewMemoryScheduledQueueFactory,
		},
		getOptionalQueueFactories,
	),
	fx.Invoke(QueueFactoryLifetimeHooks),
)

// additionalQueueFactories is a container for a list of queue factories that are only added to the group if
// they are enabled. This exists because there is no way to conditionally add to a group with a provider that returns
// a single object. For example, this doesn't work because it will always add the factory to the group, which can
// cause NPEs:
//
//	fx.Annotated{
//	  Group: "queueFactory",
//	  Target: func() QueueFactory { return isEnabled ? NewQueueFactory() : nil },
//	},
type additionalQueueFactories struct {
	// This is what tells fx to add the factories to the group whenever this object is provided.
	fx.Out

	// Factories is a list of queue factories that will be added to the `group:"queueFactory"` group.
	Factories []QueueFactory `group:"queueFactory,flatten"`
}

// getOptionalQueueFactories returns an additionalQueueFactories which contains a list of queue factories that will be
// added to the `group:"queueFactory"` group. The factories are added to the group only if they are enabled, which
// is why we must return a list here.
func getOptionalQueueFactories(
	registry tasks.TaskCategoryRegistry,
	params ArchivalQueueFactoryParams,
) additionalQueueFactories {
	if _, ok := registry.GetCategoryByID(tasks.CategoryIDArchival); !ok {
		return additionalQueueFactories{}
	}
	return additionalQueueFactories{
		Factories: []QueueFactory{
			NewArchivalQueueFactory(params),
		},
	}
}

func QueueSchedulerRateLimiterProvider(
	ownershipBasedQuotaScaler shard.LazyLoadedOwnershipBasedQuotaScaler,
	serviceResolver membership.ServiceResolver,
	config *configs.Config,
	timeSource clock.TimeSource,
) (queues.SchedulerRateLimiter, error) {
	return queues.NewPrioritySchedulerRateLimiter(
		shard.NewOwnershipAwareNamespaceQuotaCalculator(
			ownershipBasedQuotaScaler,
			serviceResolver,
			config.TaskSchedulerNamespaceMaxQPS,
			config.TaskSchedulerGlobalNamespaceMaxQPS,
		).GetQuota,
		shard.NewOwnershipAwareQuotaCalculator(
			ownershipBasedQuotaScaler,
			serviceResolver,
			config.TaskSchedulerMaxQPS,
			config.TaskSchedulerGlobalMaxQPS,
		).GetQuota,
		shard.NewOwnershipAwareNamespaceQuotaCalculator(
			ownershipBasedQuotaScaler,
			serviceResolver,
			config.PersistenceNamespaceMaxQPS,
			config.PersistenceGlobalNamespaceMaxQPS,
		).GetQuota,
		shard.NewOwnershipAwareQuotaCalculator(
			ownershipBasedQuotaScaler,
			serviceResolver,
			config.PersistenceMaxQPS,
			config.PersistenceGlobalMaxQPS,
		).GetQuota,
	)
}

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

func (f *QueueFactoryBase) Start() {
	if f.HostScheduler != nil {
		f.HostScheduler.Start()
	}
}

func (f *QueueFactoryBase) Stop() {
	if f.HostScheduler != nil {
		f.HostScheduler.Stop()
	}
}

func (f *QueueFactoryBase) NewExecutableFactory(
	executor queues.Executor,
	scheduler queues.Scheduler,
	rescheduler queues.Rescheduler,
	executableWrapper queues.ExecutableWrapper,
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
) queues.ExecutableFactory {
	factory := queues.NewExecutableFactory(
		executor,
		scheduler,
		rescheduler,
		f.HostPriorityAssigner,
		timeSource,
		namespaceRegistry,
		clusterMetadata,
		logger,
		metricsHandler,
	)
	if executableWrapper == nil {
		return factory
	}
	return queues.NewExecutableFactoryWrapper(factory, executableWrapper)
}

func NewQueueHostRateLimiter(
	hostRPS dynamicconfig.IntPropertyFn,
	persistenceMaxRPS dynamicconfig.IntPropertyFn,
	persistenceMaxRPSRatio float64,
) quotas.RateLimiter {
	return quotas.NewDefaultOutgoingRateLimiter(
		NewHostRateLimiterRateFn(
			hostRPS,
			persistenceMaxRPS,
			persistenceMaxRPSRatio,
		),
	)
}

func NewHostRateLimiterRateFn(
	hostRPS dynamicconfig.IntPropertyFn,
	persistenceMaxRPS dynamicconfig.IntPropertyFn,
	persistenceMaxRPSRatio float64,
) quotas.RateFn {
	return func() float64 {
		if maxPollHostRps := hostRPS(); maxPollHostRps > 0 {
			return float64(maxPollHostRps)
		}

		// ensure queue loading won't consume all persistence tokens
		// especially upon host restart when we need to perform a load
		// for all shards
		return float64(persistenceMaxRPS()) * persistenceMaxRPSRatio
	}
}
