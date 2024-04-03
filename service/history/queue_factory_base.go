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

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas/calculator"
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
		DLQWriter            *queues.DLQWriter
		ExecutorWrapper      queues.ExecutorWrapper `optional:"true"`
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
	fx.Provide(
		QueueSchedulerRateLimiterProvider,
		func(tqm persistence.HistoryTaskQueueManager) queues.QueueWriter {
			return tqm
		},
		queues.NewDLQWriter,
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
	archivalParams ArchivalQueueFactoryParams,
	callbackParams outboundQueueFactoryParams,
	config *configs.Config,
) additionalQueueFactories {
	factories := []QueueFactory{}
	if _, ok := registry.GetCategoryByID(tasks.CategoryIDArchival); ok {
		factories = append(factories, NewArchivalQueueFactory(archivalParams))
	}
	if _, ok := registry.GetCategoryByID(tasks.CategoryIDOutbound); ok {
		factories = append(factories, NewOutboundQueueFactory(callbackParams))
	}
	return additionalQueueFactories{
		Factories: factories,
	}
}

func QueueSchedulerRateLimiterProvider(
	ownershipBasedQuotaScaler shard.LazyLoadedOwnershipBasedQuotaScaler,
	serviceResolver membership.ServiceResolver,
	config *configs.Config,
	timeSource clock.TimeSource,
	logger log.SnTaggedLogger,
) (queues.SchedulerRateLimiter, error) {
	return queues.NewPrioritySchedulerRateLimiter(
		calculator.NewLoggedNamespaceCalculator(
			shard.NewOwnershipAwareNamespaceQuotaCalculator(
				ownershipBasedQuotaScaler,
				serviceResolver,
				config.TaskSchedulerNamespaceMaxQPS,
				config.TaskSchedulerGlobalNamespaceMaxQPS,
			),
			log.With(logger, tag.ComponentTaskScheduler, tag.ScopeNamespace),
		).GetQuota,
		calculator.NewLoggedCalculator(
			shard.NewOwnershipAwareQuotaCalculator(
				ownershipBasedQuotaScaler,
				serviceResolver,
				config.TaskSchedulerMaxQPS,
				config.TaskSchedulerGlobalMaxQPS,
			),
			log.With(logger, tag.ComponentTaskScheduler, tag.ScopeHost),
		).GetQuota,
		// TODO: reuse persistence rate limit calculator in PersistenceRateLimitingParamsProvider
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

func NewHostRateLimiterRateFn(
	hostRPS dynamicconfig.IntPropertyFn,
	persistenceMaxRPS dynamicconfig.IntPropertyFn,
	persistenceMaxRPSRatio float64,
) quotas.RateFn {
	// TODO: reuse persistence rate limit calculator in PersistenceRateLimitingParamsProvider

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
