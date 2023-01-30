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
	"time"

	"go.uber.org/fx"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	QueueFactoryFxGroup = "queueFactory"

	HostSchedulerMaxDispatchThrottleDuration  = 3 * time.Second
	ShardSchedulerMaxDispatchThrottleDuration = 5 * time.Second
)

type (
	QueueFactory interface {
		common.Daemon

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
	}

	QueueFactoryBase struct {
		HostScheduler         queues.Scheduler
		HostPriorityAssigner  queues.PriorityAssigner
		HostReaderRateLimiter quotas.RequestRateLimiter
	}

	QueueFactoriesLifetimeHookParams struct {
		fx.In

		Lifecycle fx.Lifecycle
		Factories []QueueFactory
	}
)

var QueueModule = fx.Options(
	fx.Provide(QueueSchedulerRateLimiterProvider),
	fx.Provide(
		fx.Annotated{
			Name:   "transferQueueFactory",
			Target: NewTransferQueueFactory,
		},
		fx.Annotated{
			Name:   "timerQueueFactory",
			Target: NewTimerQueueFactory,
		},
		fx.Annotated{
			Name:   "visibilityQueueFactory",
			Target: NewVisibilityQueueFactory,
		},
		fx.Annotated{
			Name:   "archivalQueueFactory",
			Target: NewArchivalQueueFactory,
		},
		getQueueFactories,
	),
	fx.Invoke(QueueFactoryLifetimeHooks),
)

type queueFactorySet struct {
	fx.In

	TransferQueueFactory   QueueFactory `name:"transferQueueFactory"`
	TimerQueueFactory      QueueFactory `name:"timerQueueFactory"`
	VisibilityQueueFactory QueueFactory `name:"visibilityQueueFactory"`
	ArchivalQueueFactory   QueueFactory `name:"archivalQueueFactory"`
}

// getQueueFactories returns factories for all the enabled queue types.
// The archival queue factory is only returned when archival is enabled in the static config.
func getQueueFactories(
	queueFactorySet queueFactorySet,
	archivalMetadata archiver.ArchivalMetadata,
) []QueueFactory {
	factories := []QueueFactory{
		queueFactorySet.TransferQueueFactory,
		queueFactorySet.TimerQueueFactory,
		queueFactorySet.VisibilityQueueFactory,
	}
	c := tasks.CategoryArchival
	// this will only affect tests because this method is only called once in production,
	// but it may be called many times across test runs, which would leave the archival queue as a dangling category
	tasks.RemoveCategory(c.ID())
	if archivalMetadata.GetHistoryConfig().StaticClusterState() == archiver.ArchivalEnabled || archivalMetadata.GetVisibilityConfig().StaticClusterState() == archiver.ArchivalEnabled {
		factories = append(factories, queueFactorySet.ArchivalQueueFactory)
		tasks.NewCategory(c.ID(), c.Type(), c.Name())
	}
	return factories
}

func QueueSchedulerRateLimiterProvider(
	config *configs.Config,
) queues.SchedulerRateLimiter {
	return queues.NewSchedulerRateLimiter(
		config.TaskSchedulerNamespaceMaxQPS,
		config.TaskSchedulerMaxQPS,
		config.PersistenceNamespaceMaxQPS,
		config.PersistenceMaxQPS,
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
