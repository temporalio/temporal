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

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
)

type (
	QueueFactoryBaseParams struct {
		fx.In

		NamespaceRegistry namespace.Registry
		ClusterMetadata   cluster.Metadata
		Config            *configs.Config
		MetricsHandler    metrics.MetricsHandler
		Logger            resource.SnTaggedLogger
	}

	QueueFactoryBase struct {
		HostScheduler        queues.Scheduler
		HostPriorityAssigner queues.PriorityAssigner
		HostRateLimiter      quotas.RateLimiter
	}

	QueueFactoriesLifetimeHookParams struct {
		fx.In

		Lifecycle fx.Lifecycle
		Factories []queues.Factory `group:"queueFactory"`
	}
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
