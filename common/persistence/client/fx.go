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

package client

import (
	"time"

	"go.uber.org/fx"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
)

type (
	PersistenceMaxQps                  dynamicconfig.IntPropertyFn
	PersistenceNamespaceMaxQps         dynamicconfig.IntPropertyFnWithNamespaceFilter
	PersistencePerShardNamespaceMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter
	OperatorRPSRatio                   dynamicconfig.FloatPropertyFn
	PersistenceBurstRatio              dynamicconfig.FloatPropertyFn

	DynamicRateLimitingParams dynamicconfig.MapPropertyFn

	ClusterName string

	NewFactoryParams struct {
		fx.In

		DataStoreFactory                   DataStoreFactory
		EventBlobCache                     persistence.XDCCache
		Cfg                                *config.Persistence
		PersistenceMaxQPS                  PersistenceMaxQps
		PersistenceNamespaceMaxQPS         PersistenceNamespaceMaxQps
		PersistencePerShardNamespaceMaxQPS PersistencePerShardNamespaceMaxQPS
		OperatorRPSRatio                   OperatorRPSRatio
		PersistenceBurstRatio              PersistenceBurstRatio
		ClusterName                        ClusterName
		ServiceName                        primitives.ServiceName
		MetricsHandler                     metrics.Handler
		Logger                             log.Logger
		HealthSignals                      persistence.HealthSignalAggregator
		DynamicRateLimitingParams          DynamicRateLimitingParams
	}

	FactoryProviderFn func(NewFactoryParams) Factory
)

var Module = fx.Options(
	fx.Provide(ClusterMetadataManagerProvider),
	fx.Invoke(ClusterMetadataManagerLifetimeHooks),
	fx.Provide(MetadataManagerProvider),
	fx.Invoke(MetadataManagerLifetimeHooks),
	fx.Provide(TaskManagerProvider),
	fx.Invoke(TaskManagerLifetimeHooks),
	fx.Provide(NamespaceReplicationQueueProvider),
	fx.Invoke(NamespaceReplicationQueueLifetimeHooks),
	fx.Provide(ShardManagerProvider),
	fx.Invoke(ShardManagerLifetimeHooks),
	fx.Provide(ExecutionManagerProvider),
	fx.Invoke(ExecutionManagerLifetimeHooks),
	fx.Provide(HistoryTaskQueueManagerProvider),
	fx.Provide(NexusEndpointManagerProvider),
	fx.Invoke(NexusEndpointManagerLifetimeHooks),
	fx.Provide(ClusterNameProvider),
	fx.Provide(DataStoreFactoryProvider),
	fx.Invoke(DataStoreFactoryLifetimeHooks),
	fx.Provide(HealthSignalAggregatorProvider),
	fx.Provide(EventBlobCacheProvider),
)

func ClusterNameProvider(config *cluster.Config) ClusterName {
	return ClusterName(config.CurrentClusterName)
}

func EventBlobCacheProvider(
	dc *dynamicconfig.Collection,
) persistence.XDCCache {
	return persistence.NewEventsBlobCache(
		dynamicconfig.XDCCacheMaxSizeBytes.Get(dc)(),
		20*time.Second,
	)
}

func FactoryProvider(
	params NewFactoryParams,
) Factory {
	var systemRequestRateLimiter, namespaceRequestRateLimiter quotas.RequestRateLimiter
	if params.PersistenceMaxQPS != nil && params.PersistenceMaxQPS() > 0 {
		systemRequestRateLimiter = NewPriorityRateLimiter(
			params.PersistenceMaxQPS,
			RequestPriorityFn,
			params.OperatorRPSRatio,
			params.PersistenceBurstRatio,
			params.HealthSignals,
			params.DynamicRateLimitingParams,
			params.MetricsHandler,
			params.Logger,
		)
		namespaceRequestRateLimiter = NewPriorityNamespaceRateLimiter(
			params.PersistenceMaxQPS,
			params.PersistenceNamespaceMaxQPS,
			params.PersistencePerShardNamespaceMaxQPS,
			RequestPriorityFn,
			params.OperatorRPSRatio,
			params.PersistenceBurstRatio,
		)
	}

	return NewFactory(
		params.DataStoreFactory,
		params.Cfg,
		systemRequestRateLimiter,
		namespaceRequestRateLimiter,
		serialization.NewSerializer(),
		params.EventBlobCache,
		string(params.ClusterName),
		params.MetricsHandler,
		params.Logger,
		params.HealthSignals,
	)
}

func HealthSignalAggregatorProvider(
	dynamicCollection *dynamicconfig.Collection,
	metricsHandler metrics.Handler,
	logger log.ThrottledLogger,
) persistence.HealthSignalAggregator {
	if dynamicconfig.PersistenceHealthSignalMetricsEnabled.Get(dynamicCollection)() {
		return persistence.NewHealthSignalAggregatorImpl(
			dynamicconfig.PersistenceHealthSignalAggregationEnabled.Get(dynamicCollection)(),
			dynamicconfig.PersistenceHealthSignalWindowSize.Get(dynamicCollection)(),
			dynamicconfig.PersistenceHealthSignalBufferSize.Get(dynamicCollection)(),
			metricsHandler,
			dynamicconfig.ShardRPSWarnLimit.Get(dynamicCollection),
			dynamicconfig.ShardPerNsRPSWarnPercent.Get(dynamicCollection),
			logger,
		)
	}

	return persistence.NoopHealthSignalAggregator
}

func ClusterMetadataManagerProvider(factory Factory) (persistence.ClusterMetadataManager, error) {
	return factory.NewClusterMetadataManager()
}
func ClusterMetadataManagerLifetimeHooks(
	lc fx.Lifecycle,
	manager persistence.ClusterMetadataManager,
) {
	lc.Append(fx.StopHook(manager.Close))
}

func MetadataManagerProvider(factory Factory) (persistence.MetadataManager, error) {
	return factory.NewMetadataManager()
}
func MetadataManagerLifetimeHooks(
	lc fx.Lifecycle,
	manager persistence.MetadataManager,
) {
	lc.Append(fx.StopHook(manager.Close))
}

func TaskManagerProvider(factory Factory) (persistence.TaskManager, error) {
	return factory.NewTaskManager()
}
func TaskManagerLifetimeHooks(
	lc fx.Lifecycle,
	manager persistence.TaskManager,
) {
	lc.Append(fx.StopHook(manager.Close))
}

func NamespaceReplicationQueueProvider(factory Factory) (persistence.NamespaceReplicationQueue, error) {
	return factory.NewNamespaceReplicationQueue()
}
func NamespaceReplicationQueueLifetimeHooks(
	lc fx.Lifecycle,
	manager persistence.NamespaceReplicationQueue,
) {
	lc.Append(fx.StopHook(manager.Stop))
}

func ShardManagerProvider(factory Factory) (persistence.ShardManager, error) {
	return factory.NewShardManager()
}
func ShardManagerLifetimeHooks(
	lc fx.Lifecycle,
	manager persistence.ShardManager,
) {
	lc.Append(fx.StopHook(manager.Close))
}

func ExecutionManagerProvider(factory Factory) (persistence.ExecutionManager, error) {
	return factory.NewExecutionManager()
}
func ExecutionManagerLifetimeHooks(
	lc fx.Lifecycle,
	manager persistence.ExecutionManager,
) {
	lc.Append(fx.StopHook(manager.Close))
}

func HistoryTaskQueueManagerProvider(factory Factory) (persistence.HistoryTaskQueueManager, error) {
	return factory.NewHistoryTaskQueueManager()
}

func NexusEndpointManagerProvider(factory Factory) (persistence.NexusEndpointManager, error) {
	return factory.NewNexusEndpointManager()
}
func NexusEndpointManagerLifetimeHooks(
	lc fx.Lifecycle,
	manager persistence.NexusEndpointManager,
) {
	lc.Append(fx.StopHook(manager.Close))
}
