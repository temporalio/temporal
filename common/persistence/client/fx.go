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
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/faultinjection"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resolver"
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

		DataStoreFactory                   persistence.DataStoreFactory
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
	fx.Provide(DataStoreFactoryProvider),
	fx.Invoke(DataStoreFactoryLifetimeHooks),
	fx.Provide(managerProvider(Factory.NewClusterMetadataManager)),
	fx.Provide(managerProvider(Factory.NewMetadataManager)),
	fx.Provide(managerProvider(Factory.NewTaskManager)),
	fx.Provide(managerProvider(Factory.NewNamespaceReplicationQueue)),
	fx.Provide(managerProvider(Factory.NewShardManager)),
	fx.Provide(managerProvider(Factory.NewExecutionManager)),
	fx.Provide(managerProvider(Factory.NewHistoryTaskQueueManager)),
	fx.Provide(managerProvider(Factory.NewNexusEndpointManager)),

	fx.Provide(ClusterNameProvider),
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

func DataStoreFactoryProvider(
	clusterName ClusterName,
	r resolver.ServiceResolver,
	cfg *config.Persistence,
	abstractDataStoreFactory AbstractDataStoreFactory,
	logger log.Logger,
	metricsHandler metrics.Handler,
) persistence.DataStoreFactory {

	var dataStoreFactory persistence.DataStoreFactory
	defaultStoreCfg := cfg.DataStores[cfg.DefaultStore]
	switch {
	case defaultStoreCfg.Cassandra != nil:
		dataStoreFactory = cassandra.NewFactory(*defaultStoreCfg.Cassandra, r, string(clusterName), logger, metricsHandler)
	case defaultStoreCfg.SQL != nil:
		dataStoreFactory = sql.NewFactory(*defaultStoreCfg.SQL, r, string(clusterName), logger, metricsHandler)
	case defaultStoreCfg.CustomDataStoreConfig != nil:
		dataStoreFactory = abstractDataStoreFactory.NewFactory(*defaultStoreCfg.CustomDataStoreConfig, r, string(clusterName), logger, metricsHandler)
	default:
		logger.Fatal("invalid config: one of cassandra or sql params must be specified for default data store")
	}

	if defaultStoreCfg.FaultInjection != nil {
		dataStoreFactory = faultinjection.NewFaultInjectionDatastoreFactory(defaultStoreCfg.FaultInjection, dataStoreFactory)
	}

	return dataStoreFactory
}

func DataStoreFactoryLifetimeHooks(lc fx.Lifecycle, f persistence.DataStoreFactory) {
	lc.Append(fx.StopHook(f.Close))
}

func managerProvider[T persistence.Closeable](newManagerFn func(Factory) (T, error)) func(Factory, fx.Lifecycle) (T, error) {
	return func(f Factory, lc fx.Lifecycle) (T, error) {
		manager, err := newManagerFn(f) // passing receiver (Factory) as first argument.
		if err != nil {
			var nilT T
			return nilT, err
		}
		lc.Append(fx.StopHook(manager.Close))
		return manager, nil
	}
}
