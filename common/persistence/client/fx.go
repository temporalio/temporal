package client

import (
	"errors"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/api/serviceerror"
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
	"go.temporal.io/server/common/persistence/telemetry"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resolver"
	otel "go.temporal.io/server/common/telemetry"
	"go.uber.org/fx"
)

type (
	PersistenceMaxQps                  dynamicconfig.IntPropertyFn
	PersistenceNamespaceMaxQps         dynamicconfig.IntPropertyFnWithNamespaceFilter
	PersistencePerShardNamespaceMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter
	OperatorRPSRatio                   dynamicconfig.FloatPropertyFn
	PersistenceBurstRatio              dynamicconfig.FloatPropertyFn

	DynamicRateLimitingParams dynamicconfig.TypedPropertyFn[dynamicconfig.DynamicRateLimitingParams]

	EnableDataLossMetrics                       dynamicconfig.BoolPropertyFn
	EnableBestEffortDeleteTasksOnWorkflowUpdate dynamicconfig.BoolPropertyFn

	ClusterName string

	NewFactoryParams struct {
		fx.In

		DataStoreFactory                            persistence.DataStoreFactory
		EventBlobCache                              persistence.XDCCache
		Cfg                                         *config.Persistence
		PersistenceMaxQPS                           PersistenceMaxQps
		PersistenceNamespaceMaxQPS                  PersistenceNamespaceMaxQps
		PersistencePerShardNamespaceMaxQPS          PersistencePerShardNamespaceMaxQPS
		OperatorRPSRatio                            OperatorRPSRatio
		PersistenceBurstRatio                       PersistenceBurstRatio
		ClusterName                                 ClusterName
		ServiceName                                 primitives.ServiceName
		MetricsHandler                              metrics.Handler
		Logger                                      log.Logger
		HealthSignals                               persistence.HealthSignalAggregator
		DynamicRateLimitingParams                   DynamicRateLimitingParams
		EnableDataLossMetrics                       EnableDataLossMetrics
		EnableBestEffortDeleteTasksOnWorkflowUpdate EnableBestEffortDeleteTasksOnWorkflowUpdate
	}

	FactoryProviderFn func(NewFactoryParams) Factory
)

var Module = fx.Options(
	fx.Provide(DataStoreFactoryProvider),
	fx.Invoke(DataStoreFactoryLifetimeHooks),
	fx.Provide(managerProvider(Factory.NewClusterMetadataManager)),
	fx.Provide(managerProvider(Factory.NewMetadataManager)),
	fx.Provide(managerProvider(Factory.NewTaskManager)),
	fx.Provide(managerProvider(Factory.NewFairTaskManager)),
	fx.Provide(managerProvider(Factory.NewNamespaceReplicationQueue)),
	fx.Provide(managerProvider(Factory.NewShardManager)),
	fx.Provide(managerProvider(Factory.NewExecutionManager)),
	fx.Provide(managerProvider(Factory.NewHistoryTaskQueueManager)),
	fx.Provide(managerProvider(Factory.NewNexusEndpointManager)),

	fx.Provide(ClusterNameProvider),
	fx.Provide(HealthSignalAggregatorProvider),
	fx.Provide(persistence.NewDLQMetricsEmitter),
	fx.Provide(EventBlobCacheProvider),
	fx.Provide(EnableDataLossMetricsProvider),
	fx.Provide(EnableBestEffortDeleteTasksOnWorkflowUpdateProvider),
)

func ClusterNameProvider(config *cluster.Config) ClusterName {
	return ClusterName(config.CurrentClusterName)
}

func EventBlobCacheProvider(
	dc *dynamicconfig.Collection,
	logger log.Logger,
) persistence.XDCCache {
	return persistence.NewEventsBlobCache(
		dynamicconfig.XDCCacheMaxSizeBytes.Get(dc)(),
		20*time.Second,
		logger,
	)
}

func EnableDataLossMetricsProvider(
	dc *dynamicconfig.Collection,
) EnableDataLossMetrics {
	return EnableDataLossMetrics(dynamicconfig.EnableDataLossMetrics.Get(dc))
}

func EnableBestEffortDeleteTasksOnWorkflowUpdateProvider(
	dc *dynamicconfig.Collection,
) EnableBestEffortDeleteTasksOnWorkflowUpdate {
	return EnableBestEffortDeleteTasksOnWorkflowUpdate(dynamicconfig.EnableBestEffortDeleteTasksOnWorkflowUpdate.Get(dc))
}

func FactoryProvider(
	params NewFactoryParams,
) Factory {
	var systemRequestRateLimiter, namespaceRequestRateLimiter, shardRequestRateLimiter quotas.RequestRateLimiter
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
			RequestPriorityFn,
			params.OperatorRPSRatio,
			params.PersistenceBurstRatio,
		)
		shardRequestRateLimiter = NewPriorityNamespaceShardRateLimiter(
			params.PersistenceMaxQPS,
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
		shardRequestRateLimiter,
		serialization.NewSerializer(),
		params.EventBlobCache,
		string(params.ClusterName),
		params.MetricsHandler,
		params.Logger,
		params.HealthSignals,
		params.EnableDataLossMetrics,
		params.EnableBestEffortDeleteTasksOnWorkflowUpdate,
	)
}

func HealthSignalAggregatorProvider(
	dynamicCollection *dynamicconfig.Collection,
	metricsHandler metrics.Handler,
	logger log.ThrottledLogger,
) persistence.HealthSignalAggregator {
	if dynamicconfig.PersistenceHealthSignalMetricsEnabled.Get(dynamicCollection)() {
		return persistence.NewHealthSignalAggregator(
			dynamicconfig.PersistenceHealthSignalAggregationEnabled.Get(dynamicCollection)(),
			dynamicconfig.PersistenceHealthSignalWindowSize.Get(dynamicCollection)(),
			dynamicconfig.PersistenceHealthSignalBufferSize.Get(dynamicCollection)(),
			metricsHandler,
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
	tracerProvider trace.TracerProvider,
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

	tracer := tracerProvider.Tracer(otel.ComponentPersistence)
	if otel.IsEnabled(tracer) {
		dataStoreFactory = telemetry.NewTelemetryDataStoreFactory(dataStoreFactory, logger, tracer)
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
			var unimpl *serviceerror.Unimplemented
			if errors.As(err, &unimpl) {
				// allow factories to return Unimplemented, and turn into nil so that fx init doesn't fail.
				err = nil
			}
			var nilT T
			return nilT, err
		}
		lc.Append(fx.StopHook(manager.Close))
		return manager, nil
	}
}
