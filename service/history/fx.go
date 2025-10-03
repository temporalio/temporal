package history

import (
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	nexusworkflow "go.temporal.io/server/components/nexusoperations/workflow"
	"go.temporal.io/server/service"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var Module = fx.Options(
	resource.Module,
	fx.Provide(hsm.NewRegistry),
	workflow.Module,
	shard.Module,
	events.Module,
	cache.Module,
	archival.Module,
	ChasmEngineModule,
	fx.Provide(ConfigProvider), // might be worth just using provider for configs.Config directly
	fx.Provide(workflow.NewCommandHandlerRegistry),
	fx.Provide(RetryableInterceptorProvider),
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(HealthSignalAggregatorProvider),
	fx.Provide(HealthCheckInterceptorProvider),
	fx.Provide(chasm.ChasmRequestInterceptorProvider),
	fx.Provide(HistoryAdditionalInterceptorsProvider),
	fx.Provide(service.GrpcServerOptionsProvider),
	fx.Provide(ESProcessorConfigProvider),
	fx.Provide(VisibilityManagerProvider),
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(PersistenceRateLimitingParamsProvider),
	service.PersistenceLazyLoadedServiceResolverModule,
	fx.Provide(ServiceResolverProvider),
	fx.Provide(EventNotifierProvider),
	fx.Provide(HistoryEngineFactoryProvider),
	fx.Provide(HandlerProvider),
	fx.Provide(ServerProvider),
	fx.Provide(NewService),
	fx.Provide(ReplicationProgressCacheProvider),
	fx.Invoke(ServiceLifetimeHooks),

	callbacks.Module,
	nexusoperations.Module,
	fx.Invoke(nexusworkflow.RegisterCommandHandlers),
)

func ServerProvider(grpcServerOptions []grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(grpcServerOptions...)
}

func ServiceResolverProvider(
	membershipMonitor membership.Monitor,
) (membership.ServiceResolver, error) {
	return membershipMonitor.GetResolver(primitives.HistoryService)
}

func HandlerProvider(args NewHandlerArgs) *Handler {
	handler := &Handler{
		status:                       common.DaemonStatusInitialized,
		config:                       args.Config,
		tokenSerializer:              tasktoken.NewSerializer(),
		logger:                       args.Logger,
		throttledLogger:              args.ThrottledLogger,
		persistenceExecutionManager:  args.PersistenceExecutionManager,
		persistenceShardManager:      args.PersistenceShardManager,
		persistenceVisibilityManager: args.PersistenceVisibilityManager,
		persistenceHealthSignal:      args.PersistenceHealthSignal,
		healthServer:                 args.HealthServer,
		historyHealthSignal:          args.HistoryHealthSignal,
		historyServiceResolver:       args.HistoryServiceResolver,
		metricsHandler:               args.MetricsHandler,
		payloadSerializer:            args.PayloadSerializer,
		timeSource:                   args.TimeSource,
		namespaceRegistry:            args.NamespaceRegistry,
		saProvider:                   args.SaProvider,
		clusterMetadata:              args.ClusterMetadata,
		archivalMetadata:             args.ArchivalMetadata,
		hostInfoProvider:             args.HostInfoProvider,
		controller:                   args.ShardController,
		eventNotifier:                args.EventNotifier,
		tracer:                       args.TracerProvider.Tracer(consts.LibraryName),
		taskQueueManager:             args.TaskQueueManager,
		taskCategoryRegistry:         args.TaskCategoryRegistry,
		dlqMetricsEmitter:            args.DLQMetricsEmitter,
		chasmEngine:                  args.ChasmEngine,

		replicationTaskFetcherFactory:    args.ReplicationTaskFetcherFactory,
		replicationTaskConverterProvider: args.ReplicationTaskConverterFactory,
		streamReceiverMonitor:            args.StreamReceiverMonitor,
		replicationServerRateLimiter:     args.ReplicationServerRateLimiter,
	}

	return handler
}

func HistoryEngineFactoryProvider(
	params HistoryEngineFactoryParams,
) shard.EngineFactory {
	return &historyEngineFactory{
		HistoryEngineFactoryParams: params,
	}
}

func ConfigProvider(
	dc *dynamicconfig.Collection,
	persistenceConfig config.Persistence,
) *configs.Config {
	return configs.NewConfig(
		dc,
		persistenceConfig.NumHistoryShards,
	)
}

func ThrottledLoggerRpsFnProvider(serviceConfig *configs.Config) resource.ThrottledLoggerRpsFn {
	return func() float64 { return float64(serviceConfig.ThrottledLogRPS()) }
}

func RetryableInterceptorProvider() *interceptor.RetryableInterceptor {
	return interceptor.NewRetryableInterceptor(
		common.CreateHistoryHandlerRetryPolicy(),
		api.IsRetryableError,
	)
}

func TelemetryInterceptorProvider(
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	serviceConfig *configs.Config,
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		namespaceRegistry,
		metricsHandler,
		logger,
		serviceConfig.LogAllReqErrors,
	)
}

func HealthSignalAggregatorProvider(
	dynamicCollection *dynamicconfig.Collection,
	logger log.ThrottledLogger,
) interceptor.HealthSignalAggregator {
	return interceptor.NewHealthSignalAggregator(
		logger,
		dynamicconfig.HistoryHealthSignalMetricsEnabled.Get(dynamicCollection),
		dynamicconfig.PersistenceHealthSignalWindowSize.Get(dynamicCollection)(),
		dynamicconfig.PersistenceHealthSignalBufferSize.Get(dynamicCollection)(),
	)
}

func HealthCheckInterceptorProvider(
	healthSignalAggregator interceptor.HealthSignalAggregator,
) *interceptor.HealthCheckInterceptor {
	return interceptor.NewHealthCheckInterceptor(
		healthSignalAggregator,
	)
}

func HistoryAdditionalInterceptorsProvider(
	healthCheckInterceptor *interceptor.HealthCheckInterceptor, chasmRequestInterceptor *chasm.ChasmRequestInterceptor,
) []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{healthCheckInterceptor.UnaryIntercept, chasmRequestInterceptor.Intercept}
}

func RateLimitInterceptorProvider(
	serviceConfig *configs.Config,
) *interceptor.RateLimitInterceptor {
	return interceptor.NewRateLimitInterceptor(
		configs.NewPriorityRateLimiter(func() float64 { return float64(serviceConfig.RPS()) }, serviceConfig.OperatorRPSRatio),
		map[string]int{
			healthpb.Health_Check_FullMethodName:                         0, // exclude health check requests from rate limiting.
			historyservice.HistoryService_DeepHealthCheck_FullMethodName: 0, // exclude deep health check requests from rate limiting.
		},
	)
}

func ESProcessorConfigProvider(
	serviceConfig *configs.Config,
) *elasticsearch.ProcessorConfig {
	return &elasticsearch.ProcessorConfig{
		IndexerConcurrency:       serviceConfig.IndexerConcurrency,
		ESProcessorNumOfWorkers:  serviceConfig.ESProcessorNumOfWorkers,
		ESProcessorBulkActions:   serviceConfig.ESProcessorBulkActions,
		ESProcessorBulkSize:      serviceConfig.ESProcessorBulkSize,
		ESProcessorFlushInterval: serviceConfig.ESProcessorFlushInterval,
		ESProcessorAckTimeout:    serviceConfig.ESProcessorAckTimeout,
	}
}

func PersistenceRateLimitingParamsProvider(
	serviceConfig *configs.Config,
	persistenceLazyLoadedServiceResolver service.PersistenceLazyLoadedServiceResolver,
	ownershipBasedQuotaScaler shard.LazyLoadedOwnershipBasedQuotaScaler,
	logger log.SnTaggedLogger,
) service.PersistenceRateLimitingParams {
	hostCalculator := calculator.NewLoggedCalculator(
		shard.NewOwnershipAwareQuotaCalculator(
			ownershipBasedQuotaScaler,
			persistenceLazyLoadedServiceResolver,
			serviceConfig.PersistenceMaxQPS,
			serviceConfig.PersistenceGlobalMaxQPS,
		),
		log.With(logger, tag.ComponentPersistence, tag.ScopeHost),
	)
	namespaceCalculator := calculator.NewLoggedNamespaceCalculator(
		shard.NewOwnershipAwareNamespaceQuotaCalculator(
			ownershipBasedQuotaScaler,
			persistenceLazyLoadedServiceResolver,
			serviceConfig.PersistenceNamespaceMaxQPS,
			serviceConfig.PersistenceGlobalNamespaceMaxQPS,
		),
		log.With(logger, tag.ComponentPersistence, tag.ScopeNamespace),
	)
	return service.PersistenceRateLimitingParams{
		PersistenceMaxQps: func() int {
			return int(hostCalculator.GetQuota())
		},
		PersistenceNamespaceMaxQps: func(namespace string) int {
			return int(namespaceCalculator.GetQuota(namespace))
		},
		PersistencePerShardNamespaceMaxQPS: persistenceClient.PersistencePerShardNamespaceMaxQPS(serviceConfig.PersistencePerShardNamespaceMaxQPS),
		OperatorRPSRatio:                   persistenceClient.OperatorRPSRatio(serviceConfig.OperatorRPSRatio),
		PersistenceBurstRatio:              persistenceClient.PersistenceBurstRatio(serviceConfig.PersistenceQPSBurstRatio),
		DynamicRateLimitingParams:          persistenceClient.DynamicRateLimitingParams(serviceConfig.PersistenceDynamicRateLimitingParams),
	}
}

func VisibilityManagerProvider(
	logger log.Logger,
	metricsHandler metrics.Handler,
	persistenceConfig *config.Persistence,
	customVisibilityStoreFactory visibility.VisibilityStoreFactory,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	serviceConfig *configs.Config,
	persistenceServiceResolver resolver.ServiceResolver,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	saProvider searchattribute.Provider,
	namespaceRegistry namespace.Registry,
) (manager.VisibilityManager, error) {
	return visibility.NewManager(
		*persistenceConfig,
		persistenceServiceResolver,
		customVisibilityStoreFactory,
		esProcessorConfig,
		saProvider,
		searchAttributesMapperProvider,
		namespaceRegistry,
		serviceConfig.VisibilityPersistenceMaxReadQPS,
		serviceConfig.VisibilityPersistenceMaxWriteQPS,
		serviceConfig.OperatorRPSRatio,
		serviceConfig.VisibilityPersistenceSlowQueryThreshold,
		serviceConfig.EnableReadFromSecondaryVisibility,
		serviceConfig.VisibilityEnableShadowReadMode,
		serviceConfig.SecondaryVisibilityWritingMode,
		serviceConfig.VisibilityDisableOrderByClause,
		serviceConfig.VisibilityEnableManualPagination,
		metricsHandler,
		logger,
	)
}

func EventNotifierProvider(
	timeSource clock.TimeSource,
	metricsHandler metrics.Handler,
	config *configs.Config,
) events.Notifier {
	return events.NewNotifier(
		timeSource,
		metricsHandler,
		config.GetShardID,
	)
}

func ServiceLifetimeHooks(lc fx.Lifecycle, svc *Service) {
	lc.Append(fx.StartStopHook(svc.Start, svc.Stop))
}

func ReplicationProgressCacheProvider(
	serviceConfig *configs.Config,
	logger log.Logger,
	handler metrics.Handler,
) replication.ProgressCache {
	return replication.NewProgressCache(serviceConfig, logger, handler)
}
