package history

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common"
	commoncache "go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/components/callbacks"
	hsmnexusoperations "go.temporal.io/server/components/nexusoperations"
	hsmnexusworkflow "go.temporal.io/server/components/nexusoperations/workflow"
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
	"go.temporal.io/server/service/worker/workerdeployment"
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
	fx.Provide(ServiceErrorInterceptorProvider),
	fx.Provide(RetryableInterceptorProvider),
	fx.Provide(ErrorHandlerProvider),
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(NamespaceRateLimitInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(HealthSignalAggregatorProvider),
	fx.Provide(HealthCheckInterceptorProvider),
	fx.Provide(ContextMetadataInterceptorProvider),
	fx.Provide(chasm.ChasmEngineInterceptorProvider),
	fx.Provide(chasm.ChasmVisibilityInterceptorProvider),
	fx.Provide(HistoryAdditionalInterceptorsProvider),
	fx.Provide(service.GrpcServerOptionsProvider),
	fx.Provide(ESProcessorConfigProvider),
	fx.Provide(VisibilityManagerProvider),
	fx.Provide(visibility.ChasmVisibilityManagerProvider),
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
	fx.Provide(VersionMembershipCacheProvider),
	workerdeployment.ClientModule,
	fx.Provide(RoutingInfoCacheProvider),
	fx.Invoke(ServiceLifetimeHooks),

	callbacks.Module,
	hsmnexusoperations.Module,
	fx.Invoke(hsmnexusworkflow.RegisterCommandHandlers),
	activity.HistoryModule,
	chasmnexus.Module,
	chasmworkflow.Module,
)

func ServerProvider(grpcServerOptions []grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(grpcServerOptions...)
}

func ServiceResolverProvider(
	membershipMonitor membership.Monitor,
) (membership.ServiceResolver, error) {
	return membershipMonitor.GetResolver(primitives.HistoryService)
}

func HandlerProvider(args NewHandlerArgs) (*Handler, error) {
	// Build and store the Nexus handler
	nexusHandler, err := buildNexusHandler(args.ChasmRegistry)
	if err != nil {
		return nil, err
	}

	handler := &Handler{
		status:          common.DaemonStatusInitialized,
		config:          args.Config,
		tokenSerializer: tasktoken.NewSerializer(),
		deepHealthCheckHandler: deepHealthCheckHandler{
			healthServer:            args.HealthServer,
			metricsHandler:          args.MetricsHandler,
			config:                  args.Config,
			historyHealthSignal:     args.HistoryHealthSignal,
			persistenceHealthSignal: args.PersistenceHealthSignal,
			startupTime:             time.Now(),
		},
		logger:                       args.Logger,
		throttledLogger:              args.ThrottledLogger,
		persistenceExecutionManager:  args.PersistenceExecutionManager,
		persistenceShardManager:      args.PersistenceShardManager,
		persistenceVisibilityManager: args.PersistenceVisibilityManager,
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
		chasmRegistry:                args.ChasmRegistry,

		replicationTaskFetcherFactory:    args.ReplicationTaskFetcherFactory,
		replicationTaskConverterProvider: args.ReplicationTaskConverterFactory,
		streamReceiverMonitor:            args.StreamReceiverMonitor,
		replicationServerRateLimiter:     args.ReplicationServerRateLimiter,
		nexusHandler:                     nexusHandler,
	}

	return handler, nil
}

func buildNexusHandler(chasmRegistry *chasm.Registry) (nexus.Handler, error) {
	nexusServices := chasmRegistry.NexusServices()
	if len(nexusServices) == 0 {
		return nil, nil
	}
	serviceRegistry := nexus.NewServiceRegistry()
	for _, svc := range nexusServices {
		// No chance of collision here since the registry would have errored out earlier.
		serviceRegistry.MustRegister(svc)
	}

	return serviceRegistry.NewHandler()
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

func ServiceErrorInterceptorProvider(
	dc *dynamicconfig.Collection,
) *interceptor.ServiceErrorInterceptor {
	return interceptor.NewServiceErrorInterceptor(
		dynamicconfig.MaxServiceErrorMessageLength.Get(dc),
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

func ErrorHandlerProvider(
	logger log.Logger,
	serviceConfig *configs.Config,
) *interceptor.RequestErrorHandler {
	return interceptor.NewRequestErrorHandler(
		logger,
		serviceConfig.LogAllReqErrors,
	)
}

func TelemetryInterceptorProvider(
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	serviceConfig *configs.Config,
	requestErrorHandler *interceptor.RequestErrorHandler,
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		namespaceRegistry,
		metricsHandler,
		logger,
		serviceConfig.LogAllReqErrors,
		requestErrorHandler,
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

func ContextMetadataInterceptorProvider(logger log.Logger) *interceptor.ContextMetadataInterceptor {
	return interceptor.NewContextMetadataInterceptor(true, logger)
}

func HistoryAdditionalInterceptorsProvider(
	healthCheckInterceptor *interceptor.HealthCheckInterceptor,
	chasmRequestEngineInterceptor *chasm.ChasmEngineInterceptor,
	chasmRequestVisibilityInterceptor *chasm.ChasmVisibilityInterceptor,
) []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		healthCheckInterceptor.UnaryIntercept,
		chasmRequestEngineInterceptor.Intercept,
		chasmRequestVisibilityInterceptor.Intercept,
	}
}

func NamespaceRateLimitInterceptorProvider(
	serviceConfig *configs.Config,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
) interceptor.NamespaceRateLimitInterceptor {

	namespaceRateFn := func(namespaceName string) float64 {
		if namespaceRPS := serviceConfig.NamespaceRPS(namespaceName); namespaceRPS > 0 {
			return float64(namespaceRPS)
		}
		// This fallback to host level rps limit when NamespaceRPS is not configured (i.e. 0)
		return float64(serviceConfig.RPS())
	}

	return interceptor.NewNamespaceRateLimitInterceptor(
		namespaceRegistry,
		configs.NewNamespaceRateLimiter(
			namespaceRateFn,
			serviceConfig.OperatorRPSRatio,
		),
		map[string]int{},      // no token overrides
		map[string]struct{}{}, // no long polls on history service
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false), // no long poll methods
		metricsHandler,
	)
}

func RateLimitInterceptorProvider(
	serviceConfig *configs.Config,
	ownershipBasedQuotaScaler shard.LazyLoadedOwnershipBasedQuotaScaler,
	metricsHandler metrics.Handler,
) *interceptor.RateLimitInterceptor {
	priorityFn, priorities := getFairnessPriorityFn(serviceConfig, ownershipBasedQuotaScaler, metricsHandler)
	return interceptor.NewRateLimitInterceptor(
		quotas.NewPriorityRateLimiterHelper(
			quotas.NewDefaultIncomingRateBurst(func() float64 { return float64(serviceConfig.RPS()) }),
			serviceConfig.OperatorRPSRatio,
			priorityFn,
			priorities,
		),
		map[string]int{
			healthpb.Health_Check_FullMethodName:                         0, // exclude health check requests from rate limiting.
			historyservice.HistoryService_DeepHealthCheck_FullMethodName: 0, // exclude deep health check requests from rate limiting.
		},
	)
}

// getFairnessPriorityFn builds the namespace-fairness-aware priority function
// for the history host RPS rate limiter, along with the priority list to
// pass to NewPriorityRateLimiterHelper.
//
// Priority layout (6 levels):
//
//	0  Operator           (in-share)
//	1  API                (in-share)
//	2  BgHigh             (in-share)
//	3  BgLow              (in-share)
//	4  over-share         (any caller type except Preemptable, over fair share)
//	5  Preemptable        (always, regardless of fairness or share state)
//
// All caller types except Preemptable participate in the fairness check:
// in-share traffic keeps its caller-type priority (including Operator at 0),
// over-share traffic — Operator included — collapses into the single
// over-share band at priority 4 and emits a demotion metric. Preemptable is
// outside the fairness model: it never consumes the namespace bucket, never
// produces a demotion metric, and always sinks to the bottom band.
//
// share(ns) = scaleFactor * FrontendGlobalNamespaceRPS(ns) * multiplier.
// OwnershipAwareNamespaceQuotaCalculator (with nil MemberCounter and
// PerInstanceQuota=0) returns scaleFactor*globalRPS when scaler is ready and
// a positive global RPS is configured, and 0 otherwise. The multiplier
// (normalized below to >0) doesn't change the sign, so GetQuota>0 alone is
// the "fairness applies" signal; the bucket rate folds in the multiplier.
func getFairnessPriorityFn(
	cfg *configs.Config,
	ownershipBasedQuotaScaler shard.LazyLoadedOwnershipBasedQuotaScaler,
	metricsHandler metrics.Handler,
) (quotas.RequestPriorityFn, []int) {
	const (
		overSharePriority   = 4
		preemptablePriority = 5
	)
	priorities := []int{
		quotas.OperatorPriority, 1, 2, 3,
		overSharePriority,
		preemptablePriority,
	}

	nsQuotaCalc := shard.NewOwnershipAwareNamespaceQuotaCalculator(
		ownershipBasedQuotaScaler,
		nil,
		func(string) int { return 0 },
		cfg.FrontendGlobalNamespaceRPS,
	)

	// Per-namespace fair-share buckets, sized at share(ns) with the standard
	// incoming burst ratio (= 2). Reuses NewNamespaceRequestRateLimiter (per-
	// namespace map keyed on req.Caller, 1-hour TTL eviction) and
	// NewDefaultIncomingRateLimiter (dynamic rate read live each call).
	nsBuckets := quotas.NewNamespaceRequestRateLimiter(
		func(req quotas.Request) quotas.RequestRateLimiter {
			ns := req.Caller
			return quotas.NewRequestRateLimiterAdapter(
				quotas.NewDefaultIncomingRateLimiter(func() float64 {
					mul := cfg.NamespaceFairShareMultiplier()
					if mul <= 0 {
						mul = 1
					}
					return nsQuotaCalc.GetQuota(ns) * mul
				}),
			)
		},
	)

	enabled := cfg.EnableNamespaceFairness
	priorityFn := func(req quotas.Request) int {
		callerTypePri := configs.RequestToPriority(req)
		if !enabled() {
			return callerTypePri
		}
		if req.CallerType == headers.CallerTypePreemptable {
			return preemptablePriority
		}
		if req.Caller == "" {
			return callerTypePri
		}
		if nsQuotaCalc.GetQuota(req.Caller) <= 0 {
			return callerTypePri
		}
		if nsBuckets.Allow(time.Now().UTC(), req) {
			return callerTypePri
		}
		metrics.ServiceRequestsNamespaceFairnessDemoted.With(metricsHandler).Record(
			1,
			metrics.NamespaceTag(req.Caller),
			metrics.StringTag("caller_type", req.CallerType),
		)
		return overSharePriority
	}
	return priorityFn, priorities
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
	chasmRegistry *chasm.Registry,
	serializer serialization.Serializer,
) (manager.VisibilityManager, error) {
	return visibility.NewManager(
		*persistenceConfig,
		persistenceServiceResolver,
		customVisibilityStoreFactory,
		esProcessorConfig,
		saProvider,
		searchAttributesMapperProvider,
		namespaceRegistry,
		chasmRegistry,
		serviceConfig.VisibilityPersistenceMaxReadQPS,
		serviceConfig.VisibilityPersistenceMaxWriteQPS,
		serviceConfig.OperatorRPSRatio,
		serviceConfig.VisibilityPersistenceSlowQueryThreshold,
		serviceConfig.EnableReadFromSecondaryVisibility,
		serviceConfig.VisibilityEnableShadowReadMode,
		serviceConfig.SecondaryVisibilityWritingMode,
		serviceConfig.VisibilityDisableOrderByClause,
		serviceConfig.VisibilityEnableManualPagination,
		serviceConfig.VisibilityEnableUnifiedQueryConverter,
		metricsHandler,
		logger,
		serializer,
	)
}

func ChasmVisibilityManagerProvider(
	chasmRegistry *chasm.Registry,
	nsRegistry namespace.Registry,
	visibilityManager manager.VisibilityManager,
) chasm.VisibilityManager {
	return visibility.NewChasmVisibilityManager(
		chasmRegistry,
		nsRegistry,
		visibilityManager,
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

func VersionMembershipCacheProvider(
	lc fx.Lifecycle,
	serviceConfig *configs.Config,
	metricsHandler metrics.Handler,
) worker_versioning.VersionMembershipAndReactivationStatusCache {
	c := commoncache.New(serviceConfig.VersionMembershipCacheMaxSize(), &commoncache.Options{
		TTL: max(1*time.Second, serviceConfig.VersionMembershipCacheTTL()),
	})
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			c.Stop()
			return nil
		},
	})
	return worker_versioning.NewVersionMembershipAndReactivationStatusCache(c, metricsHandler)
}

func RoutingInfoCacheProvider(
	lc fx.Lifecycle,
	serviceConfig *configs.Config,
	metricsHandler metrics.Handler,
) worker_versioning.RoutingInfoCache {
	c := commoncache.New(serviceConfig.RoutingInfoCacheMaxSize(), &commoncache.Options{
		TTL: max(1*time.Second, serviceConfig.RoutingInfoCacheTTL()),
	})
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			c.Stop()
			return nil
		},
	})
	return worker_versioning.NewRoutingInfoCache(c, metricsHandler)
}
