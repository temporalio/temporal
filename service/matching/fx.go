package matching

import (
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service"
	"go.temporal.io/server/service/matching/configs"
	"go.temporal.io/server/service/matching/workers"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var Module = fx.Options(
	resource.Module,
	workerdeployment.Module,
	fx.Provide(ConfigProvider),
	fx.Provide(PersistenceRateLimitingParamsProvider),
	service.PersistenceLazyLoadedServiceResolverModule,
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(ServiceErrorInterceptorProvider),
	fx.Provide(ContextMetadataInterceptorProvider),
	fx.Provide(RetryableInterceptorProvider),
	fx.Provide(ErrorHandlerProvider),
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(NamespaceRateLimitInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(VisibilityManagerProvider),
	fx.Provide(WorkersRegistryProvider),
	fx.Provide(NewHandler),
	fx.Provide(service.GrpcServerOptionsProvider),
	fx.Provide(NamespaceReplicationQueueProvider),
	fx.Provide(ServiceResolverProvider),
	fx.Provide(ServerProvider),
	fx.Provide(NewService),
	fx.Invoke(ServiceLifetimeHooks),
)

func ServerProvider(grpcServerOptions []grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(grpcServerOptions...)
}

func ConfigProvider(
	dc *dynamicconfig.Collection,
	persistenceConfig config.Persistence,
) *Config {
	return NewConfig(dc)
}

func ServiceErrorInterceptorProvider(
	dc *dynamicconfig.Collection,
) *interceptor.ServiceErrorInterceptor {
	return interceptor.NewServiceErrorInterceptor(
		dynamicconfig.MaxServiceErrorMessageLength.Get(dc),
	)
}

func RetryableInterceptorProvider() *interceptor.RetryableInterceptor {
	return interceptor.NewRetryableInterceptor(
		common.CreateMatchingHandlerRetryPolicy(),
		common.IsServiceHandlerRetryableError,
	)
}

func ErrorHandlerProvider(
	logger log.Logger,
	serviceConfig *Config,
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
	serviceConfig *Config,
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

func ThrottledLoggerRpsFnProvider(serviceConfig *Config) resource.ThrottledLoggerRpsFn {
	return func() float64 { return float64(serviceConfig.ThrottledLogRPS()) }
}

func NamespaceRateLimitInterceptorProvider(
	serviceConfig *Config,
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
		map[string]int{},       // no token overrides
		configs.PollTaskAPISet, // set of APIs that will wait for token instead of immediate rejection
		serviceConfig.PollWaitForNamespaceRateLimitToken,
		metricsHandler,
	)
}

func RateLimitInterceptorProvider(
	serviceConfig *Config,
	metricsHandler metrics.Handler,
) *interceptor.RateLimitInterceptor {
	priorityFn, priorities := getFairnessPriorityFn(serviceConfig, metricsHandler)
	return interceptor.NewRateLimitInterceptor(
		quotas.NewPriorityRateLimiterHelper(
			quotas.NewDefaultIncomingRateBurst(func() float64 { return float64(serviceConfig.RPS()) }),
			serviceConfig.OperatorRPSRatio,
			priorityFn,
			priorities,
		),
		map[string]int{
			healthpb.Health_Check_FullMethodName: 0, // exclude health check requests from rate limiting.
		},
	)
}

// getFairnessPriorityFn builds the namespace-fairness-aware priority function
// for the matching host RPS rate limiter, along with the priority list to pass
// to NewPriorityRateLimiterHelper.
//
// Priority layout (5 levels):
//
//	0  Operator
//	1  Normal API band
//	2  CancelOutstandingWorkerPolls + unknown
//	3  over-share
//	4  Preemptable
//
// All caller types except Preemptable participate in the fairness check:
// in-share traffic keeps its API priority (including Operator at 0), and
// over-share traffic collapses into the single over-share band at priority 3
// and emits a demotion metric. Preemptable is outside the fairness model: it
// never consumes the namespace bucket, never produces a demotion metric, and
// always sinks to the bottom band.
//
// share(ns) = MatchingRPS * NamespaceFairShare(ns). NamespaceFairShare is a
// float in [0, 1]. This single knob also controls whether fairness applies:
// values outside the open interval (0, 1) disable the fairness check for
// that namespace, so the namespace keeps its API priority. 0 = feature off;
// >=1 = allow up to the full host budget (no demotion possible before the
// host limit anyway).
func getFairnessPriorityFn(
	cfg *Config,
	metricsHandler metrics.Handler,
) (quotas.RequestPriorityFn, []int) {
	const (
		overSharePriority   = 3
		preemptablePriority = 4
	)
	priorities := append([]int{}, configs.APIPrioritiesOrdered...)
	priorities = append(priorities, overSharePriority, preemptablePriority)

	// share returns the per-namespace fair-share rate, or 0 when the configured
	// fair share is outside (0, 1) — both endpoints mean "fairness disabled
	// for this namespace."
	share := func(ns string) float64 {
		fs := cfg.NamespaceFairShare(ns)
		if fs <= 0 || fs >= 1 {
			return 0
		}
		return float64(cfg.RPS()) * fs
	}

	// Per-namespace fair-share buckets, sized at share(ns) with the standard
	// incoming burst ratio. Reuses NewNamespaceRequestRateLimiter (per-namespace
	// map keyed on req.Caller, 1-hour TTL eviction) and NewDefaultIncomingRateLimiter
	// (dynamic rate read live each call).
	nsBuckets := quotas.NewNamespaceRequestRateLimiter(
		func(req quotas.Request) quotas.RequestRateLimiter {
			ns := req.Caller
			return quotas.NewRequestRateLimiterAdapter(
				quotas.NewDefaultIncomingRateLimiter(func() float64 { return share(ns) }),
			)
		},
	)

	priorityFn := func(req quotas.Request) int {
		apiPri := configs.RequestToPriority(req)
		if req.Caller == "" {
			return apiPri
		}
		if share(req.Caller) <= 0 {
			return apiPri
		}
		if req.CallerType == headers.CallerTypePreemptable {
			return preemptablePriority
		}
		if nsBuckets.Allow(time.Now().UTC(), req) {
			return apiPri
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

// PersistenceRateLimitingParamsProvider is the same between services but uses different config sources.
// if-case comes from resourceImpl.New.
func PersistenceRateLimitingParamsProvider(
	serviceConfig *Config,
	persistenceLazyLoadedServiceResolver service.PersistenceLazyLoadedServiceResolver,
	logger log.SnTaggedLogger,
) service.PersistenceRateLimitingParams {
	return service.NewPersistenceRateLimitingParams(
		serviceConfig.PersistenceMaxQPS,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.PersistenceNamespaceMaxQPS,
		serviceConfig.PersistenceGlobalNamespaceMaxQPS,
		serviceConfig.PersistencePerShardNamespaceMaxQPS,
		serviceConfig.OperatorRPSRatio,
		serviceConfig.PersistenceQPSBurstRatio,
		serviceConfig.PersistenceDynamicRateLimitingParams,
		persistenceLazyLoadedServiceResolver,
		logger,
	)
}

func ServiceResolverProvider(
	membershipMonitor membership.Monitor,
) (membership.ServiceResolver, error) {
	return membershipMonitor.GetResolver(primitives.MatchingService)
}

// TaskQueueReplicatorNamespaceReplicationQueue is used to ensure the replicator only gets set if global namespaces are
// enabled on this cluster. See NamespaceReplicationQueueProvider below.
type TaskQueueReplicatorNamespaceReplicationQueue persistence.NamespaceReplicationQueue

func NamespaceReplicationQueueProvider(
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	clusterMetadata cluster.Metadata,
) TaskQueueReplicatorNamespaceReplicationQueue {
	var replicatorNamespaceReplicationQueue persistence.NamespaceReplicationQueue
	if clusterMetadata.IsGlobalNamespaceEnabled() {
		replicatorNamespaceReplicationQueue = namespaceReplicationQueue
	}
	return replicatorNamespaceReplicationQueue
}

func VisibilityManagerProvider(
	logger log.Logger,
	persistenceConfig *config.Persistence,
	customVisibilityStoreFactory visibility.VisibilityStoreFactory,
	metricsHandler metrics.Handler,
	serviceConfig *Config,
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
		nil, // matching visibility never writes
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
		dynamicconfig.GetStringPropertyFn(visibility.SecondaryVisibilityWritingModeOff), // matching visibility never writes
		serviceConfig.VisibilityDisableOrderByClause,
		serviceConfig.VisibilityEnableManualPagination,
		serviceConfig.VisibilityEnableUnifiedQueryConverter,
		metricsHandler,
		logger,
		serializer,
	)
}

func ContextMetadataInterceptorProvider(logger log.Logger) *interceptor.ContextMetadataInterceptor {
	return interceptor.NewContextMetadataInterceptor(true, logger)
}

func ServiceLifetimeHooks(lc fx.Lifecycle, svc *Service) {
	lc.Append(fx.StartStopHook(svc.Start, svc.Stop))
}

func WorkersRegistryProvider(
	lc fx.Lifecycle,
	metricsHandler metrics.Handler,
	serviceConfig *Config,
) workers.Registry {
	return workers.NewRegistry(lc, workers.RegistryParams{
		NumBuckets:       serviceConfig.WorkerRegistryNumBuckets,
		TTL:              serviceConfig.WorkerRegistryEntryTTL,
		MinEvictAge:      serviceConfig.WorkerRegistryMinEvictAge,
		MaxItems:         serviceConfig.WorkerRegistryMaxEntries,
		EvictionInterval: serviceConfig.WorkerRegistryEvictionInterval,
		MetricsHandler:   metricsHandler,
		MetricsConfig: workers.WorkerMetricsConfig{
			EnablePluginMetrics:            serviceConfig.EnableWorkerPluginMetrics,
			EnablePollerAutoscalingMetrics: serviceConfig.EnablePollerAutoscalingMetrics,
			BreakdownMetricsByTaskQueue:    serviceConfig.BreakdownMetricsByTaskQueue,
			ExternalPayloadsEnabled:        serviceConfig.ExternalPayloadsEnabled,
		},
	})
}
