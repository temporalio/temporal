package frontend

import (
	"fmt"
	"net"

	"github.com/gorilla/mux"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/telemetry"
	nexusfrontend "go.temporal.io/server/components/nexusoperations/frontend"
	"go.temporal.io/server/service"
	"go.temporal.io/server/service/frontend/configs"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/worker/deployment"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

type (
	FEReplicatorNamespaceReplicationQueue persistence.NamespaceReplicationQueue

	namespaceChecker struct {
		r namespace.Registry
	}
)

var Module = fx.Options(
	resource.Module,
	scheduler.Module,
	deployment.Module,
	workerdeployment.Module,
	// Note that with this approach routes may be registered in arbitrary order.
	// This is okay because our routes don't have overlapping matches.
	// The only important detail is that the PathPrefix("/") route registered in the HTTPAPIServerProvider comes last.
	// Coincidentally, this is the case today, likely because it has more dependencies that the other dependencies.
	// This approach isn't perfect but at it allows the router to be pluggable and we have enough functional test
	// coverage to catch misconfiguration.
	// A more robust approach would require using fx groups but we shouldn't overcomplicate until this becomes an issue.
	fx.Provide(MuxRouterProvider),
	fx.Provide(ConfigProvider),
	fx.Provide(NamespaceLogInterceptorProvider),
	fx.Provide(NamespaceHandoverInterceptorProvider),
	fx.Provide(RedirectionInterceptorProvider),
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(RetryableInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(interceptor.NewHealthInterceptor),
	fx.Provide(NamespaceCountLimitInterceptorProvider),
	fx.Provide(NamespaceValidatorInterceptorProvider),
	fx.Provide(NamespaceRateLimitInterceptorProvider),
	fx.Provide(SDKVersionInterceptorProvider),
	fx.Provide(CallerInfoInterceptorProvider),
	fx.Provide(SlowRequestLoggerInterceptorProvider),
	fx.Provide(MaskInternalErrorDetailsInterceptorProvider),
	fx.Provide(GrpcServerOptionsProvider),
	fx.Provide(VisibilityManagerProvider),
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(PersistenceRateLimitingParamsProvider),
	service.PersistenceLazyLoadedServiceResolverModule,
	fx.Provide(FEReplicatorNamespaceReplicationQueueProvider),
	fx.Provide(AuthorizationInterceptorProvider),
	fx.Provide(NamespaceCheckerProvider),
	fx.Provide(func(so GrpcServerOptions) *grpc.Server { return grpc.NewServer(so.Options...) }),
	fx.Provide(HandlerProvider),
	fx.Provide(AdminHandlerProvider),
	fx.Provide(OperatorHandlerProvider),
	fx.Provide(NewVersionChecker),
	fx.Provide(ServiceResolverProvider),
	fx.Invoke(RegisterNexusHTTPHandler),
	fx.Invoke(RegisterOpenAPIHTTPHandler),
	fx.Provide(HTTPAPIServerProvider),
	fx.Provide(NewServiceProvider),
	fx.Provide(NexusEndpointClientProvider),
	fx.Provide(NexusEndpointRegistryProvider),
	fx.Invoke(ServiceLifetimeHooks),
	fx.Invoke(EndpointRegistryLifetimeHooks),
	nexusfrontend.Module,
	activity.FrontendModule,
)

func NewServiceProvider(
	serviceConfig *Config,
	server *grpc.Server,
	healthServer *health.Server,
	httpAPIServer *HTTPAPIServer,
	handler Handler,
	adminHandler *AdminHandler,
	operatorHandler *OperatorHandlerImpl,
	versionChecker *VersionChecker,
	visibilityMgr manager.VisibilityManager,
	logger log.SnTaggedLogger,
	grpcListener net.Listener,
	metricsHandler metrics.Handler,
	membershipMonitor membership.Monitor,
) *Service {
	return NewService(
		serviceConfig,
		server,
		healthServer,
		httpAPIServer,
		handler,
		adminHandler,
		operatorHandler,
		versionChecker,
		visibilityMgr,
		logger,
		grpcListener,
		metricsHandler,
		membershipMonitor,
	)
}

// GrpcServerOptions are the options to build the frontend gRPC server along
// with the interceptors that are already set in the options.
type GrpcServerOptions struct {
	Options           []grpc.ServerOption
	UnaryInterceptors []grpc.UnaryServerInterceptor
}

func AuthorizationInterceptorProvider(
	cfg *config.Config,
	serviceConfig *Config,
	logger log.Logger,
	namespaceChecker authorization.NamespaceChecker,
	metricsHandler metrics.Handler,
	authorizer authorization.Authorizer,
	claimMapper authorization.ClaimMapper,
	audienceGetter authorization.JWTAudienceMapper,
) *authorization.Interceptor {
	return authorization.NewInterceptor(
		claimMapper,
		authorizer,
		metricsHandler,
		logger,
		namespaceChecker,
		audienceGetter,
		cfg.Global.Authorization.AuthHeaderName,
		cfg.Global.Authorization.AuthExtraHeaderName,
		serviceConfig.ExposeAuthorizerErrors,
	)
}

func NamespaceCheckerProvider(registry namespace.Registry) authorization.NamespaceChecker {
	return &namespaceChecker{r: registry}
}

func (n *namespaceChecker) Exists(name namespace.Name) error {
	// This will get called before the namespace state validation interceptor. We want to
	// disable readthrough to avoid polluting the negative lookup cache, e.g. if this call is
	// for RegisterNamespace and the namespace doesn't exist yet.
	opts := namespace.GetNamespaceOptions{DisableReadthrough: true}
	_, err := n.r.GetNamespaceWithOptions(name, opts)
	return err
}

func GrpcServerOptionsProvider(
	logger log.Logger,
	cfg *config.Config,
	serviceConfig *Config,
	serviceName primitives.ServiceName,
	rpcFactory common.RPCFactory,
	namespaceLogInterceptor *interceptor.NamespaceLogInterceptor,
	namespaceRateLimiterInterceptor interceptor.NamespaceRateLimitInterceptor,
	namespaceCountLimiterInterceptor *interceptor.ConcurrentRequestLimitInterceptor,
	namespaceValidatorInterceptor *interceptor.NamespaceValidatorInterceptor,
	namespaceHandoverInterceptor *interceptor.NamespaceHandoverInterceptor,
	redirectionInterceptor *interceptor.Redirection,
	telemetryInterceptor *interceptor.TelemetryInterceptor,
	retryableInterceptor *interceptor.RetryableInterceptor,
	healthInterceptor *interceptor.HealthInterceptor,
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
	traceStatsHandler telemetry.ServerStatsHandler,
	metricsStatsHandler metrics.ServerStatsHandler,
	sdkVersionInterceptor *interceptor.SDKVersionInterceptor,
	callerInfoInterceptor *interceptor.CallerInfoInterceptor,
	authInterceptor *authorization.Interceptor,
	maskInternalErrorDetailsInterceptor *interceptor.MaskInternalErrorDetailsInterceptor,
	slowRequestLoggerInterceptor *interceptor.SlowRequestLoggerInterceptor,
	customInterceptors []grpc.UnaryServerInterceptor,
	metricsHandler metrics.Handler,
) GrpcServerOptions {
	kep := keepalive.EnforcementPolicy{
		MinTime:             serviceConfig.KeepAliveMinTime(),
		PermitWithoutStream: serviceConfig.KeepAlivePermitWithoutStream(),
	}
	kp := keepalive.ServerParameters{
		MaxConnectionIdle:     serviceConfig.KeepAliveMaxConnectionIdle(),
		MaxConnectionAge:      serviceConfig.KeepAliveMaxConnectionAge(),
		MaxConnectionAgeGrace: serviceConfig.KeepAliveMaxConnectionAgeGrace(),
		Time:                  serviceConfig.KeepAliveTime(),
		Timeout:               serviceConfig.KeepAliveTimeout(),
	}
	var grpcServerOptions []grpc.ServerOption
	var err error
	switch serviceName {
	case primitives.FrontendService:
		grpcServerOptions, err = rpcFactory.GetFrontendGRPCServerOptions()
	case primitives.InternalFrontendService:
		grpcServerOptions, err = rpcFactory.GetInternodeGRPCServerOptions()
	default:
		err = fmt.Errorf("unexpected frontend service name %q", serviceName)
	}
	if err != nil {
		logger.Fatal("creating gRPC server options failed", tag.Error(err))
	}
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		// Order or interceptors is important
		// Mask error interceptor should be the most outer interceptor since it handle the errors format
		// Service Error Interceptor should be the next most outer interceptor on error handling
		maskInternalErrorDetailsInterceptor.Intercept,
		interceptor.ServiceErrorInterceptor,
		interceptor.NewFrontendServiceErrorInterceptor(logger),
		namespaceValidatorInterceptor.NamespaceValidateIntercept,
		namespaceLogInterceptor.Intercept, // TODO: Deprecate this with a outer custom interceptor
		metrics.NewServerMetricsContextInjectorInterceptor(),
		authInterceptor.Intercept,
		// Handover interceptor has to above redirection because the request will route to the correct cluster after handover completed.
		// And retry cannot be performed before customInterceptors.
		namespaceHandoverInterceptor.Intercept,
		redirectionInterceptor.Intercept,
		telemetryInterceptor.UnaryIntercept,
		healthInterceptor.Intercept,
		namespaceValidatorInterceptor.StateValidationIntercept,
		namespaceCountLimiterInterceptor.Intercept,
		namespaceRateLimiterInterceptor.Intercept,
		rateLimitInterceptor.Intercept,
		sdkVersionInterceptor.Intercept,
		callerInfoInterceptor.Intercept,
		slowRequestLoggerInterceptor.Intercept,
	}
	if len(customInterceptors) > 0 {
		// TODO: Deprecate WithChainedFrontendGrpcInterceptors and provide a inner custom interceptor
		unaryInterceptors = append(unaryInterceptors, customInterceptors...)
	}
	// retry interceptor should be the most inner interceptor
	unaryInterceptors = append(unaryInterceptors, retryableInterceptor.Intercept)

	streamInterceptor := []grpc.StreamServerInterceptor{
		telemetryInterceptor.StreamIntercept,
	}

	grpcServerOptions = append(
		grpcServerOptions,
		grpc.KeepaliveParams(kp),
		grpc.KeepaliveEnforcementPolicy(kep),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptor...),
	)

	multiStats := rpc.MultiStatsHandler{}
	if traceStatsHandler != nil {
		multiStats = append(multiStats, traceStatsHandler)
	}
	if metricsStatsHandler != nil {
		multiStats = append(multiStats, metricsStatsHandler)
	}
	if len(multiStats) > 0 {
		grpcServerOptions = append(grpcServerOptions, grpc.StatsHandler(multiStats))
	}
	return GrpcServerOptions{Options: grpcServerOptions, UnaryInterceptors: unaryInterceptors}
}

func ConfigProvider(
	dc *dynamicconfig.Collection,
	persistenceConfig config.Persistence,
) *Config {
	return NewConfig(
		dc,
		persistenceConfig.NumHistoryShards,
	)
}

func ThrottledLoggerRpsFnProvider(serviceConfig *Config) resource.ThrottledLoggerRpsFn {
	return func() float64 { return float64(serviceConfig.ThrottledLogRPS()) }
}

func NamespaceLogInterceptorProvider(
	namespaceLogger resource.NamespaceLogger,
	namespaceRegistry namespace.Registry,
) *interceptor.NamespaceLogInterceptor {
	return interceptor.NewNamespaceLogInterceptor(
		namespaceRegistry,
		namespaceLogger)
}

func RetryableInterceptorProvider() *interceptor.RetryableInterceptor {
	return interceptor.NewRetryableInterceptor(
		common.CreateFrontendHandlerRetryPolicy(),
		common.IsServiceHandlerRetryableError,
	)
}

func RedirectionInterceptorProvider(
	configuration *Config,
	namespaceCache namespace.Registry,
	policy config.DCRedirectionPolicy,
	logger log.Logger,
	clientBean client.Bean,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
	clusterMetadata cluster.Metadata,
) *interceptor.Redirection {
	return interceptor.NewRedirection(
		configuration.EnableNamespaceNotActiveAutoForwarding,
		namespaceCache,
		policy,
		logger,
		clientBean,
		metricsHandler,
		timeSource,
		clusterMetadata,
	)
}

func NamespaceHandoverInterceptorProvider(
	dc *dynamicconfig.Collection,
	namespaceCache namespace.Registry,
	logger log.Logger,
	metricsHandler metrics.Handler,
	timeSource clock.TimeSource,
) *interceptor.NamespaceHandoverInterceptor {
	return interceptor.NewNamespaceHandoverInterceptor(
		dc,
		namespaceCache,
		metricsHandler,
		logger,
		timeSource,
	)
}

func TelemetryInterceptorProvider(
	logger log.Logger,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	serviceConfig *Config,
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		namespaceRegistry,
		metricsHandler,
		logger,
		serviceConfig.LogAllReqErrors,
	)
}

func getRateFnWithMetrics(rateFn quotas.RateFn, handler metrics.Handler) quotas.RateFn {
	return func() float64 {
		rate := rateFn()
		metrics.HostRPSLimit.With(handler).Record(rate)
		return rate
	}
}

func RateLimitInterceptorProvider(
	serviceConfig *Config,
	frontendServiceResolver membership.ServiceResolver,
	handler metrics.Handler,
	logger log.SnTaggedLogger,
) *interceptor.RateLimitInterceptor {
	rateFn := calculator.NewLoggedCalculator(
		calculator.ClusterAwareQuotaCalculator{
			MemberCounter:    frontendServiceResolver,
			PerInstanceQuota: serviceConfig.RPS,
			GlobalQuota:      serviceConfig.GlobalRPS,
		},
		log.With(logger, tag.ComponentRPCHandler, tag.ScopeHost),
	).GetQuota
	rateFnWithMetrics := getRateFnWithMetrics(rateFn, handler)

	namespaceReplicationInducingRateFn := func() float64 {
		return float64(serviceConfig.NamespaceReplicationInducingAPIsRPS())
	}

	return interceptor.NewRateLimitInterceptor(
		configs.NewRequestToRateLimiter(
			quotas.NewDefaultIncomingRateBurst(rateFnWithMetrics),
			quotas.NewDefaultIncomingRateBurst(rateFn),
			quotas.NewDefaultIncomingRateBurst(namespaceReplicationInducingRateFn),
			serviceConfig.OperatorRPSRatio,
		),
		map[string]int{
			healthpb.Health_Check_FullMethodName:                     0, // exclude health check requests from rate limiting.
			adminservice.AdminService_DeepHealthCheck_FullMethodName: 0, // exclude deep health check requests from rate limiting.
		},
	)
}

func MaskInternalErrorDetailsInterceptorProvider(
	logger log.Logger,
	serviceConfig *Config,
	namespaceRegistry namespace.Registry,
) *interceptor.MaskInternalErrorDetailsInterceptor {
	return interceptor.NewMaskInternalErrorDetailsInterceptor(
		serviceConfig.MaskInternalErrorDetails, namespaceRegistry, logger,
	)
}

func NamespaceRateLimitInterceptorProvider(
	serviceName primitives.ServiceName,
	serviceConfig *Config,
	namespaceRegistry namespace.Registry,
	frontendServiceResolver membership.ServiceResolver,
	logger log.SnTaggedLogger,
) interceptor.NamespaceRateLimitInterceptor {
	var globalNamespaceRPS, globalNamespaceVisibilityRPS, globalNamespaceNamespaceReplicationInducingAPIsRPS dynamicconfig.IntPropertyFnWithNamespaceFilter

	switch serviceName {
	case primitives.FrontendService:
		globalNamespaceRPS = serviceConfig.GlobalNamespaceRPS
		globalNamespaceVisibilityRPS = serviceConfig.GlobalNamespaceVisibilityRPS
		globalNamespaceNamespaceReplicationInducingAPIsRPS = serviceConfig.GlobalNamespaceNamespaceReplicationInducingAPIsRPS
	case primitives.InternalFrontendService:
		globalNamespaceRPS = serviceConfig.InternalFEGlobalNamespaceRPS
		globalNamespaceVisibilityRPS = serviceConfig.InternalFEGlobalNamespaceVisibilityRPS
		// Internal frontend has no special limit for this set of APIs
		globalNamespaceNamespaceReplicationInducingAPIsRPS = serviceConfig.InternalFEGlobalNamespaceRPS
	default:
		panic("invalid service name")
	}

	namespaceRateFn := calculator.NewLoggedNamespaceCalculator(
		calculator.ClusterAwareNamespaceQuotaCalculator{
			MemberCounter:    frontendServiceResolver,
			PerInstanceQuota: serviceConfig.MaxNamespaceRPSPerInstance,
			GlobalQuota:      globalNamespaceRPS,
		},
		log.With(logger, tag.ComponentRPCHandler, tag.ScopeNamespace),
	).GetQuota
	visibilityRateFn := calculator.NewLoggedNamespaceCalculator(
		calculator.ClusterAwareNamespaceQuotaCalculator{
			MemberCounter:    frontendServiceResolver,
			PerInstanceQuota: serviceConfig.MaxNamespaceVisibilityRPSPerInstance,
			GlobalQuota:      globalNamespaceVisibilityRPS,
		},
		log.With(logger, tag.ComponentVisibilityHandler, tag.ScopeNamespace),
	).GetQuota
	namespaceReplicationInducingRateFn := calculator.NewLoggedNamespaceCalculator(
		calculator.ClusterAwareNamespaceQuotaCalculator{
			MemberCounter:    frontendServiceResolver,
			PerInstanceQuota: serviceConfig.MaxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance,
			GlobalQuota:      globalNamespaceNamespaceReplicationInducingAPIsRPS,
		},
		log.With(logger, tag.ComponentNamespaceReplication, tag.ScopeNamespace),
	).GetQuota
	namespaceRateLimiter := quotas.NewNamespaceRequestRateLimiter(
		func(req quotas.Request) quotas.RequestRateLimiter {
			return configs.NewRequestToRateLimiter(
				configs.NewNamespaceRateBurst(req.Caller, namespaceRateFn, serviceConfig.MaxNamespaceBurstRatioPerInstance),
				configs.NewNamespaceRateBurst(req.Caller, visibilityRateFn, serviceConfig.MaxNamespaceVisibilityBurstRatioPerInstance),
				configs.NewNamespaceRateBurst(req.Caller, namespaceReplicationInducingRateFn, serviceConfig.MaxNamespaceNamespaceReplicationInducingAPIsBurstRatioPerInstance),
				serviceConfig.OperatorRPSRatio,
			)
		},
	)
	return interceptor.NewNamespaceRateLimitInterceptor(namespaceRegistry, namespaceRateLimiter, map[string]int{})
}

func NamespaceCountLimitInterceptorProvider(
	serviceConfig *Config,
	namespaceRegistry namespace.Registry,
	serviceResolver membership.ServiceResolver,
	logger log.SnTaggedLogger,
) *interceptor.ConcurrentRequestLimitInterceptor {
	return interceptor.NewConcurrentRequestLimitInterceptor(
		namespaceRegistry,
		serviceResolver,
		logger,
		serviceConfig.MaxConcurrentLongRunningRequestsPerInstance,
		serviceConfig.MaxGlobalConcurrentLongRunningRequests,
		configs.ExecutionAPICountLimitOverride,
	)
}

func NamespaceValidatorInterceptorProvider(
	serviceConfig *Config,
	namespaceRegistry namespace.Registry,
) *interceptor.NamespaceValidatorInterceptor {
	return interceptor.NewNamespaceValidatorInterceptor(
		namespaceRegistry,
		serviceConfig.EnableTokenNamespaceEnforcement,
		serviceConfig.MaxIDLengthLimit,
	)
}

func SDKVersionInterceptorProvider() *interceptor.SDKVersionInterceptor {
	return interceptor.NewSDKVersionInterceptor()
}

func CallerInfoInterceptorProvider(
	namespaceRegistry namespace.Registry,
) *interceptor.CallerInfoInterceptor {
	return interceptor.NewCallerInfoInterceptor(namespaceRegistry)
}

func SlowRequestLoggerInterceptorProvider(
	logger log.Logger,
	dc *dynamicconfig.Collection,
) *interceptor.SlowRequestLoggerInterceptor {
	return interceptor.NewSlowRequestLoggerInterceptor(
		logger,
		dynamicconfig.SlowRequestLoggingThreshold.Get(dc),
	)
}

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
) (manager.VisibilityManager, error) {
	return visibility.NewManager(
		*persistenceConfig,
		persistenceServiceResolver,
		customVisibilityStoreFactory,
		nil, // frontend visibility never write
		saProvider,
		searchAttributesMapperProvider,
		namespaceRegistry,
		serviceConfig.VisibilityPersistenceMaxReadQPS,
		serviceConfig.VisibilityPersistenceMaxWriteQPS,
		serviceConfig.OperatorRPSRatio,
		serviceConfig.VisibilityPersistenceSlowQueryThreshold,
		serviceConfig.EnableReadFromSecondaryVisibility,
		serviceConfig.VisibilityEnableShadowReadMode,
		dynamicconfig.GetStringPropertyFn(visibility.SecondaryVisibilityWritingModeOff), // frontend visibility never write
		serviceConfig.VisibilityDisableOrderByClause,
		serviceConfig.VisibilityEnableManualPagination,
		metricsHandler,
		logger,
	)
}

func FEReplicatorNamespaceReplicationQueueProvider(
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	clusterMetadata cluster.Metadata,
) FEReplicatorNamespaceReplicationQueue {
	var replicatorNamespaceReplicationQueue persistence.NamespaceReplicationQueue
	if clusterMetadata.IsGlobalNamespaceEnabled() {
		replicatorNamespaceReplicationQueue = namespaceReplicationQueue
	}
	return replicatorNamespaceReplicationQueue
}

func ServiceResolverProvider(
	membershipMonitor membership.Monitor,
	serviceName primitives.ServiceName,
) (membership.ServiceResolver, error) {
	return membershipMonitor.GetResolver(serviceName)
}

func AdminHandlerProvider(
	persistenceConfig *config.Persistence,
	configuration *Config,
	replicatorNamespaceReplicationQueue FEReplicatorNamespaceReplicationQueue,
	visibilityMgr manager.VisibilityManager,
	logger log.SnTaggedLogger,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	taskManager persistence.TaskManager,
	fairTaskManager persistence.FairTaskManager,
	persistenceExecutionManager persistence.ExecutionManager,
	clusterMetadataManager persistence.ClusterMetadataManager,
	persistenceMetadataManager persistence.MetadataManager,
	clientFactory client.Factory,
	clientBean client.Bean,
	historyClient resource.HistoryClient,
	sdkClientFactory sdk.ClientFactory,
	membershipMonitor membership.Monitor,
	hostInfoProvider membership.HostInfoProvider,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	saProvider searchattribute.Provider,
	saManager searchattribute.Manager,
	saMapperProvider searchattribute.MapperProvider,
	clusterMetadata cluster.Metadata,
	healthServer *health.Server,
	eventSerializer serialization.Serializer,
	timeSource clock.TimeSource,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
	matchingClient resource.MatchingClient,
) *AdminHandler {
	args := NewAdminHandlerArgs{
		persistenceConfig,
		configuration,
		namespaceReplicationQueue,
		replicatorNamespaceReplicationQueue,
		visibilityMgr,
		logger,
		taskManager,
		fairTaskManager,
		persistenceExecutionManager,
		clusterMetadataManager,
		persistenceMetadataManager,
		clientFactory,
		clientBean,
		historyClient,
		sdkClientFactory,
		membershipMonitor,
		hostInfoProvider,
		metricsHandler,
		namespaceRegistry,
		saProvider,
		saManager,
		saMapperProvider,
		clusterMetadata,
		healthServer,
		eventSerializer,
		timeSource,
		taskCategoryRegistry,
		matchingClient,
	}
	return NewAdminHandler(args)
}

func OperatorHandlerProvider(
	configuration *Config,
	logger log.SnTaggedLogger,
	sdkClientFactory sdk.ClientFactory,
	metricsHandler metrics.Handler,
	visibilityMgr manager.VisibilityManager,
	saManager searchattribute.Manager,
	healthServer *health.Server,
	historyClient resource.HistoryClient,
	clusterMetadataManager persistence.ClusterMetadataManager,
	clusterMetadata cluster.Metadata,
	clientFactory client.Factory,
	namespaceRegistry namespace.Registry,
	nexusEndpointClient *NexusEndpointClient,
) *OperatorHandlerImpl {
	args := NewOperatorHandlerImplArgs{
		configuration,
		logger,
		sdkClientFactory,
		metricsHandler,
		visibilityMgr,
		saManager,
		healthServer,
		historyClient,
		clusterMetadataManager,
		clusterMetadata,
		clientFactory,
		namespaceRegistry,
		nexusEndpointClient,
	}
	return NewOperatorHandlerImpl(args)
}

func HandlerProvider(
	cfg *config.Config,
	serviceName primitives.ServiceName,
	dcRedirectionPolicy config.DCRedirectionPolicy,
	serviceConfig *Config,
	versionChecker *VersionChecker,
	namespaceReplicationQueue FEReplicatorNamespaceReplicationQueue,
	visibilityMgr manager.VisibilityManager,
	logger log.SnTaggedLogger,
	throttledLogger log.ThrottledLogger,
	persistenceExecutionManager persistence.ExecutionManager,
	clusterMetadataManager persistence.ClusterMetadataManager,
	persistenceMetadataManager persistence.MetadataManager,
	clientBean client.Bean,
	historyClient resource.HistoryClient,
	matchingClient resource.MatchingClient,
	deploymentStoreClient deployment.DeploymentStoreClient,
	workerDeploymentStoreClient workerdeployment.Client,
	archiverProvider provider.ArchiverProvider,
	metricsHandler metrics.Handler,
	payloadSerializer serialization.Serializer,
	timeSource clock.TimeSource,
	namespaceRegistry namespace.Registry,
	saMapperProvider searchattribute.MapperProvider,
	saProvider searchattribute.Provider,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	healthServer *health.Server,
	membershipMonitor membership.Monitor,
	healthInterceptor *interceptor.HealthInterceptor,
	scheduleSpecBuilder *scheduler.SpecBuilder,
	activityHandler activity.FrontendHandler,
) Handler {
	wfHandler := NewWorkflowHandler(
		serviceConfig,
		namespaceReplicationQueue,
		visibilityMgr,
		logger,
		throttledLogger,
		persistenceExecutionManager.GetName(),
		clusterMetadataManager,
		persistenceMetadataManager,
		historyClient,
		matchingClient,
		deploymentStoreClient,
		workerDeploymentStoreClient,
		archiverProvider,
		payloadSerializer,
		namespaceRegistry,
		saMapperProvider,
		saProvider,
		clusterMetadata,
		archivalMetadata,
		healthServer,
		timeSource,
		membershipMonitor,
		healthInterceptor,
		scheduleSpecBuilder,
		httpEnabled(cfg, serviceName),
		activityHandler,
	)
	return wfHandler
}

func RegisterNexusHTTPHandler(
	serviceConfig *Config,
	serviceName primitives.ServiceName,
	matchingClient resource.MatchingClient,
	metricsHandler metrics.Handler,
	clusterMetadata cluster.Metadata,
	clientCache *cluster.FrontendHTTPClientCache,
	namespaceRegistry namespace.Registry,
	endpointRegistry nexus.EndpointRegistry,
	authInterceptor *authorization.Interceptor,
	telemetryInterceptor *interceptor.TelemetryInterceptor,
	redirectionInterceptor *interceptor.Redirection,
	namespaceRateLimiterInterceptor interceptor.NamespaceRateLimitInterceptor,
	namespaceCountLimiterInterceptor *interceptor.ConcurrentRequestLimitInterceptor,
	namespaceValidatorInterceptor *interceptor.NamespaceValidatorInterceptor,
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
	logger log.Logger,
	router *mux.Router,
	httpTraceProvider nexus.HTTPClientTraceProvider,
) {
	h := NewNexusHTTPHandler(
		serviceConfig,
		matchingClient,
		metricsHandler,
		clusterMetadata,
		clientCache,
		namespaceRegistry,
		endpointRegistry,
		authInterceptor,
		telemetryInterceptor,
		redirectionInterceptor,
		namespaceValidatorInterceptor,
		namespaceRateLimiterInterceptor,
		namespaceCountLimiterInterceptor,
		rateLimitInterceptor,
		logger,
		httpTraceProvider,
	)
	h.RegisterRoutes(router)
}

func RegisterOpenAPIHTTPHandler(
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
	logger log.Logger,
	router *mux.Router,
) *OpenAPIHTTPHandler {
	h := NewOpenAPIHTTPHandler(
		rateLimitInterceptor,
		logger,
	)
	h.RegisterRoutes(router)
	return h
}

func MuxRouterProvider() *mux.Router {
	// Instantiate a router to support additional route prefixes.
	return mux.NewRouter().UseEncodedPath()
}

func httpEnabled(cfg *config.Config, serviceName primitives.ServiceName) bool {
	// If the service is not the frontend service, HTTP API is disabled
	if serviceName != primitives.FrontendService && serviceName != primitives.InternalFrontendService {
		return false
	}
	// If HTTP API port is 0, it is disabled
	return cfg.Services[string(serviceName)].RPC.HTTPPort != 0
}

// HTTPAPIServerProvider provides an HTTP API server if enabled or nil
// otherwise.
func HTTPAPIServerProvider(
	cfg *config.Config,
	serviceName primitives.ServiceName,
	serviceConfig *Config,
	grpcListener net.Listener,
	tlsConfigProvider encryption.TLSConfigProvider,
	handler Handler,
	operatorHandler *OperatorHandlerImpl,
	grpcServerOptions GrpcServerOptions,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	router *mux.Router,
) (*HTTPAPIServer, error) {
	if !httpEnabled(cfg, serviceName) {
		return nil, nil
	}
	rpcConfig := cfg.Services[string(serviceName)].RPC
	return NewHTTPAPIServer(
		serviceConfig,
		rpcConfig,
		grpcListener,
		tlsConfigProvider,
		handler,
		operatorHandler,
		grpcServerOptions.UnaryInterceptors,
		metricsHandler,
		router,
		namespaceRegistry,
		logger,
	)
}

func NexusEndpointClientProvider(
	dc *dynamicconfig.Collection,
	namespaceRegistry namespace.Registry,
	matchingClient resource.MatchingClient,
	nexusEndpointManager persistence.NexusEndpointManager,
	logger log.Logger,
) *NexusEndpointClient {
	clientConfig := newNexusEndpointClientConfig(dc)
	return newNexusEndpointClient(
		clientConfig,
		namespaceRegistry,
		matchingClient,
		nexusEndpointManager,
		logger,
	)
}

func NexusEndpointRegistryProvider(
	matchingClient resource.MatchingClient,
	nexusEndpointManager persistence.NexusEndpointManager,
	dc *dynamicconfig.Collection,
	logger log.Logger,
	metricsHandler metrics.Handler,
) nexus.EndpointRegistry {
	registryConfig := nexus.NewEndpointRegistryConfig(dc)
	return nexus.NewEndpointRegistry(
		registryConfig,
		matchingClient,
		nexusEndpointManager,
		logger,
		metricsHandler,
	)
}

func EndpointRegistryLifetimeHooks(lc fx.Lifecycle, registry nexus.EndpointRegistry) {
	lc.Append(fx.StartStopHook(registry.StartLifecycle, registry.StopLifecycle))
}

func ServiceLifetimeHooks(lc fx.Lifecycle, svc *Service) {
	lc.Append(fx.StartStopHook(svc.Start, svc.Stop))
}
