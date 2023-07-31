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

package frontend

import (
	"fmt"
	"net"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/keepalive"

	"go.temporal.io/server/api/historyservice/v1"
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
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service"
	"go.temporal.io/server/service/frontend/configs"
)

type FEReplicatorNamespaceReplicationQueue persistence.NamespaceReplicationQueue

var Module = fx.Options(
	resource.Module,
	fx.Provide(dynamicconfig.NewCollection),
	fx.Provide(ConfigProvider),
	fx.Provide(NamespaceLogInterceptorProvider),
	fx.Provide(RedirectionInterceptorProvider),
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(RetryableInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(NamespaceCountLimitInterceptorProvider),
	fx.Provide(NamespaceValidatorInterceptorProvider),
	fx.Provide(NamespaceRateLimitInterceptorProvider),
	fx.Provide(SDKVersionInterceptorProvider),
	fx.Provide(CallerInfoInterceptorProvider),
	fx.Provide(GrpcServerOptionsProvider),
	fx.Provide(VisibilityManagerProvider),
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(PersistenceRateLimitingParamsProvider),
	fx.Provide(FEReplicatorNamespaceReplicationQueueProvider),
	fx.Provide(func(so []grpc.ServerOption) *grpc.Server { return grpc.NewServer(so...) }),
	fx.Provide(HandlerProvider),
	fx.Provide(AdminHandlerProvider),
	fx.Provide(OperatorHandlerProvider),
	fx.Provide(NewVersionChecker),
	fx.Provide(ServiceResolverProvider),
	fx.Provide(NewServiceProvider),
	fx.Invoke(ServiceLifetimeHooks),
)

func NewServiceProvider(
	serviceConfig *Config,
	server *grpc.Server,
	healthServer *health.Server,
	handler Handler,
	adminHandler *AdminHandler,
	operatorHandler *OperatorHandlerImpl,
	versionChecker *VersionChecker,
	visibilityMgr manager.VisibilityManager,
	logger log.SnTaggedLogger,
	grpcListener net.Listener,
	metricsHandler metrics.Handler,
	faultInjectionDataStoreFactory *persistenceClient.FaultInjectionDataStoreFactory,
	membershipMonitor membership.Monitor,
) *Service {
	return NewService(
		serviceConfig,
		server,
		healthServer,
		handler,
		adminHandler,
		operatorHandler,
		versionChecker,
		visibilityMgr,
		logger,
		grpcListener,
		metricsHandler,
		faultInjectionDataStoreFactory,
		membershipMonitor,
	)
}

func GrpcServerOptionsProvider(
	logger log.Logger,
	serviceConfig *Config,
	serviceName primitives.ServiceName,
	rpcFactory common.RPCFactory,
	namespaceLogInterceptor *interceptor.NamespaceLogInterceptor,
	namespaceRateLimiterInterceptor *interceptor.NamespaceRateLimitInterceptor,
	namespaceCountLimiterInterceptor *interceptor.ConcurrentRequestLimitInterceptor,
	namespaceValidatorInterceptor *interceptor.NamespaceValidatorInterceptor,
	redirectionInterceptor *RedirectionInterceptor,
	telemetryInterceptor *interceptor.TelemetryInterceptor,
	retryableInterceptor *interceptor.RetryableInterceptor,
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
	traceInterceptor telemetry.ServerTraceInterceptor,
	sdkVersionInterceptor *interceptor.SDKVersionInterceptor,
	callerInfoInterceptor *interceptor.CallerInfoInterceptor,
	authorizer authorization.Authorizer,
	claimMapper authorization.ClaimMapper,
	audienceGetter authorization.JWTAudienceMapper,
	customInterceptors []grpc.UnaryServerInterceptor,
	metricsHandler metrics.Handler,
) []grpc.ServerOption {
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
		// Service Error Interceptor should be the most outer interceptor on error handling
		rpc.ServiceErrorInterceptor,
		namespaceValidatorInterceptor.NamespaceValidateIntercept,
		namespaceLogInterceptor.Intercept, // TODO: Deprecate this with a outer custom interceptor
		grpc.UnaryServerInterceptor(traceInterceptor),
		metrics.NewServerMetricsContextInjectorInterceptor(),
		redirectionInterceptor.Intercept,
		telemetryInterceptor.UnaryIntercept,
		authorization.NewAuthorizationInterceptor(
			claimMapper,
			authorizer,
			metricsHandler,
			logger,
			audienceGetter,
		),
		namespaceValidatorInterceptor.StateValidationIntercept,
		namespaceCountLimiterInterceptor.Intercept,
		namespaceRateLimiterInterceptor.Intercept,
		rateLimitInterceptor.Intercept,
		sdkVersionInterceptor.Intercept,
		callerInfoInterceptor.Intercept,
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

	return append(
		grpcServerOptions,
		grpc.KeepaliveParams(kp),
		grpc.KeepaliveEnforcementPolicy(kep),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptor...),
	)
}

func ConfigProvider(
	dc *dynamicconfig.Collection,
	persistenceConfig config.Persistence,
) *Config {
	return NewConfig(
		dc,
		persistenceConfig.NumHistoryShards,
		persistenceConfig.StandardVisibilityConfigExist(),
		persistenceConfig.AdvancedVisibilityConfigExist(),
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
) *RedirectionInterceptor {
	return NewRedirectionInterceptor(
		configuration,
		namespaceCache,
		policy,
		logger,
		clientBean,
		metricsHandler,
		timeSource,
		clusterMetadata,
	)
}

func TelemetryInterceptorProvider(
	logger log.Logger,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		namespaceRegistry,
		metricsHandler,
		logger,
	)
}

func RateLimitInterceptorProvider(
	serviceConfig *Config,
	frontendServiceResolver membership.ServiceResolver,
) *interceptor.RateLimitInterceptor {
	rateFn := quotas.ClusterAwareQuotaCalculator{
		MemberCounter:    frontendServiceResolver,
		PerInstanceQuota: serviceConfig.RPS,
		GlobalQuota:      serviceConfig.GlobalRPS,
	}.GetQuota
	namespaceReplicationInducingRateFn := func() float64 {
		return float64(serviceConfig.NamespaceReplicationInducingAPIsRPS())
	}

	return interceptor.NewRateLimitInterceptor(
		configs.NewRequestToRateLimiter(
			quotas.NewDefaultIncomingRateLimiter(rateFn),
			quotas.NewDefaultIncomingRateLimiter(rateFn),
			quotas.NewDefaultIncomingRateLimiter(namespaceReplicationInducingRateFn),
			quotas.NewDefaultIncomingRateLimiter(rateFn),
			serviceConfig.OperatorRPSRatio,
		),
		map[string]int{},
	)
}

func NamespaceRateLimitInterceptorProvider(
	serviceName primitives.ServiceName,
	serviceConfig *Config,
	namespaceRegistry namespace.Registry,
	frontendServiceResolver membership.ServiceResolver,
) *interceptor.NamespaceRateLimitInterceptor {
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

	rateFn := quotas.ClusterAwareNamespaceSpecificQuotaCalculator{
		MemberCounter:    frontendServiceResolver,
		PerInstanceQuota: serviceConfig.MaxNamespaceRPSPerInstance,
		GlobalQuota:      globalNamespaceRPS,
	}.GetQuota
	visibilityRateFn := quotas.ClusterAwareNamespaceSpecificQuotaCalculator{
		MemberCounter:    frontendServiceResolver,
		PerInstanceQuota: serviceConfig.MaxNamespaceVisibilityRPSPerInstance,
		GlobalQuota:      globalNamespaceVisibilityRPS,
	}.GetQuota
	namespaceReplicationInducingRateFn := quotas.ClusterAwareNamespaceSpecificQuotaCalculator{
		MemberCounter:    frontendServiceResolver,
		PerInstanceQuota: serviceConfig.MaxNamespaceNamespaceReplicationInducingAPIsRPSPerInstance,
		GlobalQuota:      globalNamespaceNamespaceReplicationInducingAPIsRPS,
	}.GetQuota
	namespaceRateLimiter := quotas.NewNamespaceRequestRateLimiter(
		func(req quotas.Request) quotas.RequestRateLimiter {
			return configs.NewRequestToRateLimiter(
				configs.NewNamespaceRateBurst(req.Caller, rateFn, serviceConfig.MaxNamespaceBurstPerInstance),
				configs.NewNamespaceRateBurst(req.Caller, visibilityRateFn, serviceConfig.MaxNamespaceVisibilityBurstPerInstance),
				configs.NewNamespaceRateBurst(req.Caller, namespaceReplicationInducingRateFn, serviceConfig.MaxNamespaceNamespaceReplicationInducingAPIsBurstPerInstance),
				configs.NewNamespaceRateBurst(req.Caller, rateFn, serviceConfig.MaxNamespaceBurstPerInstance),
				serviceConfig.OperatorRPSRatio,
			)
		},
	)
	return interceptor.NewNamespaceRateLimitInterceptor(namespaceRegistry, namespaceRateLimiter, map[string]int{})
}

func NamespaceCountLimitInterceptorProvider(
	serviceConfig *Config,
	namespaceRegistry namespace.Registry,
	logger log.SnTaggedLogger,
) *interceptor.ConcurrentRequestLimitInterceptor {
	return interceptor.NewConcurrentRequestLimitInterceptor(
		namespaceRegistry,
		logger,
		serviceConfig.MaxNamespaceCountPerInstance,
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

func PersistenceRateLimitingParamsProvider(
	serviceConfig *Config,
) service.PersistenceRateLimitingParams {
	return service.NewPersistenceRateLimitingParams(
		serviceConfig.PersistenceMaxQPS,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.PersistenceNamespaceMaxQPS,
		serviceConfig.PersistencePerShardNamespaceMaxQPS,
		serviceConfig.EnablePersistencePriorityRateLimiting,
		serviceConfig.OperatorRPSRatio,
		serviceConfig.PersistenceDynamicRateLimitingParams,
	)
}

func VisibilityManagerProvider(
	logger log.Logger,
	persistenceConfig *config.Persistence,
	metricsHandler metrics.Handler,
	serviceConfig *Config,
	esClient esclient.Client,
	persistenceServiceResolver resolver.ServiceResolver,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	saProvider searchattribute.Provider,
) (manager.VisibilityManager, error) {
	return visibility.NewManager(
		*persistenceConfig,
		persistenceServiceResolver,
		esClient,
		nil, // frontend visibility never write
		saProvider,
		searchAttributesMapperProvider,
		serviceConfig.VisibilityPersistenceMaxReadQPS,
		serviceConfig.VisibilityPersistenceMaxWriteQPS,
		serviceConfig.OperatorRPSRatio,
		serviceConfig.EnableReadFromSecondaryVisibility,
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
	esClient esclient.Client,
	visibilityMrg manager.VisibilityManager,
	logger log.SnTaggedLogger,
	persistenceExecutionManager persistence.ExecutionManager,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	taskManager persistence.TaskManager,
	clusterMetadataManager persistence.ClusterMetadataManager,
	persistenceMetadataManager persistence.MetadataManager,
	clientFactory client.Factory,
	clientBean client.Bean,
	historyClient historyservice.HistoryServiceClient,
	sdkClientFactory sdk.ClientFactory,
	membershipMonitor membership.Monitor,
	hostInfoProvider membership.HostInfoProvider,
	archiverProvider provider.ArchiverProvider,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	saProvider searchattribute.Provider,
	saManager searchattribute.Manager,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	healthServer *health.Server,
	eventSerializer serialization.Serializer,
	timeSource clock.TimeSource,
) *AdminHandler {
	args := NewAdminHandlerArgs{
		persistenceConfig,
		configuration,
		namespaceReplicationQueue,
		replicatorNamespaceReplicationQueue,
		esClient,
		visibilityMrg,
		logger,
		persistenceExecutionManager,
		taskManager,
		clusterMetadataManager,
		persistenceMetadataManager,
		clientFactory,
		clientBean,
		historyClient,
		sdkClientFactory,
		membershipMonitor,
		hostInfoProvider,
		archiverProvider,
		metricsHandler,
		namespaceRegistry,
		saProvider,
		saManager,
		clusterMetadata,
		archivalMetadata,
		healthServer,
		eventSerializer,
		timeSource,
	}
	return NewAdminHandler(args)
}

func OperatorHandlerProvider(
	configuration *Config,
	esClient esclient.Client,
	logger log.SnTaggedLogger,
	sdkClientFactory sdk.ClientFactory,
	metricsHandler metrics.Handler,
	visibilityMgr manager.VisibilityManager,
	saProvider searchattribute.Provider,
	saManager searchattribute.Manager,
	healthServer *health.Server,
	historyClient historyservice.HistoryServiceClient,
	clusterMetadataManager persistence.ClusterMetadataManager,
	clusterMetadata cluster.Metadata,
	clientFactory client.Factory,
) *OperatorHandlerImpl {
	args := NewOperatorHandlerImplArgs{
		configuration,
		esClient,
		logger,
		sdkClientFactory,
		metricsHandler,
		visibilityMgr,
		saProvider,
		saManager,
		healthServer,
		historyClient,
		clusterMetadataManager,
		clusterMetadata,
		clientFactory,
	}
	return NewOperatorHandlerImpl(args)
}

func HandlerProvider(
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
	historyClient historyservice.HistoryServiceClient,
	matchingClient resource.MatchingClient,
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
) Handler {
	wfHandler := NewWorkflowHandler(
		serviceConfig,
		namespaceReplicationQueue,
		visibilityMgr,
		logger,
		throttledLogger,
		persistenceExecutionManager,
		clusterMetadataManager,
		persistenceMetadataManager,
		historyClient,
		matchingClient,
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
	)
	return wfHandler
}

func ServiceLifetimeHooks(lc fx.Lifecycle, svc *Service) {
	lc.Append(fx.StartStopHook(svc.Start, svc.Stop))
}
