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
	"context"
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
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(NamespaceCountLimitInterceptorProvider),
	fx.Provide(NamespaceValidatorInterceptorProvider),
	fx.Provide(NamespaceRateLimitInterceptorProvider),
	fx.Provide(SDKVersionInterceptorProvider),
	fx.Provide(GrpcServerOptionsProvider),
	fx.Provide(VisibilityManagerProvider),
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(PersistenceMaxQpsProvider),
	fx.Provide(FEReplicatorNamespaceReplicationQueueProvider),
	fx.Provide(func(so []grpc.ServerOption) *grpc.Server { return grpc.NewServer(so...) }),
	fx.Provide(healthServerProvider),
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
	logger resource.SnTaggedLogger,
	grpcListener net.Listener,
	metricsHandler metrics.MetricsHandler,
	faultInjectionDataStoreFactory *persistenceClient.FaultInjectionDataStoreFactory,
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
	)
}

func GrpcServerOptionsProvider(
	logger log.Logger,
	serviceConfig *Config,
	rpcFactory common.RPCFactory,
	namespaceLogInterceptor *interceptor.NamespaceLogInterceptor,
	namespaceRateLimiterInterceptor *interceptor.NamespaceRateLimitInterceptor,
	namespaceCountLimiterInterceptor *interceptor.NamespaceCountLimitInterceptor,
	namespaceValidatorInterceptor *interceptor.NamespaceValidatorInterceptor,
	telemetryInterceptor *interceptor.TelemetryInterceptor,
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
	traceInterceptor telemetry.ServerTraceInterceptor,
	sdkVersionInterceptor *interceptor.SDKVersionInterceptor,
	authorizer authorization.Authorizer,
	claimMapper authorization.ClaimMapper,
	audienceGetter authorization.JWTAudienceMapper,
	customInterceptors []grpc.UnaryServerInterceptor,
	metricsClient metrics.Client,
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
	grpcServerOptions, err := rpcFactory.GetFrontendGRPCServerOptions()
	if err != nil {
		logger.Fatal("creating gRPC server options failed", tag.Error(err))
	}
	interceptors := []grpc.UnaryServerInterceptor{
		namespaceLogInterceptor.Intercept,
		rpc.ServiceErrorInterceptor,
		grpc.UnaryServerInterceptor(traceInterceptor),
		metrics.NewServerMetricsContextInjectorInterceptor(),
		telemetryInterceptor.Intercept,
		namespaceValidatorInterceptor.Intercept,
		namespaceCountLimiterInterceptor.Intercept,
		namespaceRateLimiterInterceptor.Intercept,
		rateLimitInterceptor.Intercept,
		authorization.NewAuthorizationInterceptor(
			claimMapper,
			authorizer,
			metricsClient,
			logger,
			audienceGetter,
		),
		sdkVersionInterceptor.Intercept,
	}
	if len(customInterceptors) > 0 {
		interceptors = append(interceptors, customInterceptors...)
	}

	return append(
		grpcServerOptions,
		grpc.KeepaliveParams(kp),
		grpc.KeepaliveEnforcementPolicy(kep),
		grpc.ChainUnaryInterceptor(interceptors...),
	)
}

func ConfigProvider(
	dc *dynamicconfig.Collection,
	persistenceConfig config.Persistence,
	esConfig *esclient.Config,
) *Config {
	return NewConfig(
		dc,
		persistenceConfig.NumHistoryShards,
		esConfig.GetVisibilityIndex(),
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

func TelemetryInterceptorProvider(
	logger log.Logger,
	metricsClient metrics.Client,
	namespaceRegistry namespace.Registry,
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		namespaceRegistry,
		metricsClient,
		metrics.FrontendAPIMetricsScopes(),
		logger,
	)
}

func RateLimitInterceptorProvider(
	serviceConfig *Config,
) *interceptor.RateLimitInterceptor {
	rateFn := func() float64 { return float64(serviceConfig.RPS()) }
	return interceptor.NewRateLimitInterceptor(
		configs.NewRequestToRateLimiter(
			quotas.NewDefaultIncomingRateLimiter(rateFn),
			quotas.NewDefaultIncomingRateLimiter(rateFn),
			quotas.NewDefaultIncomingRateLimiter(rateFn),
		),
		map[string]int{},
	)
}

func NamespaceRateLimitInterceptorProvider(
	serviceConfig *Config,
	namespaceRegistry namespace.Registry,
	frontendServiceResolver membership.ServiceResolver,
) *interceptor.NamespaceRateLimitInterceptor {
	rateFn := func(namespace string) float64 {
		return namespaceRPS(
			serviceConfig.MaxNamespaceRPSPerInstance,
			serviceConfig.GlobalNamespaceRPS,
			frontendServiceResolver,
			namespace,
		)
	}

	visibilityRateFn := func(namespace string) float64 {
		return namespaceRPS(
			serviceConfig.MaxNamespaceVisibilityRPSPerInstance,
			serviceConfig.GlobalNamespaceRPS,
			frontendServiceResolver,
			namespace,
		)
	}
	namespaceRateLimiter := quotas.NewNamespaceRateLimiter(
		func(req quotas.Request) quotas.RequestRateLimiter {
			return configs.NewRequestToRateLimiter(
				configs.NewNamespaceRateBurst(req.Caller, rateFn, serviceConfig.MaxNamespaceBurstPerInstance),
				configs.NewNamespaceRateBurst(req.Caller, visibilityRateFn, serviceConfig.MaxNamespaceVisibilityBurstPerInstance),
				configs.NewNamespaceRateBurst(req.Caller, rateFn, serviceConfig.MaxNamespaceBurstPerInstance),
			)
		},
	)
	return interceptor.NewNamespaceRateLimitInterceptor(namespaceRegistry, namespaceRateLimiter, map[string]int{})
}

func NamespaceCountLimitInterceptorProvider(
	serviceConfig *Config,
	namespaceRegistry namespace.Registry,
	logger resource.SnTaggedLogger,
) *interceptor.NamespaceCountLimitInterceptor {
	return interceptor.NewNamespaceCountLimitInterceptor(
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
	)
}

func SDKVersionInterceptorProvider() *interceptor.SDKVersionInterceptor {
	return interceptor.NewSDKVersionInterceptor()
}

func PersistenceMaxQpsProvider(
	serviceConfig *Config,
) persistenceClient.PersistenceMaxQps {
	return service.PersistenceMaxQpsFn(serviceConfig.PersistenceMaxQPS, serviceConfig.PersistenceGlobalMaxQPS)
}

func VisibilityManagerProvider(
	logger log.Logger,
	persistenceConfig *config.Persistence,
	metricsClient metrics.Client,
	serviceConfig *Config,
	esConfig *esclient.Config,
	esClient esclient.Client,
	persistenceServiceResolver resolver.ServiceResolver,
	searchAttributesMapper searchattribute.Mapper,
	saProvider searchattribute.Provider,
) (manager.VisibilityManager, error) {
	return visibility.NewManager(
		*persistenceConfig,
		persistenceServiceResolver,
		esConfig.GetVisibilityIndex(),
		esConfig.GetSecondaryVisibilityIndex(),
		esClient,
		nil, // frontend visibility never write
		saProvider,
		searchAttributesMapper,
		serviceConfig.StandardVisibilityPersistenceMaxReadQPS,
		serviceConfig.StandardVisibilityPersistenceMaxWriteQPS,
		serviceConfig.AdvancedVisibilityPersistenceMaxReadQPS,
		serviceConfig.AdvancedVisibilityPersistenceMaxWriteQPS,
		serviceConfig.EnableReadVisibilityFromES,
		dynamicconfig.GetStringPropertyFn(visibility.AdvancedVisibilityWritingModeOff), // frontend visibility never write
		serviceConfig.EnableReadFromSecondaryAdvancedVisibility,
		dynamicconfig.GetBoolPropertyFn(false), // frontend visibility never write
		metricsClient,
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

func ServiceResolverProvider(membershipMonitor membership.Monitor) (membership.ServiceResolver, error) {
	return membershipMonitor.GetResolver(common.FrontendServiceName)
}

func healthServerProvider() *health.Server {
	return health.NewServer()
}

func AdminHandlerProvider(
	persistenceConfig *config.Persistence,
	config *Config,
	replicatorNamespaceReplicationQueue FEReplicatorNamespaceReplicationQueue,
	esConfig *esclient.Config,
	esClient esclient.Client,
	visibilityMrg manager.VisibilityManager,
	logger resource.SnTaggedLogger,
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
	archiverProvider provider.ArchiverProvider,
	metricsClient metrics.Client,
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
		config,
		namespaceReplicationQueue,
		replicatorNamespaceReplicationQueue,
		esConfig,
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
		archiverProvider,
		metricsClient,
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
	config *Config,
	esConfig *esclient.Config,
	esClient esclient.Client,
	logger resource.SnTaggedLogger,
	sdkClientFactory sdk.ClientFactory,
	metricsClient metrics.Client,
	saProvider searchattribute.Provider,
	saManager searchattribute.Manager,
	healthServer *health.Server,
	historyClient historyservice.HistoryServiceClient,
	namespaceRegistry namespace.Registry,
) *OperatorHandlerImpl {
	args := NewOperatorHandlerImplArgs{
		config,
		esConfig,
		esClient,
		logger,
		sdkClientFactory,
		metricsClient,
		saProvider,
		saManager,
		healthServer,
		historyClient,
		namespaceRegistry,
	}
	return NewOperatorHandlerImpl(args)
}

func HandlerProvider(
	dcRedirectionPolicy config.DCRedirectionPolicy,
	serviceConfig *Config,
	versionChecker *VersionChecker,
	namespaceReplicationQueue FEReplicatorNamespaceReplicationQueue,
	visibilityMgr manager.VisibilityManager,
	logger resource.SnTaggedLogger,
	throttledLogger resource.ThrottledLogger,
	persistenceExecutionManager persistence.ExecutionManager,
	clusterMetadataManager persistence.ClusterMetadataManager,
	persistenceMetadataManager persistence.MetadataManager,
	clientBean client.Bean,
	historyClient historyservice.HistoryServiceClient,
	matchingClient resource.MatchingClient,
	archiverProvider provider.ArchiverProvider,
	metricsClient metrics.Client,
	payloadSerializer serialization.Serializer,
	timeSource clock.TimeSource,
	namespaceRegistry namespace.Registry,
	saMapper searchattribute.Mapper,
	saProvider searchattribute.Provider,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	healthServer *health.Server,
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
		saMapper,
		saProvider,
		clusterMetadata,
		archivalMetadata,
		healthServer,
		timeSource,
	)
	handler := NewDCRedirectionHandler(wfHandler, dcRedirectionPolicy, logger, clientBean, metricsClient, timeSource, namespaceRegistry, clusterMetadata)
	return handler
}

func ServiceLifetimeHooks(
	lc fx.Lifecycle,
	svcStoppedCh chan struct{},
	svc *Service,
) {
	lc.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				go func(svc common.Daemon, svcStoppedCh chan<- struct{}) {
					// Start is blocked until Stop() is called.
					svc.Start()
					close(svcStoppedCh)
				}(svc, svcStoppedCh)

				return nil
			},
			OnStop: func(ctx context.Context) error {
				svc.Stop()
				return nil
			},
		},
	)
}
