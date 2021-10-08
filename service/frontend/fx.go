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

	"go.uber.org/fx"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service"
	"go.temporal.io/server/service/frontend/configs"
)

var Module = fx.Options(
	resource.Module,
	fx.Provide(ParamsExpandProvider), // BootstrapParams should be deprecated
	fx.Provide(dynamicconfig.NewCollection),
	fx.Provide(ConfigProvider),
	fx.Provide(NamespaceLogInterceptorProvider),
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(NamespaceCountLimitInterceptorProvider),
	fx.Provide(NamespaceRateLimitInterceptorProvider),
	fx.Provide(GrpcServerOptionsProvider),
	fx.Provide(VisibilityManagerProvider),
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(PersistenceMaxQpsProvider),
	fx.Provide(NamespaceReplicationQueueProvider),
	fx.Provide(HandlerProvider),
	fx.Provide(func(so []grpc.ServerOption) *grpc.Server { return grpc.NewServer(so...) }),
	fx.Provide(NewAdminHandler),
	fx.Provide(NewVersionChecker),
	fx.Provide(NewService),
	fx.Invoke(ServiceLifetimeHooks),
)

func ParamsExpandProvider(params *resource.BootstrapParams) (
	log.Logger,
	dynamicconfig.Client,
	config.Persistence,
	*esclient.Config,
	common.RPCFactory,
) {
	return params.Logger,
		params.DynamicConfigClient,
		params.PersistenceConfig,
		params.ESConfig,
		params.RPCFactory
}

func GrpcServerOptionsProvider(
	params *resource.BootstrapParams,
	logger log.Logger,
	serviceConfig *Config,
	serviceResource resource.Resource,
	rpcFactory common.RPCFactory,
	namespaceLogInterceptor *interceptor.NamespaceLogInterceptor,
	namespaceRateLimiterInterceptor *interceptor.NamespaceRateLimitInterceptor,
	namespaceCountLimiterInterceptor *interceptor.NamespaceCountLimitInterceptor,
	telemetryInterceptor *interceptor.TelemetryInterceptor,
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
) []grpc.ServerOption {
	kep := keepalive.EnforcementPolicy{
		MinTime:             serviceConfig.KeepAliveMinTime(),
		PermitWithoutStream: serviceConfig.KeepAlivePermitWithoutStream(),
	}
	var kp = keepalive.ServerParameters{
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
	return append(
		grpcServerOptions,
		grpc.KeepaliveParams(kp),
		grpc.KeepaliveEnforcementPolicy(kep),
		grpc.ChainUnaryInterceptor(
			namespaceLogInterceptor.Intercept,
			rpc.ServiceErrorInterceptor,
			metrics.NewServerMetricsContextInjectorInterceptor(),
			telemetryInterceptor.Intercept,
			rateLimitInterceptor.Intercept,
			namespaceRateLimiterInterceptor.Intercept,
			namespaceCountLimiterInterceptor.Intercept,
			authorization.NewAuthorizationInterceptor(
				params.ClaimMapper,
				params.Authorizer,
				serviceResource.GetMetricsClient(),
				params.Logger,
				params.AudienceGetter,
			),
		),
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
	params *resource.BootstrapParams,
	serviceResource resource.Resource,
) *interceptor.NamespaceLogInterceptor {
	namespaceLogger := params.NamespaceLogger
	return interceptor.NewNamespaceLogInterceptor(
		serviceResource.GetNamespaceRegistry(),
		namespaceLogger)
}

func TelemetryInterceptorProvider(
	logger log.Logger,
	resource resource.Resource,
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		resource.GetNamespaceRegistry(),
		resource.GetMetricsClient(),
		metrics.FrontendAPIMetricsScopes(),
		logger,
	)
}

func RateLimitInterceptorProvider(
	serviceConfig *Config,
) *interceptor.RateLimitInterceptor {
	return interceptor.NewRateLimitInterceptor(
		configs.NewRequestToRateLimiter(func() float64 { return float64(serviceConfig.RPS()) }),
		map[string]int{},
	)
}

func NamespaceRateLimitInterceptorProvider(
	serviceConfig *Config,
	serviceResource resource.Resource,
) *interceptor.NamespaceRateLimitInterceptor {
	return interceptor.NewNamespaceRateLimitInterceptor(
		serviceResource.GetNamespaceRegistry(),
		quotas.NewNamespaceRateLimiter(
			func(req quotas.Request) quotas.RequestRateLimiter {
				return configs.NewRequestToRateLimiter(func() float64 {
					return namespaceRPS(
						serviceConfig,
						serviceResource.GetFrontendServiceResolver(),
						req.Caller,
					)
				})
			},
		),
		map[string]int{},
	)
}

func NamespaceCountLimitInterceptorProvider(
	serviceConfig *Config,
	serviceResource resource.Resource,
) *interceptor.NamespaceCountLimitInterceptor {
	return interceptor.NewNamespaceCountLimitInterceptor(
		serviceResource.GetNamespaceRegistry(),
		serviceConfig.MaxNamespaceCountPerInstance,
		configs.ExecutionAPICountLimitOverride,
	)
}

func PersistenceMaxQpsProvider(
	serviceConfig *Config,
) persistenceClient.PersistenceMaxQps {
	return service.PersistenceMaxQpsFn(serviceConfig.PersistenceMaxQPS, serviceConfig.PersistenceGlobalMaxQPS)
}

func VisibilityManagerProvider(
	params *resource.BootstrapParams,
	serviceResource resource.Resource,
	serviceConfig *Config,
) (manager.VisibilityManager, error) {
	return visibility.NewManager(
		params.PersistenceConfig,
		params.PersistenceServiceResolver,
		params.ESConfig.GetVisibilityIndex(),
		params.ESClient,
		nil, // frontend visibility never write
		serviceResource.GetSearchAttributesProvider(),
		params.SearchAttributesMapper,
		serviceConfig.StandardVisibilityPersistenceMaxReadQPS,
		serviceConfig.StandardVisibilityPersistenceMaxWriteQPS,
		serviceConfig.AdvancedVisibilityPersistenceMaxReadQPS,
		serviceConfig.AdvancedVisibilityPersistenceMaxWriteQPS,
		serviceConfig.EnableReadVisibilityFromES,
		dynamicconfig.GetStringPropertyFn(visibility.AdvancedVisibilityWritingModeOff), // frontend visibility never write
		params.MetricsClient,
		params.Logger,
	)
}

func NamespaceReplicationQueueProvider(
	serviceResource resource.Resource,
) persistence.NamespaceReplicationQueue {
	var namespaceReplicationQueue persistence.NamespaceReplicationQueue
	clusterMetadata := serviceResource.GetClusterMetadata()
	if clusterMetadata.IsGlobalNamespaceEnabled() {
		namespaceReplicationQueue = serviceResource.GetNamespaceReplicationQueue()
	}
	return namespaceReplicationQueue
}

func HandlerProvider(
	params *resource.BootstrapParams,
	serviceResource resource.Resource,
	serviceConfig *Config,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	visibilityMgr manager.VisibilityManager,
) Handler {
	wfHandler := NewWorkflowHandler(serviceResource, serviceConfig, namespaceReplicationQueue, visibilityMgr)
	handler := NewDCRedirectionHandler(wfHandler, params.DCRedirectionPolicy)
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
