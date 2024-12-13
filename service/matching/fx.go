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

package matching

import (
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service"
	"go.temporal.io/server/service/matching/configs"
	"go.temporal.io/server/service/worker/deployment"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var Module = fx.Options(
	resource.Module,
	dynamicconfig.Module,
	deployment.Module,
	fx.Provide(ConfigProvider),
	fx.Provide(PersistenceRateLimitingParamsProvider),
	service.PersistenceLazyLoadedServiceResolverModule,
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(RetryableInterceptorProvider),
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(VisibilityManagerProvider),
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

func RetryableInterceptorProvider() *interceptor.RetryableInterceptor {
	return interceptor.NewRetryableInterceptor(
		common.CreateMatchingHandlerRetryPolicy(),
		common.IsServiceHandlerRetryableError,
	)
}

func TelemetryInterceptorProvider(
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	metricsHandler metrics.Handler,
	serviceConfig *Config,
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		namespaceRegistry,
		metricsHandler,
		logger,
		serviceConfig.LogAllReqErrors,
	)
}

func ThrottledLoggerRpsFnProvider(serviceConfig *Config) resource.ThrottledLoggerRpsFn {
	return func() float64 { return float64(serviceConfig.ThrottledLogRPS()) }
}

func RateLimitInterceptorProvider(
	serviceConfig *Config,
) *interceptor.RateLimitInterceptor {
	return interceptor.NewRateLimitInterceptor(
		configs.NewPriorityRateLimiter(func() float64 { return float64(serviceConfig.RPS()) }, serviceConfig.OperatorRPSRatio),
		map[string]int{
			healthpb.Health_Check_FullMethodName: 0, // exclude health check requests from rate limiting.
		},
	)
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
) (manager.VisibilityManager, error) {
	return visibility.NewManager(
		*persistenceConfig,
		persistenceServiceResolver,
		customVisibilityStoreFactory,
		nil, // matching visibility never writes
		saProvider,
		searchAttributesMapperProvider,
		namespaceRegistry,
		serviceConfig.VisibilityPersistenceMaxReadQPS,
		serviceConfig.VisibilityPersistenceMaxWriteQPS,
		serviceConfig.OperatorRPSRatio,
		serviceConfig.VisibilityPersistenceSlowQueryThreshold,
		serviceConfig.EnableReadFromSecondaryVisibility,
		serviceConfig.VisibilityEnableShadowReadMode,
		dynamicconfig.GetStringPropertyFn(visibility.SecondaryVisibilityWritingModeOff), // matching visibility never writes
		serviceConfig.VisibilityDisableOrderByClause,
		serviceConfig.VisibilityEnableManualPagination,
		metricsHandler,
		logger,
	)
}

func ServiceLifetimeHooks(lc fx.Lifecycle, svc *Service) {
	lc.Append(fx.StartStopHook(svc.Start, svc.Stop))
}
