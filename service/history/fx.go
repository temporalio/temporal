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

package history

import (
	"net"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"

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
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"

	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	nexusworkflow "go.temporal.io/server/components/nexusoperations/workflow"
)

var Module = fx.Options(
	resource.Module,
	fx.Provide(hsm.NewRegistry),
	workflow.Module,
	shard.Module,
	events.Module,
	cache.Module,
	archival.Module,
	fx.Provide(dynamicconfig.NewCollection),
	fx.Provide(ConfigProvider), // might be worth just using provider for configs.Config directly
	fx.Provide(workflow.NewCommandHandlerRegistry),
	fx.Provide(RetryableInterceptorProvider),
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
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
	fx.Provide(ServiceProvider),
	fx.Invoke(ServiceLifetimeHooks),

	callbacks.Module,
	nexusoperations.Module,
	fx.Invoke(nexusworkflow.RegisterCommandHandlers),
)

func ServiceProvider(
	grpcServerOptions []grpc.ServerOption,
	serviceConfig *configs.Config,
	visibilityMgr manager.VisibilityManager,
	handler *Handler,
	logger log.SnTaggedLogger,
	grpcListener net.Listener,
	membershipMonitor membership.Monitor,
	metricsHandler metrics.Handler,
	healthServer *health.Server,
) *Service {
	return NewService(
		grpcServerOptions,
		serviceConfig,
		visibilityMgr,
		handler,
		logger,
		grpcListener,
		membershipMonitor,
		metricsHandler,
		healthServer,
	)
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
		tokenSerializer:              common.NewProtoTaskTokenSerializer(),
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

		replicationTaskFetcherFactory:    args.ReplicationTaskFetcherFactory,
		replicationTaskConverterProvider: args.ReplicationTaskConverterFactory,
		streamReceiverMonitor:            args.StreamReceiverMonitor,
	}

	// prevent us from trying to serve requests before shard controller is started and ready
	handler.startWG.Add(1)
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
	esConfig *esclient.Config,
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
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		namespaceRegistry,
		metricsHandler,
		logger,
	)
}

func RateLimitInterceptorProvider(
	serviceConfig *configs.Config,
) *interceptor.RateLimitInterceptor {
	return interceptor.NewRateLimitInterceptor(
		configs.NewPriorityRateLimiter(func() float64 { return float64(serviceConfig.RPS()) }, serviceConfig.OperatorRPSRatio),
		map[string]int{},
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
	esClient esclient.Client,
	persistenceServiceResolver resolver.ServiceResolver,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	saProvider searchattribute.Provider,
) (manager.VisibilityManager, error) {
	return visibility.NewManager(
		*persistenceConfig,
		persistenceServiceResolver,
		customVisibilityStoreFactory,
		esClient,
		esProcessorConfig,
		saProvider,
		searchAttributesMapperProvider,
		serviceConfig.VisibilityPersistenceMaxReadQPS,
		serviceConfig.VisibilityPersistenceMaxWriteQPS,
		serviceConfig.OperatorRPSRatio,
		serviceConfig.EnableReadFromSecondaryVisibility,
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
