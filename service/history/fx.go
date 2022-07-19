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
	"context"
	"net"

	"go.uber.org/fx"
	"google.golang.org/grpc"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	warchiver "go.temporal.io/server/service/worker/archiver"
)

var Module = fx.Options(
	resource.Module,
	workflow.Module,
	shard.Module,
	fx.Provide(dynamicconfig.NewCollection),
	fx.Provide(ConfigProvider), // might be worth just using provider for configs.Config directly
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(service.GrpcServerOptionsProvider),
	fx.Provide(ESProcessorConfigProvider),
	fx.Provide(VisibilityManagerProvider),
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(PersistenceMaxQpsProvider),
	fx.Provide(ServiceResolverProvider),
	fx.Provide(EventNotifierProvider),
	fx.Provide(ArchivalClientProvider),
	fx.Provide(HistoryEngineFactoryProvider),
	fx.Provide(HandlerProvider),
	fx.Provide(ServiceProvider),
	fx.Invoke(ServiceLifetimeHooks),
)

func ServiceProvider(
	grpcServerOptions []grpc.ServerOption,
	serviceConfig *configs.Config,
	visibilityMgr manager.VisibilityManager,
	handler *Handler,
	logger resource.SnTaggedLogger,
	grpcListener net.Listener,
	membershipMonitor membership.Monitor,
	metricsHandler metrics.MetricsHandler,
	faultInjectionDataStoreFactory *persistenceClient.FaultInjectionDataStoreFactory,
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
		faultInjectionDataStoreFactory,
	)
}

func ServiceResolverProvider(membershipMonitor membership.Monitor) (membership.ServiceResolver, error) {
	return membershipMonitor.GetResolver(common.HistoryServiceName)
}

func HandlerProvider(args NewHandlerArgs) *Handler {
	handler := &Handler{
		status:                        common.DaemonStatusInitialized,
		config:                        args.Config,
		tokenSerializer:               common.NewProtoTaskTokenSerializer(),
		logger:                        args.Logger,
		throttledLogger:               args.ThrottledLogger,
		persistenceExecutionManager:   args.PersistenceExecutionManager,
		persistenceShardManager:       args.PersistenceShardManager,
		persistenceVisibilityManager:  args.PersistenceVisibilityManager,
		historyServiceResolver:        args.HistoryServiceResolver,
		metricsClient:                 args.MetricsClient,
		payloadSerializer:             args.PayloadSerializer,
		timeSource:                    args.TimeSource,
		namespaceRegistry:             args.NamespaceRegistry,
		saProvider:                    args.SaProvider,
		saMapper:                      args.SaMapper,
		clusterMetadata:               args.ClusterMetadata,
		archivalMetadata:              args.ArchivalMetadata,
		hostInfoProvider:              args.HostInfoProvider,
		controller:                    args.ShardController,
		eventNotifier:                 args.EventNotifier,
		replicationTaskFetcherFactory: args.ReplicationTaskFetcherFactory,
		tracer:                        args.TracerProvider.Tracer(consts.LibraryName),
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
	return configs.NewConfig(dc,
		persistenceConfig.NumHistoryShards,
		persistenceConfig.AdvancedVisibilityConfigExist(),
		esConfig.GetVisibilityIndex())
}

func ThrottledLoggerRpsFnProvider(serviceConfig *configs.Config) resource.ThrottledLoggerRpsFn {
	return func() float64 { return float64(serviceConfig.ThrottledLogRPS()) }
}

func TelemetryInterceptorProvider(
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	metricsClient metrics.Client,
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		namespaceRegistry,
		metricsClient,
		metrics.HistoryAPIMetricsScopes(),
		logger,
	)
}

func RateLimitInterceptorProvider(
	serviceConfig *configs.Config,
) *interceptor.RateLimitInterceptor {
	return interceptor.NewRateLimitInterceptor(
		configs.NewPriorityRateLimiter(func() float64 { return float64(serviceConfig.RPS()) }),
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

func PersistenceMaxQpsProvider(
	serviceConfig *configs.Config,
) persistenceClient.PersistenceMaxQps {
	return service.PersistenceMaxQpsFn(serviceConfig.PersistenceMaxQPS, serviceConfig.PersistenceGlobalMaxQPS)
}

func VisibilityManagerProvider(
	logger log.Logger,
	metricsClient metrics.Client,
	persistenceConfig *config.Persistence,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	serviceConfig *configs.Config,
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
		esProcessorConfig,
		saProvider,
		searchAttributesMapper,
		serviceConfig.StandardVisibilityPersistenceMaxReadQPS,
		serviceConfig.StandardVisibilityPersistenceMaxWriteQPS,
		serviceConfig.AdvancedVisibilityPersistenceMaxReadQPS,
		serviceConfig.AdvancedVisibilityPersistenceMaxWriteQPS,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false), // history visibility never read
		serviceConfig.AdvancedVisibilityWritingMode,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false), // history visibility never read
		serviceConfig.EnableWriteToSecondaryAdvancedVisibility,
		metricsClient,
		logger,
	)
}

func EventNotifierProvider(
	timeSource clock.TimeSource,
	metricsClient metrics.Client,
	config *configs.Config,
) events.Notifier {
	return events.NewNotifier(
		timeSource,
		metricsClient,
		config.GetShardID,
	)
}

func ArchivalClientProvider(
	archiverProvider provider.ArchiverProvider,
	sdkClientFactory sdk.ClientFactory,
	logger log.Logger,
	metricsClient metrics.Client,
	config *configs.Config,
) warchiver.Client {
	return warchiver.NewClient(
		metricsClient,
		logger,
		sdkClientFactory,
		config.NumArchiveSystemWorkflows,
		config.ArchiveRequestRPS,
		archiverProvider,
	)
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
