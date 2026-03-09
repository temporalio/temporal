package worker

import (
	"context"
	"os"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service"
	"go.temporal.io/server/service/worker/batcher"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.temporal.io/server/service/worker/deletenamespace"
	"go.temporal.io/server/service/worker/dlq"
	"go.temporal.io/server/service/worker/dummy"
	"go.temporal.io/server/service/worker/migration"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/service/worker/workerdeployment"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

var Module = fx.Options(
	migration.Module,
	resource.Module,
	deletenamespace.Module,
	scheduler.Module,
	batcher.Module,
	workerdeployment.Module,
	dlq.Module,
	dummy.Module,
	fx.Provide(
		func(c resource.HistoryClient) dlq.HistoryClient {
			return c
		},
		func(m cluster.Metadata) dlq.CurrentClusterName {
			return dlq.CurrentClusterName(m.GetCurrentClusterName())
		},
		func(b client.Bean) dlq.TaskClientDialer {
			return dlq.TaskClientDialerFn(func(_ context.Context, address string) (dlq.TaskClient, error) {
				c, err := b.GetRemoteAdminClient(address)
				if err != nil {
					return nil, err
				}
				return dlq.AddTasksFn(func(
					ctx context.Context,
					req *adminservice.AddTasksRequest,
				) (*adminservice.AddTasksResponse, error) {
					return c.AddTasks(ctx, req)
				}), nil
			})
		},
	),
	fx.Provide(HostInfoProvider),
	fx.Provide(VisibilityManagerProvider),
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(ConfigProvider),
	fx.Provide(PersistenceRateLimitingParamsProvider),
	service.PersistenceLazyLoadedServiceResolverModule,
	fx.Provide(ServiceResolverProvider),
	fx.Provide(func(
		clusterMetadata cluster.Metadata,
		metadataManager persistence.MetadataManager,
		logger log.Logger,
	) nsreplication.TaskExecutor {
		return nsreplication.NewTaskExecutor(
			clusterMetadata.GetCurrentClusterName(),
			metadataManager,
			logger,
		)
	}),
	fx.Provide(ServerProvider),
	fx.Provide(NewService),
	fx.Provide(fx.Annotate(NewWorkerManager, fx.ParamTags(workercommon.WorkerComponentTag))),
	fx.Provide(PerNamespaceWorkerManagerProvider),
	fx.Invoke(ServiceLifetimeHooks),
)

func ThrottledLoggerRpsFnProvider(serviceConfig *Config) resource.ThrottledLoggerRpsFn {
	return func() float64 { return float64(serviceConfig.ThrottledLogRPS()) }
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

func HostInfoProvider() (membership.HostInfo, error) {
	hn, err := os.Hostname()
	return membership.NewHostInfoFromAddress(hn), err
}

func ServiceResolverProvider(
	membershipMonitor membership.Monitor,
) (membership.ServiceResolver, error) {
	return membershipMonitor.GetResolver(primitives.WorkerService)
}

func ConfigProvider(
	dc *dynamicconfig.Collection,
	persistenceConfig *config.Persistence,
) *Config {
	return NewConfig(
		dc,
		persistenceConfig,
	)
}

func VisibilityManagerProvider(
	logger log.Logger,
	metricsHandler metrics.Handler,
	persistenceConfig *config.Persistence,
	customVisibilityStoreFactory visibility.VisibilityStoreFactory,
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
		nil, // worker visibility never write
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
		dynamicconfig.GetStringPropertyFn(visibility.SecondaryVisibilityWritingModeOff), // worker visibility never write
		serviceConfig.VisibilityDisableOrderByClause,
		serviceConfig.VisibilityEnableManualPagination,
		serviceConfig.VisibilityEnableUnifiedQueryConverter,
		metricsHandler,
		logger,
		serializer,
	)
}

func ServiceLifetimeHooks(lc fx.Lifecycle, svc *Service) {
	lc.Append(fx.StartStopHook(svc.Start, svc.Stop))
}

type perNamespaceWorkerManagerInitParams struct {
	fx.In
	Logger            log.Logger
	SdkClientFactory  sdk.ClientFactory
	NamespaceRegistry namespace.Registry
	HostName          resource.HostName
	Config            *Config
	ClusterMetadata   cluster.Metadata
	Components        []workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
}

func PerNamespaceWorkerManagerProvider(params perNamespaceWorkerManagerInitParams) *PerNamespaceWorkerManager {
	return NewPerNamespaceWorkerManager(
		params.Logger,
		params.SdkClientFactory,
		params.NamespaceRegistry,
		params.HostName,
		params.Config,
		params.ClusterMetadata,
		params.Components,
		primitives.PerNSWorkerTaskQueue,
	)
}

func ServerProvider(rpcFactory common.RPCFactory, logger log.Logger) *grpc.Server {
	opts, err := rpcFactory.GetInternodeGRPCServerOptions()
	if err != nil {
		logger.Fatal("Failed to get gRPC server options", tag.Error(err))
	}
	return grpc.NewServer(opts...)
}
