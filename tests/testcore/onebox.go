package testcore

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand"
	"net"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	chasmtests "go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/grpcinject"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/service/history"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/service/worker"
	"go.temporal.io/server/temporal"
	"go.uber.org/fx"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
)

type (
	TemporalImpl struct {
		fxApps []*fx.App

		// This is used to wait for namespace registries to have noticed a change in some xdc tests.
		namespaceRegistries []namespace.Registry
		// Address for SDK to connect to, using membership grpc resolver.
		frontendMembershipAddress string
		chasmEngine               chasm.Engine
		chasmVisibilityMgr        chasm.VisibilityManager

		// These are routing/load balancing clients but do not do retries:
		adminClient    adminservice.AdminServiceClient
		frontendClient workflowservice.WorkflowServiceClient
		operatorClient operatorservice.OperatorServiceClient
		historyClient  historyservice.HistoryServiceClient
		matchingClient matchingservice.MatchingServiceClient

		dcClient                         *dynamicconfig.MemoryClient
		testHooks                        testhooks.TestHooks
		logger                           log.Logger
		clusterMetadataConfig            *cluster.Config
		persistenceConfig                config.Persistence
		metadataMgr                      persistence.MetadataManager
		clusterMetadataMgr               persistence.ClusterMetadataManager
		shardMgr                         persistence.ShardManager
		taskMgr                          persistence.TaskManager
		executionManager                 persistence.ExecutionManager
		namespaceReplicationQueue        persistence.NamespaceReplicationQueue
		abstractDataStoreFactory         persistenceClient.AbstractDataStoreFactory
		visibilityStoreFactory           visibility.VisibilityStoreFactory
		archiverMetadata                 carchiver.ArchivalMetadata
		archiverProvider                 provider.ArchiverProvider
		frontendConfig                   FrontendConfig
		historyConfig                    HistoryConfig
		matchingConfig                   MatchingConfig
		workerConfig                     WorkerConfig
		esConfig                         *esclient.Config
		esClient                         esclient.Client
		mockAdminClient                  map[string]adminservice.AdminServiceClient
		namespaceReplicationTaskExecutor nsreplication.TaskExecutor
		tlsConfigProvider                *encryption.FixedTLSConfigProvider
		captureMetricsHandler            *metricstest.CaptureHandler
		hostsByProtocolByService         map[transferProtocol]map[primitives.ServiceName]static.Hosts

		onGetClaims               func(*authorization.AuthInfo) (*authorization.Claims, error)
		onAuthorize               func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error)
		callbackLock              sync.RWMutex // Must be used for above callbacks
		serviceFxOptions          map[primitives.ServiceName][]fx.Option
		taskCategoryRegistry      tasks.TaskCategoryRegistry
		chasmRegistry             *chasm.Registry
		grpcClientInterceptor     *grpcinject.Interceptor
		replicationStreamRecorder *ReplicationStreamRecorder
		taskQueueRecorder         *TaskQueueRecorder
		spanExporters             map[telemetry.SpanExporterType]sdktrace.SpanExporter
	}

	// FrontendConfig is the config for the frontend service
	FrontendConfig struct {
		NumFrontendHosts int
	}

	// HistoryConfig contains configs for history service
	HistoryConfig struct {
		NumHistoryShards int32
		NumHistoryHosts  int
	}

	// MatchingConfig is the config for the matching service
	MatchingConfig struct {
		NumMatchingHosts int
	}

	// WorkerConfig is the config for the worker service
	WorkerConfig struct {
		NumWorkers    int
		DisableWorker bool // overrides NumWorkers
	}

	// TemporalParams contains everything needed to bootstrap Temporal
	TemporalParams struct {
		ClusterMetadataConfig            *cluster.Config
		PersistenceConfig                config.Persistence
		MetadataMgr                      persistence.MetadataManager
		ClusterMetadataManager           persistence.ClusterMetadataManager
		ShardMgr                         persistence.ShardManager
		ExecutionManager                 persistence.ExecutionManager
		TaskMgr                          persistence.TaskManager
		NamespaceReplicationQueue        persistence.NamespaceReplicationQueue
		AbstractDataStoreFactory         persistenceClient.AbstractDataStoreFactory
		VisibilityStoreFactory           visibility.VisibilityStoreFactory
		Logger                           log.Logger
		ArchiverMetadata                 carchiver.ArchivalMetadata
		ArchiverProvider                 provider.ArchiverProvider
		EnableReadHistoryFromArchival    bool
		FrontendConfig                   FrontendConfig
		HistoryConfig                    HistoryConfig
		MatchingConfig                   MatchingConfig
		WorkerConfig                     WorkerConfig
		ESConfig                         *esclient.Config
		ESClient                         esclient.Client
		MockAdminClient                  map[string]adminservice.AdminServiceClient
		NamespaceReplicationTaskExecutor nsreplication.TaskExecutor
		DynamicConfigOverrides           map[dynamicconfig.Key]any
		TLSConfigProvider                *encryption.FixedTLSConfigProvider
		CaptureMetricsHandler            *metricstest.CaptureHandler
		// ServiceFxOptions is populated by WithFxOptionsForService.
		ServiceFxOptions         map[primitives.ServiceName][]fx.Option
		TaskCategoryRegistry     tasks.TaskCategoryRegistry
		HostsByProtocolByService map[transferProtocol]map[primitives.ServiceName]static.Hosts
		SpanExporters            map[telemetry.SpanExporterType]sdktrace.SpanExporter
	}

	listenHostPort string
	httpPort       int
)

const NamespaceCacheRefreshInterval = time.Second

var chasmFxOptions = fx.Options(
	temporal.ChasmLibraryOptions,
	chasmtests.Module,
)

// newTemporal returns an instance that hosts full temporal in one process
func newTemporal(t *testing.T, params *TemporalParams) *TemporalImpl {
	impl := &TemporalImpl{
		logger:                           params.Logger,
		clusterMetadataConfig:            params.ClusterMetadataConfig,
		persistenceConfig:                params.PersistenceConfig,
		metadataMgr:                      params.MetadataMgr,
		clusterMetadataMgr:               params.ClusterMetadataManager,
		shardMgr:                         params.ShardMgr,
		taskMgr:                          params.TaskMgr,
		executionManager:                 params.ExecutionManager,
		namespaceReplicationQueue:        params.NamespaceReplicationQueue,
		abstractDataStoreFactory:         params.AbstractDataStoreFactory,
		visibilityStoreFactory:           params.VisibilityStoreFactory,
		esConfig:                         params.ESConfig,
		esClient:                         params.ESClient,
		archiverMetadata:                 params.ArchiverMetadata,
		archiverProvider:                 params.ArchiverProvider,
		frontendConfig:                   params.FrontendConfig,
		historyConfig:                    params.HistoryConfig,
		matchingConfig:                   params.MatchingConfig,
		workerConfig:                     params.WorkerConfig,
		mockAdminClient:                  params.MockAdminClient,
		namespaceReplicationTaskExecutor: params.NamespaceReplicationTaskExecutor,
		tlsConfigProvider:                params.TLSConfigProvider,
		captureMetricsHandler:            params.CaptureMetricsHandler,
		dcClient:                         dynamicconfig.NewMemoryClient(),
		testHooks:                        testhooks.NewTestHooks(),
		serviceFxOptions:                 params.ServiceFxOptions,
		taskCategoryRegistry:             params.TaskCategoryRegistry,
		hostsByProtocolByService:         params.HostsByProtocolByService,
		grpcClientInterceptor:            grpcinject.NewInterceptor(),
		replicationStreamRecorder:        NewReplicationStreamRecorder(),
		spanExporters:                    params.SpanExporters,
	}

	// Configure output file path for on-demand logging (call WriteToLog() to write)
	clusterName := params.ClusterMetadataConfig.CurrentClusterName
	outputFile := fmt.Sprintf("/tmp/replication_stream_messages_%s.txt", clusterName)
	impl.replicationStreamRecorder.SetOutputFile(outputFile)

	for k, v := range dynamicConfigOverrides {
		impl.overrideDynamicConfig(t, k, v)
	}
	for k, v := range params.DynamicConfigOverrides {
		impl.overrideDynamicConfig(t, k, v)
	}

	return impl
}

func (c *TemporalImpl) Start() error {
	// create temporal-system namespace, this must be created before starting
	// the services - so directly use the metadataManager to create this
	if err := c.createSystemNamespace(); err != nil {
		return err
	}
	c.startMatching()
	c.startHistory()
	c.startFrontend()
	c.startWorker()

	return nil
}

func (c *TemporalImpl) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	slices.Reverse(c.fxApps) // less log spam if we go backwards
	var errs []error
	for _, app := range c.fxApps {
		errs = append(errs, app.Stop(ctx))
	}

	return multierr.Combine(errs...)
}

func (c *TemporalImpl) makeHostMap(serviceName primitives.ServiceName, self string) map[primitives.ServiceName]static.Hosts {
	hostMap := maps.Clone(c.hostsByProtocolByService[grpcProtocol])
	hosts := hostMap[serviceName]
	hosts.Self = self
	hostMap[serviceName] = hosts
	return hostMap
}

// Use this to get an address for a remote cluster to connect to.
func (c *TemporalImpl) RemoteFrontendGRPCAddress() string {
	return c.hostsByProtocolByService[grpcProtocol][primitives.FrontendService].All[0]
}

func (c *TemporalImpl) FrontendHTTPAddress() string {
	// randomize like a load balancer would
	addrs := c.hostsByProtocolByService[httpProtocol][primitives.FrontendService].All
	return addrs[rand.Intn(len(addrs))]
}

func (c *TemporalImpl) FrontendGRPCAddress() string {
	return c.hostsByProtocolByService[grpcProtocol][primitives.FrontendService].All[0]
}

func (c *TemporalImpl) WorkerGRPCAddress() string {
	return c.hostsByProtocolByService[grpcProtocol][primitives.WorkerService].All[0]
}

func (c *TemporalImpl) AdminClient() adminservice.AdminServiceClient {
	return c.adminClient
}

func (c *TemporalImpl) OperatorClient() operatorservice.OperatorServiceClient {
	return c.operatorClient
}

func (c *TemporalImpl) FrontendClient() workflowservice.WorkflowServiceClient {
	return c.frontendClient
}

func (c *TemporalImpl) HistoryClient() historyservice.HistoryServiceClient {
	return c.historyClient
}

func (c *TemporalImpl) MatchingClient() matchingservice.MatchingServiceClient {
	return c.matchingClient
}

func (c *TemporalImpl) DcClient() *dynamicconfig.MemoryClient {
	return c.dcClient
}

func (c *TemporalImpl) NamespaceRegistries() []namespace.Registry {
	return c.namespaceRegistries
}

func (c *TemporalImpl) ChasmEngine() (chasm.Engine, error) {
	if numHistoryHosts := len(c.hostsByProtocolByService[grpcProtocol][primitives.HistoryService].All); numHistoryHosts != 1 {
		return nil, fmt.Errorf("expected exactly one host for chasm engine, got %d", numHistoryHosts)
	}
	return c.chasmEngine, nil
}

func (c *TemporalImpl) ChasmVisibilityManager() chasm.VisibilityManager {
	return c.chasmVisibilityMgr
}

func (c *TemporalImpl) copyPersistenceConfig() config.Persistence {
	persistenceConfig := copyPersistenceConfig(c.persistenceConfig)
	if c.esConfig != nil {
		esDataStoreName := "es-visibility"
		persistenceConfig.VisibilityStore = esDataStoreName
		persistenceConfig.DataStores[esDataStoreName] = config.DataStore{
			Elasticsearch: c.esConfig,
		}
	}
	return persistenceConfig
}

func (c *TemporalImpl) startFrontend() {
	serviceName := primitives.FrontendService

	// steal these references from one frontend, it doesn't matter which
	var rpcFactory common.RPCFactory
	var historyRawClient resource.HistoryRawClient
	var matchingRawClient resource.MatchingRawClient
	var grpcResolver *membership.GRPCResolver

	for _, host := range c.hostsByProtocolByService[grpcProtocol][serviceName].All {
		logger := log.With(c.logger, tag.Host(host))
		var namespaceRegistry namespace.Registry
		app := fx.New(
			fx.Supply(
				c.copyPersistenceConfig(),
				serviceName,
				c.mockAdminClient,
			),
			fx.Provide(c.frontendConfigProvider),
			fx.Provide(func() listenHostPort { return listenHostPort(host) }),
			fx.Provide(func() httpPort { return mustPortFromAddress(c.FrontendHTTPAddress()) }),
			fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
			fx.Provide(func() log.Logger { return logger }),
			fx.Provide(func() log.ThrottledLogger { return logger }),
			fx.Provide(func() resource.NamespaceLogger { return logger }),
			fx.Provide(c.newRPCFactory),
			fx.Provide(c.GetGrpcClientInterceptor),
			static.MembershipModule(c.makeHostMap(serviceName, host)),
			fx.Provide(func() *cluster.Config { return c.clusterMetadataConfig }),
			fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
			fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
			fx.Provide(sdkClientFactoryProvider),
			fx.Provide(c.GetMetricsHandler),
			fx.Provide(func() []grpc.UnaryServerInterceptor {
				if c.replicationStreamRecorder != nil {
					return []grpc.UnaryServerInterceptor{
						c.replicationStreamRecorder.UnaryServerInterceptor(c.clusterMetadataConfig.CurrentClusterName),
					}
				}
				return nil
			}),
			fx.Provide(func() []grpc.StreamServerInterceptor {
				if c.replicationStreamRecorder != nil {
					return []grpc.StreamServerInterceptor{
						c.replicationStreamRecorder.StreamServerInterceptor(c.clusterMetadataConfig.CurrentClusterName),
					}
				}
				return nil
			}),
			fx.Provide(func() authorization.Authorizer { return c }),
			fx.Provide(func() authorization.ClaimMapper { return c }),
			fx.Provide(func() authorization.JWTAudienceMapper { return nil }),
			fx.Provide(c.newClientFactoryProvider),
			fx.Provide(func() searchattribute.Mapper { return nil }),
			// Comment the line above and uncomment the line below to test with search attributes mapper.
			// fx.Provide(func() searchattribute.Mapper { return NewSearchAttributeTestMapper() }),
			fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
			fx.Provide(persistenceClient.FactoryProvider),
			fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return c.abstractDataStoreFactory }),
			fx.Provide(func() visibility.VisibilityStoreFactory { return c.visibilityStoreFactory }),
			fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
			fx.Decorate(func() testhooks.TestHooks { return c.testHooks }),
			fx.Provide(resource.DefaultSnTaggedLoggerProvider),
			fx.Provide(func() esclient.Client { return c.esClient }),
			fx.Provide(c.GetTLSConfigProvider),
			fx.Provide(c.GetTaskCategoryRegistry),
			temporal.TraceExportModule,
			temporal.ServiceTracingModule,
			frontend.Module,
			fx.Populate(&namespaceRegistry, &rpcFactory, &historyRawClient, &matchingRawClient, &grpcResolver),
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.FrontendService),
			chasmFxOptions,
		)
		err := app.Err()
		if err != nil {
			logger.Fatal("unable to construct frontend service", tag.Error(err))
		}

		c.fxApps = append(c.fxApps, app)
		c.namespaceRegistries = append(c.namespaceRegistries, namespaceRegistry)

		if err := app.Start(context.Background()); err != nil {
			logger.Fatal("unable to start frontend service", tag.Error(err))
		}
	}

	// This connection/clients uses membership to find frontends and load-balance among them.
	connection := rpcFactory.CreateLocalFrontendGRPCConnection()
	c.frontendClient = workflowservice.NewWorkflowServiceClient(connection)
	c.adminClient = adminservice.NewAdminServiceClient(connection)
	c.operatorClient = operatorservice.NewOperatorServiceClient(connection)

	// We also set the history and matching clients here, stealing them from one of the frontends.
	c.historyClient = historyRawClient
	c.matchingClient = matchingRawClient

	// Address for SDKs
	c.frontendMembershipAddress = grpcResolver.MakeURL(serviceName)
}

func (c *TemporalImpl) startHistory() {
	serviceName := primitives.HistoryService

	for _, host := range c.hostsByProtocolByService[grpcProtocol][serviceName].All {
		var namespaceRegistry namespace.Registry
		logger := log.With(c.logger, tag.Host(host))
		app := fx.New(
			fx.Supply(
				c.copyPersistenceConfig(),
				serviceName,
				c.mockAdminClient,
			),
			fx.Provide(c.configProvider),
			fx.Provide(c.GetMetricsHandler),
			fx.Provide(func() listenHostPort { return listenHostPort(host) }),
			fx.Provide(func() httpPort { return mustPortFromAddress(c.FrontendHTTPAddress()) }),
			fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
			fx.Provide(func() log.Logger { return logger }),
			fx.Provide(func() log.ThrottledLogger { return logger }),
			fx.Provide(c.newRPCFactory),
			fx.Provide(c.GetGrpcClientInterceptor),
			fx.Decorate(func(base persistence.ExecutionManager, logger log.Logger) persistence.ExecutionManager {
				// Wrap ExecutionManager with recorder to capture task writes
				// This wraps the FINAL ExecutionManager after all FX processing (metrics, retries, etc.)
				c.taskQueueRecorder = NewTaskQueueRecorder(base, logger)
				return c.taskQueueRecorder
			}),
			fx.Decorate(func(base []grpc.UnaryServerInterceptor) []grpc.UnaryServerInterceptor {
				if c.replicationStreamRecorder != nil {
					return append(base, c.replicationStreamRecorder.UnaryServerInterceptor(c.clusterMetadataConfig.CurrentClusterName))
				}
				return base
			}),
			fx.Provide(func() []grpc.StreamServerInterceptor {
				if c.replicationStreamRecorder != nil {
					return []grpc.StreamServerInterceptor{
						c.replicationStreamRecorder.StreamServerInterceptor(c.clusterMetadataConfig.CurrentClusterName),
					}
				}
				return nil
			}),
			static.MembershipModule(c.makeHostMap(serviceName, host)),
			fx.Provide(func() *cluster.Config { return c.clusterMetadataConfig }),
			fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
			fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
			fx.Provide(sdkClientFactoryProvider),
			fx.Provide(c.newClientFactoryProvider),
			fx.Provide(func() searchattribute.Mapper { return nil }),
			// Comment the line above and uncomment the line below to test with search attributes mapper.
			// fx.Provide(func() searchattribute.Mapper { return NewSearchAttributeTestMapper() }),
			fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
			fx.Provide(persistenceClient.FactoryProvider),
			fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return c.abstractDataStoreFactory }),
			fx.Provide(func() visibility.VisibilityStoreFactory { return c.visibilityStoreFactory }),
			fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
			fx.Decorate(func() testhooks.TestHooks { return c.testHooks }),
			fx.Provide(resource.DefaultSnTaggedLoggerProvider),
			fx.Provide(func() esclient.Client { return c.esClient }),
			fx.Provide(c.GetTLSConfigProvider),
			fx.Provide(c.GetTaskCategoryRegistry),
			temporal.TraceExportModule,
			temporal.ServiceTracingModule,
			history.QueueModule,
			history.Module,
			replication.Module,
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.HistoryService),
			chasmFxOptions,
			fx.Populate(&namespaceRegistry),
			fx.Populate(&c.chasmEngine),
			fx.Populate(&c.chasmVisibilityMgr),
			fx.Populate(&c.chasmRegistry),
		)
		err := app.Err()
		if err != nil {
			logger.Fatal("unable to construct history service", tag.Error(err))
		}
		c.fxApps = append(c.fxApps, app)
		c.namespaceRegistries = append(c.namespaceRegistries, namespaceRegistry)

		if err := app.Start(context.Background()); err != nil {
			logger.Fatal("unable to start history service", tag.Error(err))
		}
	}
}

func (c *TemporalImpl) startMatching() {
	serviceName := primitives.MatchingService

	for _, host := range c.hostsByProtocolByService[grpcProtocol][serviceName].All {
		var namespaceRegistry namespace.Registry
		logger := log.With(c.logger, tag.Host(host))
		app := fx.New(
			fx.Supply(
				c.copyPersistenceConfig(),
				serviceName,
				c.mockAdminClient,
			),
			fx.Provide(c.configProvider),
			fx.Provide(c.GetMetricsHandler),
			fx.Provide(func() listenHostPort { return listenHostPort(host) }),
			fx.Provide(func() httpPort { return mustPortFromAddress(c.FrontendHTTPAddress()) }),
			fx.Provide(func() log.Logger { return logger }),
			fx.Provide(func() log.ThrottledLogger { return logger }),
			fx.Provide(c.newRPCFactory),
			fx.Provide(c.GetGrpcClientInterceptor),
			static.MembershipModule(c.makeHostMap(serviceName, host)),
			fx.Provide(func() *cluster.Config { return c.clusterMetadataConfig }),
			fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
			fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
			fx.Provide(c.newClientFactoryProvider),
			fx.Provide(func() searchattribute.Mapper { return nil }),
			fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
			fx.Provide(persistenceClient.FactoryProvider),
			fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return c.abstractDataStoreFactory }),
			fx.Provide(func() visibility.VisibilityStoreFactory { return c.visibilityStoreFactory }),
			fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
			fx.Decorate(func() testhooks.TestHooks { return c.testHooks }),
			fx.Provide(func() esclient.Client { return c.esClient }),
			fx.Provide(c.GetTLSConfigProvider),
			fx.Provide(resource.DefaultSnTaggedLoggerProvider),
			fx.Provide(c.GetTaskCategoryRegistry),
			temporal.TraceExportModule,
			temporal.ServiceTracingModule,
			matching.Module,
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.MatchingService),
			chasmFxOptions,
			fx.Populate(&namespaceRegistry),
		)
		err := app.Err()
		if err != nil {
			logger.Fatal("unable to start matching service", tag.Error(err))
		}
		c.fxApps = append(c.fxApps, app)
		c.namespaceRegistries = append(c.namespaceRegistries, namespaceRegistry)
		if err := app.Start(context.Background()); err != nil {
			logger.Fatal("unable to start matching service", tag.Error(err))
		}
	}
}

func (c *TemporalImpl) startWorker() {
	serviceName := primitives.WorkerService

	clusterConfigCopy := cluster.Config{
		EnableGlobalNamespace:    c.clusterMetadataConfig.EnableGlobalNamespace,
		FailoverVersionIncrement: c.clusterMetadataConfig.FailoverVersionIncrement,
		MasterClusterName:        c.clusterMetadataConfig.MasterClusterName,
		CurrentClusterName:       c.clusterMetadataConfig.CurrentClusterName,
		ClusterInformation:       maps.Clone(c.clusterMetadataConfig.ClusterInformation),
	}

	for _, host := range c.hostsByProtocolByService[grpcProtocol][serviceName].All {
		var namespaceRegistry namespace.Registry
		logger := log.With(c.logger, tag.Host(host))
		app := fx.New(

			fx.Supply(
				c.copyPersistenceConfig(),
				serviceName,
				c.mockAdminClient,
			),
			fx.Provide(c.configProvider),
			fx.Provide(c.GetMetricsHandler),
			fx.Provide(func() listenHostPort { return listenHostPort(host) }),
			fx.Provide(func() httpPort { return mustPortFromAddress(c.FrontendHTTPAddress()) }),
			fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
			fx.Provide(func() log.Logger { return logger }),
			fx.Provide(func() log.ThrottledLogger { return logger }),
			fx.Provide(c.newRPCFactory),
			fx.Provide(c.GetGrpcClientInterceptor),
			static.MembershipModule(c.makeHostMap(serviceName, host)),
			fx.Provide(func() *cluster.Config { return &clusterConfigCopy }),
			fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
			fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
			fx.Provide(sdkClientFactoryProvider),
			fx.Provide(c.newClientFactoryProvider),
			fx.Provide(func() searchattribute.Mapper { return nil }),
			fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
			fx.Provide(persistenceClient.FactoryProvider),
			fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return c.abstractDataStoreFactory }),
			fx.Provide(func() visibility.VisibilityStoreFactory { return c.visibilityStoreFactory }),
			fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
			fx.Decorate(func() testhooks.TestHooks { return c.testHooks }),
			fx.Provide(resource.DefaultSnTaggedLoggerProvider),
			fx.Provide(func() esclient.Client { return c.esClient }),
			fx.Provide(c.GetTLSConfigProvider),
			fx.Provide(c.GetTaskCategoryRegistry),
			temporal.TraceExportModule,
			temporal.ServiceTracingModule,
			worker.Module,
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.WorkerService),
			chasmFxOptions,
			fx.Populate(&namespaceRegistry),
		)
		err := app.Err()
		if err != nil {
			logger.Fatal("unable to start worker service", tag.Error(err))
		}

		c.fxApps = append(c.fxApps, app)
		c.namespaceRegistries = append(c.namespaceRegistries, namespaceRegistry)
		if err := app.Start(context.Background()); err != nil {
			logger.Fatal("unable to start worker service", tag.Error(err))
		}
	}
}

func (c *TemporalImpl) getFxOptionsForService(serviceName primitives.ServiceName) fx.Option {
	return fx.Options(c.serviceFxOptions[serviceName]...)
}

func (c *TemporalImpl) createSystemNamespace() error {
	err := c.metadataMgr.InitializeSystemNamespaces(context.Background(), c.clusterMetadataConfig.CurrentClusterName)
	if err != nil {
		return fmt.Errorf("failed to create temporal-system namespace: %v", err)
	}
	return nil
}

func (c *TemporalImpl) GetExecutionManager() persistence.ExecutionManager {
	if c.taskQueueRecorder != nil {
		return c.taskQueueRecorder
	}
	return c.executionManager
}

func (c *TemporalImpl) GetTaskQueueRecorder() *TaskQueueRecorder {
	return c.taskQueueRecorder
}

func (c *TemporalImpl) SetTaskQueueRecorder(recorder *TaskQueueRecorder) {
	c.taskQueueRecorder = recorder
}

func (c *TemporalImpl) GetTLSConfigProvider() encryption.TLSConfigProvider {
	// If we just return this directly, the interface will be non-nil but the
	// pointer will be nil
	if c.tlsConfigProvider != nil {
		return c.tlsConfigProvider
	}
	return nil
}

func (c *TemporalImpl) GetGrpcClientInterceptor() *grpcinject.Interceptor {
	return c.grpcClientInterceptor
}

func (c *TemporalImpl) GetTaskCategoryRegistry() tasks.TaskCategoryRegistry {
	return c.taskCategoryRegistry
}

func (c *TemporalImpl) GetCHASMRegistry() *chasm.Registry {
	return c.chasmRegistry
}

func (c *TemporalImpl) TlsConfigProvider() *encryption.FixedTLSConfigProvider {
	return c.tlsConfigProvider
}

func (c *TemporalImpl) CaptureMetricsHandler() *metricstest.CaptureHandler {
	return c.captureMetricsHandler
}

func (c *TemporalImpl) GetMetricsHandler() metrics.Handler {
	if c.captureMetricsHandler != nil {
		return c.captureMetricsHandler
	}
	return metrics.NoopMetricsHandler
}

func (c *TemporalImpl) frontendConfigProvider() *config.Config {
	// Set HTTP port and a test HTTP forwarded header
	return &config.Config{
		Services: map[string]config.Service{
			string(primitives.FrontendService): {
				RPC: config.RPC{
					HTTPPort: int(mustPortFromAddress(c.FrontendHTTPAddress())),
					HTTPAdditionalForwardedHeaders: []string{
						"this-header-forwarded",
						"this-header-prefix-forwarded-*",
					},
				},
			},
		},
		ExporterConfig: telemetry.ExportConfig{
			CustomExporters: c.spanExporters,
		},
	}
}

func (c *TemporalImpl) configProvider(serviceName primitives.ServiceName) *config.Config {
	return &config.Config{
		Services: map[string]config.Service{
			string(serviceName): {
				RPC: config.RPC{},
			},
		},
		ExporterConfig: telemetry.ExportConfig{
			CustomExporters: c.spanExporters,
		},
	}
}

func (c *TemporalImpl) newRPCFactory(
	sn primitives.ServiceName,
	grpcHostPort listenHostPort,
	logger log.Logger,
	grpcResolver *membership.GRPCResolver,
	tlsConfigProvider encryption.TLSConfigProvider,
	monitor membership.Monitor,
	tracingStatsHandler telemetry.ClientStatsHandler,
	grpcClientInterceptor *grpcinject.Interceptor,
	httpPort httpPort,
	metricsHandler metrics.Handler,
) (common.RPCFactory, error) {
	host, portStr, err := net.SplitHostPort(string(grpcHostPort))
	if err != nil {
		return nil, fmt.Errorf("failed parsing host:port: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}
	var frontendTLSConfig *tls.Config
	if tlsConfigProvider != nil {
		if frontendTLSConfig, err = tlsConfigProvider.GetFrontendClientConfig(); err != nil {
			return nil, fmt.Errorf("failed getting client TLS config: %w", err)
		}
	}
	var options []grpc.DialOption
	if tracingStatsHandler != nil {
		options = append(options, grpc.WithStatsHandler(tracingStatsHandler))
	}
	if grpcClientInterceptor != nil {
		options = append(options,
			grpc.WithChainUnaryInterceptor(grpcClientInterceptor.Unary()),
			grpc.WithChainStreamInterceptor(grpcClientInterceptor.Stream()),
		)
	}
	// Add replication stream recorder interceptor
	if c.replicationStreamRecorder != nil {
		options = append(options,
			grpc.WithChainUnaryInterceptor(c.replicationStreamRecorder.UnaryInterceptor(c.clusterMetadataConfig.CurrentClusterName)),
			grpc.WithChainStreamInterceptor(c.replicationStreamRecorder.StreamInterceptor(c.clusterMetadataConfig.CurrentClusterName)),
		)
	}
	rpcConfig := config.RPC{BindOnIP: host, GRPCPort: port, HTTPPort: int(httpPort)}
	cfg := &config.Config{
		Services: map[string]config.Service{
			string(sn): {
				RPC: rpcConfig,
			},
		},
	}
	return rpc.NewFactory(
		cfg,
		sn,
		logger,
		metricsHandler,
		tlsConfigProvider,
		grpcResolver.MakeURL(primitives.FrontendService),
		grpcResolver.MakeURL(primitives.FrontendService),
		int(httpPort),
		frontendTLSConfig,
		options,
		map[primitives.ServiceName][]grpc.DialOption{},
		monitor,
	), nil
}

func (c *TemporalImpl) newClientFactoryProvider(
	config *cluster.Config,
	mockAdminClient map[string]adminservice.AdminServiceClient,
) client.FactoryProvider {
	return &clientFactoryProvider{
		config:          config,
		mockAdminClient: mockAdminClient,
	}
}

type clientFactoryProvider struct {
	config          *cluster.Config
	mockAdminClient map[string]adminservice.AdminServiceClient
}

func (p *clientFactoryProvider) NewFactory(
	rpcFactory common.RPCFactory,
	monitor membership.Monitor,
	metricsHandler metrics.Handler,
	dc *dynamicconfig.Collection,
	testHooks testhooks.TestHooks,
	numberOfHistoryShards int32,
	logger log.Logger,
	throttledLogger log.Logger,
) client.Factory {
	f := client.NewFactoryProvider().NewFactory(
		rpcFactory,
		monitor,
		metricsHandler,
		dc,
		testHooks,
		numberOfHistoryShards,
		logger,
		throttledLogger,
	)
	return &clientFactory{
		Factory:         f,
		config:          p.config,
		mockAdminClient: p.mockAdminClient,
	}
}

type clientFactory struct {
	client.Factory
	config          *cluster.Config
	mockAdminClient map[string]adminservice.AdminServiceClient
}

// override just this one and look up connections in mock admin client map
func (f *clientFactory) NewRemoteAdminClientWithTimeout(rpcAddress string, timeout time.Duration, largeTimeout time.Duration) adminservice.AdminServiceClient {
	var clusterName string
	for name, info := range f.config.ClusterInformation {
		if rpcAddress == info.RPCAddress {
			clusterName = name
		}
	}
	if mock, ok := f.mockAdminClient[clusterName]; ok {
		return mock
	}
	return f.Factory.NewRemoteAdminClientWithTimeout(rpcAddress, timeout, largeTimeout)
}

func (c *TemporalImpl) SetOnGetClaims(fn func(*authorization.AuthInfo) (*authorization.Claims, error)) {
	c.callbackLock.Lock()
	c.onGetClaims = fn
	c.callbackLock.Unlock()
}

func (c *TemporalImpl) GetClaims(authInfo *authorization.AuthInfo) (*authorization.Claims, error) {
	c.callbackLock.RLock()
	onGetClaims := c.onGetClaims
	c.callbackLock.RUnlock()
	if onGetClaims != nil {
		return onGetClaims(authInfo)
	}
	return &authorization.Claims{System: authorization.RoleAdmin}, nil
}

func (c *TemporalImpl) SetOnAuthorize(
	fn func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error),
) {
	c.callbackLock.Lock()
	c.onAuthorize = fn
	c.callbackLock.Unlock()
}

func (c *TemporalImpl) Authorize(
	ctx context.Context,
	caller *authorization.Claims,
	target *authorization.CallTarget,
) (authorization.Result, error) {
	c.callbackLock.RLock()
	onAuthorize := c.onAuthorize
	c.callbackLock.RUnlock()
	if onAuthorize != nil {
		return onAuthorize(ctx, caller, target)
	}
	return authorization.Result{Decision: authorization.DecisionAllow}, nil
}

// copyPersistenceConfig makes a deep copy of persistence config.
// This is just a temp fix for the race condition of persistence config.
// The race condition happens because all the services are using the same datastore map in the config.
// Also all services will retry to modify the maxQPS field in the datastore during start up and use the modified maxQPS value to create a persistence factory.
func copyPersistenceConfig(cfg config.Persistence) config.Persistence {
	var newCfg config.Persistence
	b, err := json.Marshal(cfg)
	if err != nil {
		panic("copy persistence config: " + err.Error())
	} else if err = json.Unmarshal(b, &newCfg); err != nil {
		panic("copy persistence config: " + err.Error())
	}
	return newCfg
}

func sdkClientFactoryProvider(
	grpcResolver *membership.GRPCResolver,
	metricsHandler metrics.Handler,
	logger log.Logger,
	dc *dynamicconfig.Collection,
	tlsConfigProvider encryption.TLSConfigProvider,
) sdk.ClientFactory {
	var tlsConfig *tls.Config
	if tlsConfigProvider != nil {
		var err error
		if tlsConfig, err = tlsConfigProvider.GetFrontendClientConfig(); err != nil {
			panic(err)
		}
	}
	return sdk.NewClientFactory(
		grpcResolver.MakeURL(primitives.FrontendService),
		tlsConfig,
		metricsHandler,
		logger,
		dynamicconfig.WorkerStickyCacheSize.Get(dc),
	)
}

func (c *TemporalImpl) overrideDynamicConfig(t *testing.T, name dynamicconfig.Key, value any) func() {
	cleanup := c.dcClient.OverrideValue(name, value)
	t.Cleanup(cleanup)
	return cleanup
}

func (c *TemporalImpl) injectHook(t *testing.T, hook testhooks.Hook, scope any) func() {
	cleanup := hook.Apply(c.testHooks, scope)
	t.Cleanup(cleanup)
	return cleanup
}

func mustPortFromAddress(addr string) httpPort {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		panic(fmt.Errorf("Invalid address: %w", err))
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Errorf("Cannot parse port: %w", err))
	}
	return httpPort(portInt)
}
