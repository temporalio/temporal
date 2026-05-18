package testcore

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand"
	"net"
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
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
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
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/grpcinject"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/temporal"
	"go.uber.org/fx"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
)

type (
	TemporalImpl struct {
		// This is used to wait for namespace registries to have noticed a change in some xdc tests.
		namespaceRegistries []namespace.Registry
		// Address for SDK to connect to, using membership grpc resolver.
		frontendMembershipAddress string
		chasmEngine               chasm.Engine
		chasmVisibilityMgr        chasm.VisibilityManager

		// These are routing/load balancing clients but do not do retries:
		adminClient     adminservice.AdminServiceClient
		frontendClient  workflowservice.WorkflowServiceClient
		operatorClient  operatorservice.OperatorServiceClient
		historyClient   historyservice.HistoryServiceClient
		matchingClient  matchingservice.MatchingServiceClient
		schedulerClient schedulerpb.SchedulerServiceClient

		dcClient                 *dynamicconfig.MemoryClient
		testHooks                testhooks.TestHooks
		logger                   log.Logger
		clusterMetadataConfig    *cluster.Config
		persistenceConfig        config.Persistence
		dcRedirectionPolicy      config.DCRedirectionPolicy
		executionManager         persistence.ExecutionManager
		abstractDataStoreFactory persistenceClient.AbstractDataStoreFactory
		visibilityStoreFactory   visibility.VisibilityStoreFactory
		archiverMetadata         carchiver.ArchivalMetadata
		archiverProvider         provider.ArchiverProvider
		workerConfig             WorkerConfig
		esConfig                 *esclient.Config
		esClient                 esclient.Client
		mockAdminClient          map[string]adminservice.AdminServiceClient
		tlsConfigProvider        *encryption.FixedTLSConfigProvider
		captureMetricsHandler    *metricstest.CaptureHandler
		hostsByProtocolByService map[transferProtocol]map[primitives.ServiceName]static.Hosts

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

		servers []*temporal.ServerFx
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
		ClusterMetadataConfig    *cluster.Config
		PersistenceConfig        config.Persistence
		ExecutionManager         persistence.ExecutionManager
		AbstractDataStoreFactory persistenceClient.AbstractDataStoreFactory
		VisibilityStoreFactory   visibility.VisibilityStoreFactory
		Logger                   log.Logger
		ArchiverMetadata         carchiver.ArchivalMetadata
		ArchiverProvider         provider.ArchiverProvider
		WorkerConfig             WorkerConfig
		ESConfig                 *esclient.Config
		ESClient                 esclient.Client
		MockAdminClient          map[string]adminservice.AdminServiceClient
		DCRedirectionPolicy      config.DCRedirectionPolicy
		DynamicConfigOverrides   map[dynamicconfig.Key]any
		TLSConfigProvider        *encryption.FixedTLSConfigProvider
		CaptureMetricsHandler    *metricstest.CaptureHandler
		// ServiceFxOptions is populated by focused TestClusterOption helpers.
		ServiceFxOptions         map[primitives.ServiceName][]fx.Option
		TaskCategoryRegistry     tasks.TaskCategoryRegistry
		HostsByProtocolByService map[transferProtocol]map[primitives.ServiceName]static.Hosts
		SpanExporters            map[telemetry.SpanExporterType]sdktrace.SpanExporter
	}

	hostRefs struct {
		namespaceRegistry namespace.Registry
		rpcFactory        common.RPCFactory
		historyRawClient  resource.HistoryRawClient
		matchingRawClient resource.MatchingRawClient
		schedulerClient   schedulerpb.SchedulerServiceClient
		grpcResolver      *membership.GRPCResolver
	}

	historyTaskQueueManagerTestHookWrapper struct {
		persistence.HistoryTaskQueueManager
		testHooks testhooks.TestHooks
	}

	httpPort int
)

const NamespaceCacheRefreshInterval = time.Second

// newTemporal returns an instance that hosts full temporal in one process
func newTemporal(t *testing.T, params *TemporalParams) *TemporalImpl {
	impl := &TemporalImpl{
		logger:                    params.Logger,
		clusterMetadataConfig:     params.ClusterMetadataConfig,
		persistenceConfig:         params.PersistenceConfig,
		executionManager:          params.ExecutionManager,
		abstractDataStoreFactory:  params.AbstractDataStoreFactory,
		visibilityStoreFactory:    params.VisibilityStoreFactory,
		esConfig:                  params.ESConfig,
		esClient:                  params.ESClient,
		archiverMetadata:          params.ArchiverMetadata,
		archiverProvider:          params.ArchiverProvider,
		workerConfig:              params.WorkerConfig,
		mockAdminClient:           params.MockAdminClient,
		dcRedirectionPolicy:       params.DCRedirectionPolicy,
		tlsConfigProvider:         params.TLSConfigProvider,
		captureMetricsHandler:     params.CaptureMetricsHandler,
		dcClient:                  dynamicconfig.NewMemoryClient(),
		testHooks:                 testhooks.NewTestHooks(),
		serviceFxOptions:          params.ServiceFxOptions,
		taskCategoryRegistry:      params.TaskCategoryRegistry,
		hostsByProtocolByService:  params.HostsByProtocolByService,
		grpcClientInterceptor:     grpcinject.NewInterceptor(),
		replicationStreamRecorder: NewReplicationStreamRecorder(),
		spanExporters:             params.SpanExporters,
	}

	// Configure output file path for on-demand logging (call WriteToLog() to write)
	clusterName := params.ClusterMetadataConfig.CurrentClusterName
	outputFile := fmt.Sprintf("/tmp/replication_stream_messages_%s.txt", clusterName)
	impl.replicationStreamRecorder.SetOutputFile(outputFile)

	// Global defaults: applied without cleanup so they persist across cluster reuse.
	for k, v := range defaultDynamicConfigOverrides {
		impl.overrideDynamicConfigForClusterLifetime(k, v)
	}
	// Override Nexus callback URL. This is parameterized on the frontend's HTTP address,
	// so it can't be overriden in the loop above.
	impl.setNexusCallbackURL()
	// Per-test overrides: cleaned up when the creating test finishes.
	for k, v := range params.DynamicConfigOverrides {
		impl.overrideDynamicConfigForTest(t, k, v)
	}
	return impl
}

func (c *TemporalImpl) Start() error {
	for _, serviceName := range []primitives.ServiceName{
		primitives.MatchingService,
		primitives.HistoryService,
		primitives.FrontendService,
		primitives.WorkerService,
	} {
		if serviceName == primitives.WorkerService && c.workerConfig.DisableWorker {
			continue
		}
		for _, host := range c.hostsByProtocolByService[grpcProtocol][serviceName].All {
			if err := c.startHost(serviceName, host); err != nil {
				return multierr.Combine(err, c.Stop())
			}
		}
	}
	return nil
}

func (c *TemporalImpl) Stop() error {
	var errs []error
	for i := len(c.servers) - 1; i >= 0; i-- { // less log spam if we go backwards
		server := c.servers[i]
		errs = append(errs, server.Stop())
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

func (c *TemporalImpl) SchedulerClient() schedulerpb.SchedulerServiceClient {
	return c.schedulerClient
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

func (c *TemporalImpl) startHost(serviceName primitives.ServiceName, host string) error {
	logger := log.With(c.logger, tag.Host(host))
	refs := &hostRefs{}
	opts := c.serverOptionsForHost(serviceName, host, logger, refs)

	server, err := temporal.NewServerFx(temporal.TopLevelModule, opts...)
	if err != nil {
		return fmt.Errorf("unable to construct %s temporal host %s: %w", serviceName, host, err)
	}
	if err := server.Start(); err != nil {
		return fmt.Errorf("unable to start %s temporal host %s: %w", serviceName, host, err)
	}
	c.servers = append(c.servers, server)
	if refs.namespaceRegistry != nil {
		c.namespaceRegistries = append(c.namespaceRegistries, refs.namespaceRegistry)
	}
	if serviceName == primitives.FrontendService && c.frontendClient == nil {
		c.setClientsFromFrontendHost(refs)
	}
	return nil
}

func (c *TemporalImpl) setClientsFromFrontendHost(refs *hostRefs) {
	// This connection/clients uses membership to find frontends and load-balance among them.
	connection := refs.rpcFactory.CreateLocalFrontendGRPCConnection()
	c.frontendClient = workflowservice.NewWorkflowServiceClient(connection)
	c.adminClient = adminservice.NewAdminServiceClient(connection)
	c.operatorClient = operatorservice.NewOperatorServiceClient(connection)

	// We also set the history, matching, and scheduler clients here, stealing them from one of the frontends.
	c.historyClient = refs.historyRawClient
	c.matchingClient = refs.matchingRawClient
	c.schedulerClient = refs.schedulerClient

	// Address for SDKs
	c.frontendMembershipAddress = refs.grpcResolver.MakeURL(primitives.FrontendService)
}

func (c *TemporalImpl) serverOptionsForHost(
	serviceName primitives.ServiceName,
	host string,
	logger log.Logger,
	refs *hostRefs,
) []temporal.ServerOption {
	options := []temporal.ServerOption{
		temporal.WithConfig(c.configForHost(serviceName, host)),
		temporal.ForServices([]string{string(serviceName)}),
		temporal.WithStaticHosts(c.makeHostMap(serviceName, host)),
		temporal.WithLogger(logger),
		temporal.WithNamespaceLogger(logger),
		temporal.WithDynamicConfigClient(c.dcClient),
		temporal.WithCustomDataStoreFactory(c.abstractDataStoreFactory),
		temporal.WithCustomVisibilityStoreFactory(c.visibilityStoreFactory),
		temporal.WithClientFactoryProvider(c.newClientFactoryProvider(c.clusterMetadataConfig, c.mockAdminClient)),
		temporal.WithAuthorizer(c),
		temporal.WithClaimMapper(func(*config.Config) authorization.ClaimMapper { return c }),
		temporal.WithAudienceGetter(func(*config.Config) authorization.JWTAudienceMapper { return nil }),
		temporal.WithSearchAttributesMapper(nil),
		temporal.WithPersistenceServiceResolver(resolver.NewNoopResolver()),
		temporal.WithCustomMetricsHandler(c.GetMetricsHandler()),
		temporal.WithFxOptionsForService(serviceName, c.fxOptionsForHost(serviceName, refs)),
	}
	if c.tlsConfigProvider != nil {
		options = append(options, temporal.WithTLSConfigFactory(c.tlsConfigProvider))
	}
	if serviceName == primitives.FrontendService && c.replicationStreamRecorder != nil {
		options = append(options, temporal.WithChainedFrontendGrpcInterceptors(
			c.replicationStreamRecorder.UnaryServerInterceptor(c.clusterMetadataConfig.CurrentClusterName),
		))
		options = append(options, temporal.WithChainedFrontendGrpcStreamInterceptors(
			c.replicationStreamRecorder.StreamServerInterceptor(c.clusterMetadataConfig.CurrentClusterName),
		))
	}
	return options
}

func (c *TemporalImpl) fxOptionsForHost(
	serviceName primitives.ServiceName,
	refs *hostRefs,
) fx.Option {
	opts := []fx.Option{
		fx.Supply(c.mockAdminClient),
		fx.Decorate(func() testhooks.TestHooks { return c.testHooks }),
		fx.Decorate(func() tasks.TaskCategoryRegistry { return c.taskCategoryRegistry }),
		fx.Decorate(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
		fx.Decorate(func() provider.ArchiverProvider { return c.archiverProvider }),
		fx.Decorate(func() persistenceClient.AbstractDataStoreFactory { return c.abstractDataStoreFactory }),
		fx.Decorate(func() visibility.VisibilityStoreFactory { return c.visibilityStoreFactory }),
		fx.Decorate(func() esclient.Client { return c.esClient }),
		fx.Decorate(func() client.FactoryProvider {
			return c.newClientFactoryProvider(c.clusterMetadataConfig, c.mockAdminClient)
		}),
		fx.Decorate(func() searchattribute.Mapper { return nil }),
		fx.Decorate(c.decorateClientDialOptions),
		fx.Populate(&refs.namespaceRegistry),
		chasmtests.Module,
		fx.Options(c.serviceFxOptions[serviceName]...),
	}
	switch serviceName {
	case primitives.FrontendService:
		opts = append(opts,
			fx.Populate(&refs.rpcFactory, &refs.historyRawClient, &refs.matchingRawClient, &refs.schedulerClient, &refs.grpcResolver),
		)
	case primitives.HistoryService:
		opts = append(opts,
			fx.Decorate(func(base persistence.ExecutionManager, logger log.Logger) persistence.ExecutionManager {
				// Wrap ExecutionManager with recorder to capture task writes
				// This wraps the FINAL ExecutionManager after all FX processing (metrics, retries, etc.)
				c.taskQueueRecorder = NewTaskQueueRecorder(base, logger)
				return c.taskQueueRecorder
			}),
			fx.Decorate(func(base persistence.HistoryTaskQueueManager) persistence.HistoryTaskQueueManager {
				return &historyTaskQueueManagerTestHookWrapper{
					HistoryTaskQueueManager: base,
					testHooks:               c.testHooks,
				}
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
			fx.Populate(&c.chasmEngine),
			fx.Populate(&c.chasmVisibilityMgr),
			fx.Populate(&c.chasmRegistry),
		)
	}
	return fx.Options(opts...)
}

func (m *historyTaskQueueManagerTestHookWrapper) DeleteTasks(
	ctx context.Context,
	request *persistence.DeleteTasksRequest,
) (*persistence.DeleteTasksResponse, error) {
	if hook, ok := testhooks.Get(m.testHooks, testhooks.HistoryTaskQueueBeforeDeleteTasks, testhooks.GlobalScope); ok {
		if err := hook(request); err != nil {
			return nil, err
		}
	}
	return m.HistoryTaskQueueManager.DeleteTasks(ctx, request)
}

func (m *historyTaskQueueManagerTestHookWrapper) EnqueueTask(
	ctx context.Context,
	request *persistence.EnqueueTaskRequest,
) (*persistence.EnqueueTaskResponse, error) {
	response, err := m.HistoryTaskQueueManager.EnqueueTask(ctx, request)
	if err != nil {
		return response, err
	}
	if hook, ok := testhooks.Get(
		m.testHooks,
		testhooks.HistoryTaskInterceptor,
		namespace.ID(request.Task.GetNamespaceID()),
	); ok {
		hook(request.Task)
	}
	return response, nil
}

func (c *TemporalImpl) decorateClientDialOptions(
	base map[primitives.ServiceName][]grpc.DialOption,
) map[primitives.ServiceName][]grpc.DialOption {
	dialOptions := c.clientDialOptions()
	if len(dialOptions) == 0 {
		return base
	}

	options := maps.Clone(base)
	for _, serviceName := range []primitives.ServiceName{
		primitives.FrontendService,
		primitives.InternalFrontendService,
		primitives.HistoryService,
		primitives.MatchingService,
	} {
		options[serviceName] = append(options[serviceName], dialOptions...)
	}
	return options
}

func (c *TemporalImpl) clientDialOptions() []grpc.DialOption {
	var options []grpc.DialOption
	if c.grpcClientInterceptor != nil {
		options = append(options,
			grpc.WithChainUnaryInterceptor(c.grpcClientInterceptor.Unary()),
			grpc.WithChainStreamInterceptor(c.grpcClientInterceptor.Stream()),
		)
	}
	if c.replicationStreamRecorder != nil {
		options = append(options,
			grpc.WithChainUnaryInterceptor(c.replicationStreamRecorder.UnaryInterceptor(c.clusterMetadataConfig.CurrentClusterName)),
			grpc.WithChainStreamInterceptor(c.replicationStreamRecorder.StreamInterceptor(c.clusterMetadataConfig.CurrentClusterName)),
		)
	}
	return options
}

func (c *TemporalImpl) configForHost(serviceName primitives.ServiceName, host string) *config.Config {
	bindIP, port := mustSplitHostPort(host)
	rpcConfig := config.RPC{
		BindOnIP: bindIP,
		GRPCPort: int(port),
	}
	if serviceName == primitives.FrontendService {
		// Set HTTP port and a test HTTP forwarded header
		_, httpPort := mustSplitHostPort(c.FrontendHTTPAddress())
		rpcConfig.HTTPPort = int(httpPort)
		rpcConfig.HTTPAdditionalForwardedHeaders = []string{
			"this-header-forwarded",
			"this-header-prefix-forwarded-*",
		}
	}

	return &config.Config{
		Global: config.Global{
			Membership: config.Membership{
				MaxJoinDuration: time.Second,
			},
		},
		Persistence:         c.copyPersistenceConfig(),
		ClusterMetadata:     c.clusterMetadataConfig,
		DCRedirectionPolicy: c.dcRedirectionPolicy,
		Services: map[string]config.Service{
			string(serviceName): {
				RPC: rpcConfig,
			},
		},
		Archival: config.Archival{
			History: config.HistoryArchival{
				State:      archivalStateString(c.archiverMetadata.GetHistoryConfig().StaticClusterState()),
				EnableRead: c.archiverMetadata.GetHistoryConfig().ReadEnabled(),
			},
			Visibility: config.VisibilityArchival{
				State:      archivalStateString(c.archiverMetadata.GetVisibilityConfig().StaticClusterState()),
				EnableRead: c.archiverMetadata.GetVisibilityConfig().ReadEnabled(),
			},
		},
		ExporterConfig: telemetry.ExportConfig{
			CustomExporters: c.spanExporters,
		},
	}
}

func archivalStateString(state carchiver.ArchivalState) string {
	switch state {
	case carchiver.ArchivalEnabled:
		return config.ArchivalEnabled
	case carchiver.ArchivalPaused:
		return config.ArchivalPaused
	default:
		return config.ArchivalDisabled
	}
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

// Deprecated: metric capture is cluster-global.
// Use (*TestEnv).StartGlobalMetricCapture() or (*TestEnv).StartNamespaceMetricCapture() instead.
func (c *TemporalImpl) CaptureMetricsHandler() *metricstest.CaptureHandler {
	return c.captureMetricsHandler
}

func (c *TemporalImpl) GetMetricsHandler() metrics.Handler {
	if c.captureMetricsHandler != nil {
		return c.captureMetricsHandler
	}
	return metrics.NoopMetricsHandler
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

func (c *TemporalImpl) setNexusCallbackURL() {
	// Set Nexus callback URL with the cluster's HTTP address. This is a sensible default to avoid
	// users to need to manually set this.
	//nolint:revive // test callback endpoints are served by the local HTTP API in functional tests
	nexusCallbackTemplate := fmt.Sprintf(
		"http://%s/namespaces/{{.NamespaceName}}/nexus/callback",
		c.FrontendHTTPAddress(),
	)
	c.overrideDynamicConfigForClusterLifetime(nexusoperations.CallbackURLTemplate.Key(), nexusCallbackTemplate)
	c.overrideDynamicConfigForClusterLifetime(chasmnexus.CallbackURLTemplate.Key(), nexusCallbackTemplate)
}

func (c *TemporalImpl) overrideDynamicConfigForClusterLifetime(name dynamicconfig.Key, value any) {
	c.dcClient.PartialOverrideValue(name, value)
}

// overrideDynamicConfigForTest overrides a dynamic config value for the duration of the test.
func (c *TemporalImpl) overrideDynamicConfigForTest(t *testing.T, name dynamicconfig.Key, value any) func() {
	cleanup := c.dcClient.PartialOverrideValue(name, value)
	t.Cleanup(cleanup)
	return cleanup
}

func (c *TemporalImpl) injectHook(t *testing.T, hook testhooks.Hook, scope any) func() {
	cleanup := hook.Apply(c.testHooks, scope)
	t.Cleanup(cleanup)
	return cleanup
}

func mustSplitHostPort(addr string) (string, httpPort) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		panic(fmt.Errorf("Invalid address: %w", err))
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Errorf("Cannot parse port: %w", err))
	}
	return host, httpPort(portInt)
}
