package testcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
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
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/rpc/auth"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/temporal"
	"go.uber.org/fx"
	"go.uber.org/multierr"
)

type (
	TemporalImpl struct {
		chasmEngine        chasm.Engine
		chasmVisibilityMgr chasm.VisibilityManager

		clients clients

		dcClient                 *dynamicconfig.MemoryClient
		testHooks                testhooks.TestHooks
		logger                   log.Logger
		config                   *config.Config
		abstractDataStoreFactory persistenceClient.AbstractDataStoreFactory
		visibilityStoreFactory   visibility.VisibilityStoreFactory
		mockAdminClient          map[string]adminservice.AdminServiceClient
		tlsConfigProvider        *encryption.FixedTLSConfigProvider
		tokenProvider            auth.TokenProvider
		captureMetricsHandler    *metricstest.CaptureHandler
		hostsByProtocolByService map[transferProtocol]map[primitives.ServiceName]static.Hosts
		serverOptions            []temporal.ServerOption

		onGetClaims               func(*authorization.AuthInfo) (*authorization.Claims, error)
		onAuthorize               func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error)
		callbackLock              sync.RWMutex // Must be used for above callbacks
		chasmRegistry             *chasm.Registry
		replicationStreamRecorder *ReplicationStreamRecorder
		historyTaskRecorder       *HistoryTaskRecorder
		enableHistoryTaskRecorder bool

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
		Config                    *config.Config
		AbstractDataStoreFactory  persistenceClient.AbstractDataStoreFactory
		VisibilityStoreFactory    visibility.VisibilityStoreFactory
		Logger                    log.Logger
		MockAdminClient           map[string]adminservice.AdminServiceClient
		DynamicConfigOverrides    map[dynamicconfig.Key]any
		TLSConfigProvider         *encryption.FixedTLSConfigProvider
		TokenProvider             auth.TokenProvider
		CaptureMetricsHandler     *metricstest.CaptureHandler
		HostsByProtocolByService  map[transferProtocol]map[primitives.ServiceName]static.Hosts
		EnableHistoryTaskRecorder bool
		ServerOptions             []temporal.ServerOption
	}

	httpPort int
)

const NamespaceCacheRefreshInterval = time.Second

// newTemporal returns an instance that hosts full temporal in one process
func newTemporal(t *testing.T, params *TemporalParams) *TemporalImpl {
	impl := &TemporalImpl{
		logger:                    params.Logger,
		config:                    params.Config,
		abstractDataStoreFactory:  params.AbstractDataStoreFactory,
		visibilityStoreFactory:    params.VisibilityStoreFactory,
		mockAdminClient:           params.MockAdminClient,
		tlsConfigProvider:         params.TLSConfigProvider,
		tokenProvider:             params.TokenProvider,
		captureMetricsHandler:     params.CaptureMetricsHandler,
		serverOptions:             params.ServerOptions,
		dcClient:                  dynamicconfig.NewMemoryClient(),
		testHooks:                 testhooks.NewTestHooks(),
		hostsByProtocolByService:  params.HostsByProtocolByService,
		replicationStreamRecorder: NewReplicationStreamRecorder(),
		enableHistoryTaskRecorder: params.EnableHistoryTaskRecorder,
	}
	impl.clients = newClients(
		impl.logger,
		impl.hostsByProtocolByService[grpcProtocol],
		impl.tlsConfigProvider,
	)
	impl.clients.enableMatchingClientRouting(
		dynamicconfig.NewCollection(impl.dcClient, impl.logger),
		impl.testHooks,
		impl.GetMetricsHandler(),
	)
	// Configure output file path for on-demand logging (call WriteToLog() to write)
	clusterName := params.Config.ClusterMetadata.CurrentClusterName
	outputFile := fmt.Sprintf("/tmp/replication_stream_messages_%s.txt", clusterName)
	impl.replicationStreamRecorder.SetOutputFile(outputFile)

	// Global defaults: applied without cleanup so they persist across cluster reuse.
	for k, v := range defaultDynamicConfigOverrides {
		impl.overrideDynamicConfigForClusterLifetime(k, v)
	}
	// Override Nexus callback URL. This is parameterized on the frontend's HTTP address,
	// so it can't be overriden in the loop above.
	if len(impl.hostsByProtocolByService[httpProtocol][primitives.FrontendService].All) > 0 {
		impl.setNexusCallbackURL()
	}
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
	errs = append(errs, c.clients.close()...)
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

func (c *TemporalImpl) frontendHTTPAddressForHost(serviceName primitives.ServiceName, host string) string {
	httpAddrs := c.hostsByProtocolByService[httpProtocol][primitives.FrontendService].All
	if serviceName != primitives.FrontendService {
		return httpAddrs[0]
	}
	grpcAddrs := c.hostsByProtocolByService[grpcProtocol][primitives.FrontendService].All
	for i, addr := range grpcAddrs {
		if addr == host && i < len(httpAddrs) {
			return httpAddrs[i]
		}
	}
	return httpAddrs[0]
}

func (c *TemporalImpl) FrontendGRPCAddress() string {
	return c.hostsByProtocolByService[grpcProtocol][primitives.FrontendService].All[0]
}

func (c *TemporalImpl) WorkerGRPCAddress() string {
	return c.hostsByProtocolByService[grpcProtocol][primitives.WorkerService].All[0]
}

func (c *TemporalImpl) AdminClient() adminservice.AdminServiceClient {
	return c.clients.AdminClient()
}

func (c *TemporalImpl) OperatorClient() operatorservice.OperatorServiceClient {
	return c.clients.OperatorClient()
}

func (c *TemporalImpl) FrontendClient() workflowservice.WorkflowServiceClient {
	return c.clients.FrontendClient()
}

func (c *TemporalImpl) HistoryClient() historyservice.HistoryServiceClient {
	return c.clients.HistoryClient()
}

func (c *TemporalImpl) MatchingClient() matchingservice.MatchingServiceClient {
	return c.clients.MatchingClient()
}

func (c *TemporalImpl) SchedulerClient() schedulerpb.SchedulerServiceClient {
	return c.clients.SchedulerClient()
}

func (c *TemporalImpl) DcClient() *dynamicconfig.MemoryClient {
	return c.dcClient
}

func (c *TemporalImpl) ChasmContext(ctx context.Context) (context.Context, error) {
	if numHistoryHosts := len(c.hostsByProtocolByService[grpcProtocol][primitives.HistoryService].All); numHistoryHosts != 1 {
		return nil, fmt.Errorf("expected exactly one history host for chasm context, got %d", numHistoryHosts)
	}
	if c.chasmEngine == nil || c.chasmVisibilityMgr == nil {
		return nil, errors.New("chasm context is not available")
	}
	ctx = chasm.NewEngineContext(ctx, c.chasmEngine)
	ctx = chasm.NewVisibilityManagerContext(ctx, c.chasmVisibilityMgr)
	return ctx, nil
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

func (c *TemporalImpl) startHost(serviceName primitives.ServiceName, host string) error {
	logger := log.With(c.logger, tag.Host(host))
	opts := c.serverOptionsForHost(serviceName, host, logger)

	cleanupHooks := c.installHostTestHooks(serviceName)
	defer cleanupHooks()

	server, err := temporal.NewServerFx(c.topLevelModuleForHost(serviceName), opts...)
	if err != nil {
		return fmt.Errorf("unable to construct %s temporal host %s: %w", serviceName, host, err)
	}
	if err := server.Start(); err != nil {
		return fmt.Errorf("unable to start %s temporal host %s: %w", serviceName, host, err)
	}
	c.servers = append(c.servers, server)
	return nil
}

func (c *TemporalImpl) topLevelModuleForHost(serviceName primitives.ServiceName) fx.Option {
	options := []fx.Option{
		temporal.TopLevelModule,
		fx.Decorate(func() testhooks.TestHooks {
			return c.testHooks
		}),
	}
	if serviceName != primitives.HistoryService || !c.enableHistoryTaskRecorder {
		return fx.Options(options...)
	}
	options = append(
		options,
		fx.Decorate(func(base persistenceClient.FactoryProviderFn) persistenceClient.FactoryProviderFn {
			return func(params persistenceClient.NewFactoryParams) persistenceClient.Factory {
				return &historyTaskRecordingPersistenceFactory{
					Factory: base(params),
					logger:  params.Logger,
					setRecorder: func(recorder *HistoryTaskRecorder) {
						c.historyTaskRecorder = recorder
					},
				}
			}
		}),
	)
	return fx.Options(options...)
}

func (c *TemporalImpl) serverOptionsForHost(
	serviceName primitives.ServiceName,
	host string,
	logger log.Logger,
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
		temporal.WithClientFactoryProvider(c.newClientFactoryProvider(c.config.ClusterMetadata, c.mockAdminClient)),
		temporal.WithAuthorizer(c),
		temporal.WithClaimMapper(func(*config.Config) authorization.ClaimMapper { return c }),
		temporal.WithAudienceGetter(func(*config.Config) authorization.JWTAudienceMapper { return nil }),
		temporal.WithSearchAttributesMapper(nil),
		temporal.WithPersistenceServiceResolver(resolver.NewNoopResolver()),
		temporal.WithCustomMetricsHandler(c.GetMetricsHandler()),
	}
	if c.tlsConfigProvider != nil {
		options = append(options, temporal.WithTLSConfigFactory(c.tlsConfigProvider))
	}
	if c.tokenProvider != nil {
		options = append(options, temporal.WithTokenProvider(c.tokenProvider))
	}
	options = append(options, c.serverOptions...)
	return options
}

func (c *TemporalImpl) installHostTestHooks(
	serviceName primitives.ServiceName,
) func() {
	var cleanups []func()
	addCleanup := func(cleanup func()) {
		cleanups = append(cleanups, cleanup)
	}

	switch serviceName {
	case primitives.HistoryService:
		addCleanup(testhooks.Set(
			c.testHooks,
			testhooks.HistoryChasmRuntimeProvider,
			func(chasmEngine chasm.Engine, chasmVisibilityManager chasm.VisibilityManager, chasmRegistry *chasm.Registry) {
				c.chasmEngine = chasmEngine
				c.chasmVisibilityMgr = chasmVisibilityManager
				c.chasmRegistry = chasmRegistry
			},
			testhooks.GlobalScope,
		))
	default:
	}

	return func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
}

func (c *TemporalImpl) configForHost(serviceName primitives.ServiceName, host string) *config.Config {
	bindIP, port := mustSplitHostPort(host)
	rpcConfig := config.RPC{
		BindOnIP: bindIP,
		GRPCPort: int(port),
	}
	frontendGRPCAddress := c.hostsByProtocolByService[grpcProtocol][primitives.FrontendService].All[0]
	if serviceName == primitives.FrontendService {
		frontendGRPCAddress = host
	}
	frontendBindIP, frontendGRPCPort := mustSplitHostPort(frontendGRPCAddress)
	_, frontendHTTPPort := mustSplitHostPort(c.frontendHTTPAddressForHost(serviceName, host))
	// Set HTTP port and a test HTTP forwarded header
	frontendRPCConfig := config.RPC{
		BindOnIP: frontendBindIP,
		GRPCPort: int(frontendGRPCPort),
		HTTPPort: int(frontendHTTPPort),
		HTTPAdditionalForwardedHeaders: []string{
			"this-header-forwarded",
			"this-header-prefix-forwarded-*",
		},
	}

	cfg := *c.config
	cfg.Persistence = copyPersistenceConfig(c.config.Persistence)
	cfg.Services = map[string]config.Service{
		string(primitives.FrontendService): {
			RPC: frontendRPCConfig,
		},
	}
	if serviceName != primitives.FrontendService {
		cfg.Services[string(serviceName)] = config.Service{
			RPC: rpcConfig,
		}
	}
	return &cfg
}

func (c *TemporalImpl) GetHistoryTaskRecorder() *HistoryTaskRecorder {
	return c.historyTaskRecorder
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

	// Preserve fault injection injectors after the JSON copy.
	for name, dataStore := range cfg.DataStores {
		if dataStore.FaultInjection == nil {
			continue
		}
		newDataStore := newCfg.DataStores[name]
		if newDataStore.FaultInjection == nil {
			newDataStore.FaultInjection = &config.FaultInjection{}
		}
		newDataStore.FaultInjection.Injector = dataStore.FaultInjection.Injector
		newCfg.DataStores[name] = newDataStore
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
