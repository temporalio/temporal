package testcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/rpc/auth"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/tests/testutils"
	"go.uber.org/multierr"
)

type (
	temporalImpl struct {
		clients
		server temporal.Server

		logger       log.Logger
		serverConfig *config.Config

		dcClient                 *dynamicconfig.MemoryClient
		testHooks                testhooks.TestHooks
		hostsByProtocolByService map[transferProtocol]map[primitives.ServiceName]static.Hosts
		baseServerOptions        []temporal.ServerOption
		workerConfig             WorkerConfig

		metadataMgr       persistence.MetadataManager
		tlsConfigProvider *encryption.FixedTLSConfigProvider
		tokenProvider     auth.TokenProvider

		captureMetricsHandler *metricstest.CaptureHandler

		chasmEngine        chasm.Engine
		chasmVisibilityMgr chasm.VisibilityManager

		replicationStreamRecorder *ReplicationStreamRecorder
		historyTaskRecorder       *HistoryTaskRecorder

		callbackLock sync.RWMutex
		onGetClaims  func(*authorization.AuthInfo) (*authorization.Claims, error)
		onAuthorize  func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error)
	}

	// HistoryConfig contains configs for history service
	HistoryConfig struct {
		NumHistoryShards int32
	}

	// WorkerConfig is the config for the worker service
	WorkerConfig struct {
		DisableWorker bool
	}

	// temporalParams contains everything needed to bootstrap Temporal
	temporalParams struct {
		Config                    *config.Config
		MetadataMgr               persistence.MetadataManager
		AbstractDataStoreFactory  persistenceClient.AbstractDataStoreFactory
		VisibilityStoreFactory    visibility.VisibilityStoreFactory
		Logger                    log.Logger
		MockAdminClient           map[string]adminservice.AdminServiceClient
		DynamicConfigOverrides    map[dynamicconfig.Key]any
		TLSConfigProvider         *encryption.FixedTLSConfigProvider
		TokenProvider             auth.TokenProvider
		CaptureMetricsHandler     *metricstest.CaptureHandler
		HostsByProtocolByService  map[transferProtocol]map[primitives.ServiceName]static.Hosts
		WorkerConfig              WorkerConfig
		EnableHistoryTaskRecorder bool
		EnableReplicationRecorder bool
		AdditionalServerOptions   []temporal.ServerOption
	}
)

const NamespaceCacheRefreshInterval = time.Second

// newTemporal returns an instance that hosts full temporal in one process
func newTemporal(t *testing.T, params *temporalParams) *temporalImpl {
	impl := &temporalImpl{
		logger:                    params.Logger,
		serverConfig:              params.Config,
		metadataMgr:               params.MetadataMgr,
		tlsConfigProvider:         params.TLSConfigProvider,
		tokenProvider:             params.TokenProvider,
		captureMetricsHandler:     params.CaptureMetricsHandler,
		dcClient:                  dynamicconfig.NewMemoryClient(),
		testHooks:                 testhooks.NewTestHooks(),
		hostsByProtocolByService:  params.HostsByProtocolByService,
		workerConfig:              params.WorkerConfig,
		replicationStreamRecorder: NewReplicationStreamRecorder(),
	}

	// Base options are independent of which services this test cluster starts.
	// [Start] adds the per-service config and static host map.
	baseServerOptions := []temporal.ServerOption{
		temporal.WithLogger(impl.logger),
		temporal.WithNamespaceLogger(impl.logger),
		temporal.WithDynamicConfigClient(impl.dcClient),
		temporal.WithCustomDataStoreFactory(params.AbstractDataStoreFactory),
		temporal.WithCustomVisibilityStoreFactory(params.VisibilityStoreFactory),
		temporal.WithClientFactoryProvider(&clientFactoryProvider{
			config:          params.Config.ClusterMetadata,
			mockAdminClient: params.MockAdminClient,
		}),
		temporal.WithTestHooks(impl.testHooks),
		temporal.WithAuthorizer(impl),
		temporal.WithClaimMapper(func(*config.Config) authorization.ClaimMapper { return impl }),
		temporal.WithAudienceGetter(func(*config.Config) authorization.JWTAudienceMapper { return nil }),
		temporal.WithSearchAttributesMapper(nil),
		temporal.WithPersistenceServiceResolver(resolver.NewNoopResolver()),
		temporal.WithCustomMetricsHandler(impl.GetMetricsHandler()),
	}
	if params.TLSConfigProvider != nil {
		baseServerOptions = append(baseServerOptions, temporal.WithTLSConfigFactory(params.TLSConfigProvider))
	}
	if params.TokenProvider != nil {
		baseServerOptions = append(baseServerOptions, temporal.WithTokenProvider(params.TokenProvider))
	}
	if params.EnableReplicationRecorder {
		baseServerOptions = append(baseServerOptions, temporal.WithAdditionalStreamInterceptors(
			impl.replicationStreamRecorder.StreamServerInterceptor(params.Config.ClusterMetadata.CurrentClusterName),
		))
	}
	if params.EnableHistoryTaskRecorder {
		base := temporal.PersistenceFactoryProvider()
		// Only history gets the recording wrapper; other services keep the production factory.
		baseServerOptions = append(baseServerOptions, temporal.WithPersistenceFactoryProvider(func(params persistenceClient.NewFactoryParams) persistenceClient.Factory {
			factory := base(params)
			if params.ServiceName != primitives.HistoryService {
				return factory
			}
			return &historyTaskRecordingPersistenceFactory{
				Factory: factory,
				logger:  params.Logger,
				setRecorder: func(recorder *HistoryTaskRecorder) {
					impl.historyTaskRecorder = recorder
				},
			}
		}))
	}
	impl.baseServerOptions = append(baseServerOptions, params.AdditionalServerOptions...)

	impl.clients = newClients(
		impl.logger,
		impl.hostsByProtocolByService[grpcProtocol],
		impl.tlsConfigProvider,
		impl.GetMetricsHandler(),
		impl.dcClient,
		impl.testHooks,
		impl.serverConfig.Persistence.NumHistoryShards,
		impl.metadataMgr,
		impl.tokenProvider,
	)

	// Configure output file path for on-demand logging (call WriteToLog() to write).
	clusterName := params.Config.ClusterMetadata.CurrentClusterName
	outputDir := filepath.Join(testutils.GetRepoRootDirectory(), ".testoutput")
	require.NoError(t, os.MkdirAll(outputDir, 0o755))
	outputFile := filepath.Join(outputDir, fmt.Sprintf("replication_stream_messages_%s.txt", clusterName))
	impl.replicationStreamRecorder.SetOutputFile(outputFile)

	// Global defaults: applied without cleanup so they persist across cluster reuse.
	for k, v := range defaultDynamicConfigOverrides {
		impl.overrideDynamicConfigForClusterLifetime(k, v)
	}

	// Override Nexus callback URL. This is parameterized on the frontend's HTTP address,
	// so it can't be overridden in the loop above.
	if len(impl.hostsByProtocolByService[httpProtocol][primitives.FrontendService].All) > 0 {
		impl.setNexusCallbackURL()
	}

	// Per-test overrides: cleaned up when the creating test finishes.
	for k, v := range params.DynamicConfigOverrides {
		impl.overrideDynamicConfigForTest(t, k, v)
	}

	return impl
}

func (c *temporalImpl) Start() error {
	// Worker is optional in functional tests; the other services are always
	// needed for a usable in-process cluster.
	services := []primitives.ServiceName{
		primitives.FrontendService,
		primitives.HistoryService,
		primitives.MatchingService,
	}
	if !c.workerConfig.DisableWorker {
		services = append(services, primitives.WorkerService)
	}

	// NewServer receives one config and one static host map for all enabled services,
	// so derive both from the ports allocated by the test cluster.
	serviceNames := make([]string, 0, len(services))
	hostsByService := make(map[primitives.ServiceName]static.Hosts, len(c.hostsByProtocolByService[grpcProtocol]))
	for serviceName, hosts := range c.hostsByProtocolByService[grpcProtocol] {
		if len(hosts.All) > 0 {
			hosts.Self = hosts.All[0]
		}
		hostsByService[serviceName] = hosts
	}

	// Keep the shared test config immutable; each server start needs its own
	// persistence copy and per-service RPC addresses.
	cfg := *c.serverConfig
	cfg.Persistence = copyPersistenceConfig(c.serverConfig.Persistence)
	cfg.Services = make(map[string]config.Service, len(services))
	for _, serviceName := range services {
		serviceNames = append(serviceNames, string(serviceName))
		hosts := hostsByService[serviceName]

		bindIP, port := mustSplitHostPort(hosts.Self)
		rpcConfig := config.RPC{
			BindOnIP: bindIP,
			GRPCPort: int(port),
		}
		if serviceName == primitives.FrontendService {
			_, frontendHTTPPort := mustSplitHostPort(c.hostsByProtocolByService[httpProtocol][serviceName].All[0])
			rpcConfig.HTTPPort = int(frontendHTTPPort)
			rpcConfig.HTTPAdditionalForwardedHeaders = []string{
				"this-header-forwarded",
				"this-header-prefix-forwarded-*",
			}
		}
		cfg.Services[string(serviceName)] = config.Service{RPC: rpcConfig}
	}

	options := []temporal.ServerOption{
		temporal.WithConfig(&cfg),
		temporal.ForServices(serviceNames),
		temporal.WithStaticHosts(hostsByService),
	}
	options = append(options, c.baseServerOptions...)

	// Capture Chasm runtime handles while the server graph is constructed.
	cleanupHooks := testhooks.Set(
		c.testHooks,
		testhooks.HistoryChasmRuntimeProvider,
		func(chasmEngine chasm.Engine, chasmVisibilityManager chasm.VisibilityManager, _ *chasm.Registry) {
			c.chasmEngine = chasmEngine
			c.chasmVisibilityMgr = chasmVisibilityManager
		},
		testhooks.GlobalScope,
	)
	defer cleanupHooks()

	server, err := temporal.NewServer(options...)
	if err != nil {
		return fmt.Errorf("unable to construct temporal server: %w", err)
	}

	if err := server.Start(); err != nil {
		return fmt.Errorf("unable to start temporal server: %w", err)
	}
	c.server = server
	return nil
}

func (c *temporalImpl) Stop() error {
	var errs []error
	if c.server != nil {
		errs = append(errs, c.server.Stop())
	}
	errs = append(errs, c.close()...)
	return multierr.Combine(errs...)
}

// Use this to get an address for a remote cluster to connect to.
func (c *temporalImpl) RemoteFrontendGRPCAddress() string {
	return c.hostsByProtocolByService[grpcProtocol][primitives.FrontendService].All[0]
}

func (c *temporalImpl) FrontendHTTPAddress() string {
	// randomize like a load balancer would
	addrs := c.hostsByProtocolByService[httpProtocol][primitives.FrontendService].All
	return addrs[rand.Intn(len(addrs))]
}

func (c *temporalImpl) FrontendGRPCAddress() string {
	return c.hostsByProtocolByService[grpcProtocol][primitives.FrontendService].All[0]
}

func (c *temporalImpl) WorkerGRPCAddress() string {
	return c.hostsByProtocolByService[grpcProtocol][primitives.WorkerService].All[0]
}

func (c *temporalImpl) DcClient() *dynamicconfig.MemoryClient {
	return c.dcClient
}

func (c *temporalImpl) ChasmContext(ctx context.Context) (context.Context, error) {
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

func (c *temporalImpl) GetHistoryTaskRecorder() *HistoryTaskRecorder {
	return c.historyTaskRecorder
}

func (c *temporalImpl) TLSConfigProvider() *encryption.FixedTLSConfigProvider {
	return c.tlsConfigProvider
}

// Deprecated: metric capture is cluster-global.
// Use (*TestEnv).StartGlobalMetricCapture() or (*TestEnv).StartNamespaceMetricCapture() instead.
func (c *temporalImpl) CaptureMetricsHandler() *metricstest.CaptureHandler {
	return c.captureMetricsHandler
}

func (c *temporalImpl) GetMetricsHandler() metrics.Handler {
	if c.captureMetricsHandler != nil {
		return c.captureMetricsHandler
	}
	return metrics.NoopMetricsHandler
}

func (c *temporalImpl) SetOnGetClaims(fn func(*authorization.AuthInfo) (*authorization.Claims, error)) {
	c.callbackLock.Lock()
	c.onGetClaims = fn
	c.callbackLock.Unlock()
}

func (c *temporalImpl) GetClaims(authInfo *authorization.AuthInfo) (*authorization.Claims, error) {
	c.callbackLock.RLock()
	onGetClaims := c.onGetClaims
	c.callbackLock.RUnlock()
	if onGetClaims != nil {
		return onGetClaims(authInfo)
	}
	return &authorization.Claims{System: authorization.RoleAdmin}, nil
}

func (c *temporalImpl) SetOnAuthorize(
	fn func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error),
) {
	c.callbackLock.Lock()
	c.onAuthorize = fn
	c.callbackLock.Unlock()
}

func (c *temporalImpl) Authorize(
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

func (c *temporalImpl) setNexusCallbackURL() {
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

func (c *temporalImpl) overrideDynamicConfigForClusterLifetime(name dynamicconfig.Key, value any) {
	c.dcClient.PartialOverrideValue(name, value)
}

// overrideDynamicConfigForTest overrides a dynamic config value for the duration of the test.
func (c *temporalImpl) overrideDynamicConfigForTest(t *testing.T, name dynamicconfig.Key, value any) func() {
	cleanup := c.dcClient.PartialOverrideValue(name, value)
	t.Cleanup(cleanup)
	return cleanup
}

func (c *temporalImpl) injectHook(t *testing.T, hook testhooks.Hook, scope any) func() {
	cleanup := hook.Apply(c.testHooks, scope)
	t.Cleanup(cleanup)
	return cleanup
}

func mustSplitHostPort(addr string) (string, int) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		panic(fmt.Errorf("Invalid address: %w", err))
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Errorf("Cannot parse port: %w", err))
	}
	return host, portInt
}
