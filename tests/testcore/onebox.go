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

	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
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
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
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

const (
	frontendPort     = 7134
	frontendHTTPPort = 7144
	historyPort      = 7132
	matchingPort     = 7136
	workerPort       = 7138 // not really listening
)

type (
	TemporalImpl struct {
		fxApps []*fx.App

		// This is used to wait for namespace registries to have noticed a change in some xdc tests.
		frontendNamespaceRegistries []namespace.Registry
		// Address for SDK to connect to, using membership grpc resolver.
		frontendMembershipAddress string

		// These are routing/load balancing clients but do not do retries:
		adminClient    adminservice.AdminServiceClient
		frontendClient workflowservice.WorkflowServiceClient
		operatorClient operatorservice.OperatorServiceClient
		historyClient  historyservice.HistoryServiceClient
		matchingClient matchingservice.MatchingServiceClient

		dcClient                         *dynamicconfig.MemoryClient
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
		clusterNo                        int // cluster number
		archiverMetadata                 carchiver.ArchivalMetadata
		archiverProvider                 provider.ArchiverProvider
		frontendConfig                   FrontendConfig
		historyConfig                    HistoryConfig
		matchingConfig                   MatchingConfig
		workerConfig                     WorkerConfig
		esConfig                         *esclient.Config
		esClient                         esclient.Client
		mockAdminClient                  map[string]adminservice.AdminServiceClient
		namespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor
		spanExporters                    []otelsdktrace.SpanExporter
		tlsConfigProvider                *encryption.FixedTLSConfigProvider
		captureMetricsHandler            *metricstest.CaptureHandler
		hostsByService                   map[primitives.ServiceName]static.Hosts

		onGetClaims          func(*authorization.AuthInfo) (*authorization.Claims, error)
		onAuthorize          func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error)
		callbackLock         sync.RWMutex // Must be used for above callbacks
		serviceFxOptions     map[primitives.ServiceName][]fx.Option
		taskCategoryRegistry tasks.TaskCategoryRegistry
	}

	// FrontendConfig is the config for the frontend service
	FrontendConfig struct {
		NumFrontendHosts int
	}

	// HistoryConfig contains configs for history service
	HistoryConfig struct {
		NumHistoryShards           int32
		NumHistoryHosts            int
		HistoryCountLimitError     int
		HistoryCountLimitWarn      int
		HistorySizeLimitError      int
		HistorySizeLimitWarn       int
		BlobSizeLimitError         int
		BlobSizeLimitWarn          int
		MutableStateSizeLimitError int
		MutableStateSizeLimitWarn  int
	}

	// MatchingConfig is the config for the matching service
	MatchingConfig struct {
		NumMatchingHosts int
	}

	// WorkerConfig is the config for the worker service
	WorkerConfig struct {
		EnableArchiver   bool
		EnableReplicator bool
		NumWorkers       int
		DisableWorker    bool // overrides NumWorkers
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
		ClusterNo                        int
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
		NamespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor
		SpanExporters                    []otelsdktrace.SpanExporter
		DynamicConfigOverrides           map[dynamicconfig.Key]interface{}
		TLSConfigProvider                *encryption.FixedTLSConfigProvider
		CaptureMetricsHandler            *metricstest.CaptureHandler
		// ServiceFxOptions is populated by WithFxOptionsForService.
		ServiceFxOptions     map[primitives.ServiceName][]fx.Option
		TaskCategoryRegistry tasks.TaskCategoryRegistry
	}

	listenHostPort string
	httpPort       int
)

const NamespaceCacheRefreshInterval = time.Second

var (
	// Override values for dynamic configs
	staticOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.FrontendRPS.Key():                                         3000,
		dynamicconfig.FrontendMaxNamespaceVisibilityRPSPerInstance.Key():        50,
		dynamicconfig.FrontendMaxNamespaceVisibilityBurstRatioPerInstance.Key(): 1,
		dynamicconfig.ReplicationTaskProcessorErrorRetryMaxAttempts.Key():       1,
		dynamicconfig.SecondaryVisibilityWritingMode.Key():                      visibility.SecondaryVisibilityWritingModeOff,
		dynamicconfig.WorkflowTaskHeartbeatTimeout.Key():                        5 * time.Second,
		dynamicconfig.ReplicationTaskFetcherAggregationInterval.Key():           200 * time.Millisecond,
		dynamicconfig.ReplicationTaskFetcherErrorRetryWait.Key():                50 * time.Millisecond,
		dynamicconfig.ReplicationTaskProcessorErrorRetryWait.Key():              time.Millisecond,
		dynamicconfig.ClusterMetadataRefreshInterval.Key():                      100 * time.Millisecond,
		dynamicconfig.NamespaceCacheRefreshInterval.Key():                       NamespaceCacheRefreshInterval,
		dynamicconfig.ReplicationEnableUpdateWithNewTaskMerge.Key():             true,
		dynamicconfig.ValidateUTF8SampleRPCRequest.Key():                        1.0,
		dynamicconfig.ValidateUTF8SampleRPCResponse.Key():                       1.0,
		dynamicconfig.ValidateUTF8SamplePersistence.Key():                       1.0,
		dynamicconfig.ValidateUTF8FailRPCRequest.Key():                          true,
		dynamicconfig.ValidateUTF8FailRPCResponse.Key():                         true,
		dynamicconfig.ValidateUTF8FailPersistence.Key():                         true,
		dynamicconfig.EnableWorkflowExecutionTimeoutTimer.Key():                 true,
		dynamicconfig.FrontendMaskInternalErrorDetails.Key():                    false,
	}
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
		clusterNo:                        params.ClusterNo,
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
		spanExporters:                    params.SpanExporters,
		tlsConfigProvider:                params.TLSConfigProvider,
		captureMetricsHandler:            params.CaptureMetricsHandler,
		dcClient:                         dynamicconfig.NewMemoryClient(),
		serviceFxOptions:                 params.ServiceFxOptions,
		taskCategoryRegistry:             params.TaskCategoryRegistry,
	}

	// set defaults
	const minNodes = 1
	impl.frontendConfig.NumFrontendHosts = max(minNodes, impl.frontendConfig.NumFrontendHosts)
	impl.historyConfig.NumHistoryHosts = max(minNodes, impl.historyConfig.NumHistoryHosts)
	impl.matchingConfig.NumMatchingHosts = max(minNodes, impl.matchingConfig.NumMatchingHosts)
	impl.workerConfig.NumWorkers = max(minNodes, impl.workerConfig.NumWorkers)
	if impl.workerConfig.DisableWorker {
		impl.workerConfig.NumWorkers = 0
	}

	impl.hostsByService = map[primitives.ServiceName]static.Hosts{
		primitives.FrontendService: static.Hosts{All: impl.FrontendGRPCAddresses()},
		primitives.MatchingService: static.Hosts{All: impl.MatchingServiceAddresses()},
		primitives.HistoryService:  static.Hosts{All: impl.HistoryServiceAddresses()},
		primitives.WorkerService:   static.Hosts{All: impl.WorkerServiceAddresses()},
	}

	for k, v := range staticOverrides {
		impl.overrideDynamicConfig(t, k, v)
	}
	for k, v := range params.DynamicConfigOverrides {
		impl.overrideDynamicConfig(t, k, v)
	}
	impl.overrideHistoryDynamicConfig(t)

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
	hostMap := maps.Clone(c.hostsByService)
	hosts := hostMap[serviceName]
	hosts.Self = self
	hostMap[serviceName] = hosts
	return hostMap
}

func (c *TemporalImpl) makeGRPCAddresses(num, port int) []string {
	hosts := make([]string, num)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("127.0.%d.%d:%d", c.clusterNo, i+1, port)
	}
	return hosts
}

func (c *TemporalImpl) FrontendGRPCAddresses() []string {
	return c.makeGRPCAddresses(c.frontendConfig.NumFrontendHosts, frontendPort)
}

// Use this to get an address for the Go SDK to connect to.
func (c *TemporalImpl) FrontendGRPCAddress() string {
	return c.frontendMembershipAddress
}

// Use this to get an address for a remote cluster to connect to.
func (c *TemporalImpl) RemoteFrontendGRPCAddress() string {
	return c.FrontendGRPCAddresses()[0]
}

func (c *TemporalImpl) FrontendHTTPAddress() string {
	// randomize like a load balancer would
	addrs := c.FrontendGRPCAddresses()
	addr := addrs[rand.Intn(len(addrs))]
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		panic(fmt.Errorf("Invalid gRPC frontend address: %w", err))
	}
	return net.JoinHostPort(host, strconv.Itoa(frontendHTTPPort))
}

func (c *TemporalImpl) HistoryServiceAddresses() []string {
	return c.makeGRPCAddresses(c.historyConfig.NumHistoryHosts, historyPort)
}

func (c *TemporalImpl) MatchingServiceAddresses() []string {
	return c.makeGRPCAddresses(c.matchingConfig.NumMatchingHosts, matchingPort)
}

func (c *TemporalImpl) WorkerServiceAddresses() []string {
	return c.makeGRPCAddresses(c.workerConfig.NumWorkers, workerPort)
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

func (c *TemporalImpl) FrontendNamespaceRegistries() []namespace.Registry {
	return c.frontendNamespaceRegistries
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

	for _, host := range c.hostsByService[serviceName].All {
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
			fx.Provide(func() httpPort { return httpPort(frontendHTTPPort) }),
			fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
			fx.Provide(func() log.Logger { return logger }),
			fx.Provide(func() log.ThrottledLogger { return logger }),
			fx.Provide(func() resource.NamespaceLogger { return logger }),
			fx.Provide(c.newRPCFactory),
			static.MembershipModule(c.makeHostMap(serviceName, host)),
			fx.Provide(func() *cluster.Config { return c.clusterMetadataConfig }),
			fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
			fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
			fx.Provide(sdkClientFactoryProvider),
			fx.Provide(c.GetMetricsHandler),
			fx.Provide(func() []grpc.UnaryServerInterceptor { return nil }),
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
			fx.Provide(resource.DefaultSnTaggedLoggerProvider),
			fx.Provide(func() *esclient.Config { return c.esConfig }),
			fx.Provide(func() esclient.Client { return c.esClient }),
			fx.Provide(c.GetTLSConfigProvider),
			fx.Provide(c.GetTaskCategoryRegistry),
			fx.Supply(c.spanExporters),
			temporal.ServiceTracingModule,
			frontend.Module,
			fx.Populate(&namespaceRegistry, &rpcFactory, &historyRawClient, &matchingRawClient, &grpcResolver),
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.FrontendService),
		)
		err := app.Err()
		if err != nil {
			logger.Fatal("unable to construct frontend service", tag.Error(err))
		}

		c.fxApps = append(c.fxApps, app)
		c.frontendNamespaceRegistries = append(c.frontendNamespaceRegistries, namespaceRegistry)

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

	for _, host := range c.hostsByService[serviceName].All {
		logger := log.With(c.logger, tag.Host(host))
		app := fx.New(
			fx.Supply(
				c.copyPersistenceConfig(),
				serviceName,
				c.mockAdminClient,
			),
			fx.Provide(c.GetMetricsHandler),
			fx.Provide(func() listenHostPort { return listenHostPort(host) }),
			fx.Provide(func() httpPort { return httpPort(frontendHTTPPort) }),
			fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
			fx.Provide(func() log.Logger { return logger }),
			fx.Provide(func() log.ThrottledLogger { return logger }),
			fx.Provide(c.newRPCFactory),
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
			fx.Provide(resource.DefaultSnTaggedLoggerProvider),
			fx.Provide(func() *esclient.Config { return c.esConfig }),
			fx.Provide(func() esclient.Client { return c.esClient }),
			fx.Provide(c.GetTLSConfigProvider),
			fx.Provide(c.GetTaskCategoryRegistry),
			fx.Supply(c.spanExporters),
			temporal.ServiceTracingModule,

			history.QueueModule,
			history.Module,
			replication.Module,
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.HistoryService),
		)
		err := app.Err()
		if err != nil {
			logger.Fatal("unable to construct history service", tag.Error(err))
		}
		c.fxApps = append(c.fxApps, app)
		if err := app.Start(context.Background()); err != nil {
			logger.Fatal("unable to start history service", tag.Error(err))
		}
	}
}

func (c *TemporalImpl) startMatching() {
	serviceName := primitives.MatchingService

	for _, host := range c.hostsByService[serviceName].All {
		logger := log.With(c.logger, tag.Host(host))
		app := fx.New(
			fx.Supply(
				c.copyPersistenceConfig(),
				serviceName,
				c.mockAdminClient,
			),
			fx.Provide(c.GetMetricsHandler),
			fx.Provide(func() listenHostPort { return listenHostPort(host) }),
			fx.Provide(func() httpPort { return httpPort(frontendHTTPPort) }),
			fx.Provide(func() log.Logger { return logger }),
			fx.Provide(func() log.ThrottledLogger { return logger }),
			fx.Provide(c.newRPCFactory),
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
			fx.Provide(func() *esclient.Config { return c.esConfig }),
			fx.Provide(func() esclient.Client { return c.esClient }),
			fx.Provide(c.GetTLSConfigProvider),
			fx.Provide(resource.DefaultSnTaggedLoggerProvider),
			fx.Provide(c.GetTaskCategoryRegistry),
			fx.Supply(c.spanExporters),
			temporal.ServiceTracingModule,
			matching.Module,
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.MatchingService),
		)
		err := app.Err()
		if err != nil {
			logger.Fatal("unable to start matching service", tag.Error(err))
		}
		c.fxApps = append(c.fxApps, app)
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
	if c.workerConfig.EnableReplicator {
		clusterConfigCopy.EnableGlobalNamespace = true
	}

	for _, host := range c.hostsByService[serviceName].All {
		logger := log.With(c.logger, tag.Host(host))
		app := fx.New(
			fx.Supply(
				c.copyPersistenceConfig(),
				serviceName,
				c.mockAdminClient,
			),
			fx.Provide(c.GetMetricsHandler),
			fx.Provide(func() listenHostPort { return listenHostPort(host) }),
			fx.Provide(func() httpPort { return httpPort(frontendHTTPPort) }),
			fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
			fx.Provide(func() log.Logger { return logger }),
			fx.Provide(func() log.ThrottledLogger { return logger }),
			fx.Provide(c.newRPCFactory),
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
			fx.Provide(resource.DefaultSnTaggedLoggerProvider),
			fx.Provide(func() esclient.Client { return c.esClient }),
			fx.Provide(func() *esclient.Config { return c.esConfig }),
			fx.Provide(c.GetTLSConfigProvider),
			fx.Provide(c.GetTaskCategoryRegistry),
			fx.Supply(c.spanExporters),
			temporal.ServiceTracingModule,
			worker.Module,
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.WorkerService),
		)
		err := app.Err()
		if err != nil {
			logger.Fatal("unable to start worker service", tag.Error(err))
		}

		c.fxApps = append(c.fxApps, app)
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
	return c.executionManager
}

func (c *TemporalImpl) GetTLSConfigProvider() encryption.TLSConfigProvider {
	// If we just return this directly, the interface will be non-nil but the
	// pointer will be nil
	if c.tlsConfigProvider != nil {
		return c.tlsConfigProvider
	}
	return nil
}

func (c *TemporalImpl) GetTaskCategoryRegistry() tasks.TaskCategoryRegistry {
	return c.taskCategoryRegistry
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
					HTTPPort: frontendHTTPPort,
					HTTPAdditionalForwardedHeaders: []string{
						"this-header-forwarded",
						"this-header-prefix-forwarded-*",
					},
				},
			},
		},
	}
}

func (c *TemporalImpl) overrideHistoryDynamicConfig(t *testing.T) {
	if c.esConfig != nil {
		c.overrideDynamicConfig(t, dynamicconfig.SecondaryVisibilityWritingMode.Key(), visibility.SecondaryVisibilityWritingModeDual)
	}
	if c.historyConfig.HistoryCountLimitWarn != 0 {
		c.overrideDynamicConfig(t, dynamicconfig.HistoryCountLimitWarn.Key(), c.historyConfig.HistoryCountLimitWarn)
	}
	if c.historyConfig.HistoryCountLimitError != 0 {
		c.overrideDynamicConfig(t, dynamicconfig.HistoryCountLimitError.Key(), c.historyConfig.HistoryCountLimitError)
	}
	if c.historyConfig.HistorySizeLimitWarn != 0 {
		c.overrideDynamicConfig(t, dynamicconfig.HistorySizeLimitWarn.Key(), c.historyConfig.HistorySizeLimitWarn)
	}
	if c.historyConfig.HistorySizeLimitError != 0 {
		c.overrideDynamicConfig(t, dynamicconfig.HistorySizeLimitError.Key(), c.historyConfig.HistorySizeLimitError)
	}
	if c.historyConfig.BlobSizeLimitError != 0 {
		c.overrideDynamicConfig(t, dynamicconfig.BlobSizeLimitError.Key(), c.historyConfig.BlobSizeLimitError)
	}
	if c.historyConfig.BlobSizeLimitWarn != 0 {
		c.overrideDynamicConfig(t, dynamicconfig.BlobSizeLimitWarn.Key(), c.historyConfig.BlobSizeLimitWarn)
	}
	if c.historyConfig.MutableStateSizeLimitError != 0 {
		c.overrideDynamicConfig(t, dynamicconfig.MutableStateSizeLimitError.Key(), c.historyConfig.MutableStateSizeLimitError)
	}
	if c.historyConfig.MutableStateSizeLimitWarn != 0 {
		c.overrideDynamicConfig(t, dynamicconfig.MutableStateSizeLimitWarn.Key(), c.historyConfig.MutableStateSizeLimitWarn)
	}

	// For DeleteWorkflowExecution tests
	c.overrideDynamicConfig(t, dynamicconfig.TransferProcessorUpdateAckInterval.Key(), 1*time.Second)
	c.overrideDynamicConfig(t, dynamicconfig.VisibilityProcessorUpdateAckInterval.Key(), 1*time.Second)
}

func (c *TemporalImpl) newRPCFactory(
	sn primitives.ServiceName,
	grpcHostPort listenHostPort,
	logger log.Logger,
	grpcResolver *membership.GRPCResolver,
	tlsConfigProvider encryption.TLSConfigProvider,
	monitor membership.Monitor,
	httpPort httpPort,
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
	return rpc.NewFactory(
		&config.RPC{BindOnIP: host, GRPCPort: port, HTTPPort: int(httpPort)},
		sn,
		logger,
		tlsConfigProvider,
		grpcResolver.MakeURL(primitives.FrontendService),
		grpcResolver.MakeURL(primitives.FrontendService),
		int(httpPort),
		frontendTLSConfig,
		nil,
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
	numberOfHistoryShards int32,
	logger log.Logger,
	throttledLogger log.Logger,
) client.Factory {
	f := client.NewFactoryProvider().NewFactory(
		rpcFactory,
		monitor,
		metricsHandler,
		dc,
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
