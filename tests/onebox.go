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

package tests

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
	"go.uber.org/multierr"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/membership/static"

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
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/service/worker"
	"go.temporal.io/server/temporal"
)

type (
	temporalImpl struct {
		frontendService *frontend.Service
		matchingService *matching.Service
		historyServices []*history.Service
		workerService   *worker.Service

		frontendApp *fx.App
		matchingApp *fx.App
		historyApps []*fx.App
		workerApp   *fx.App

		matchingNamespaceRegistry  namespace.Registry
		frontendNamespaceRegistry  namespace.Registry
		historyNamespaceRegistries []namespace.Registry
		workerNamespaceRegistry    namespace.Registry

		adminClient                      adminservice.AdminServiceClient
		frontendClient                   workflowservice.WorkflowServiceClient
		operatorClient                   operatorservice.OperatorServiceClient
		historyClient                    historyservice.HistoryServiceClient
		matchingClient                   matchingservice.MatchingServiceClient
		dcClient                         *dcClient
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
		shutdownCh                       chan struct{}
		shutdownWG                       sync.WaitGroup
		clusterNo                        int // cluster number
		archiverMetadata                 carchiver.ArchivalMetadata
		archiverProvider                 provider.ArchiverProvider
		historyConfig                    *HistoryConfig
		esConfig                         *esclient.Config
		esClient                         esclient.Client
		workerConfig                     *WorkerConfig
		mockAdminClient                  map[string]adminservice.AdminServiceClient
		namespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor
		spanExporters                    []otelsdktrace.SpanExporter
		tlsConfigProvider                *encryption.FixedTLSConfigProvider
		captureMetricsHandler            *metricstest.CaptureHandler

		onGetClaims          func(*authorization.AuthInfo) (*authorization.Claims, error)
		onAuthorize          func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error)
		callbackLock         sync.RWMutex // Must be used for above callbacks
		serviceFxOptions     map[primitives.ServiceName][]fx.Option
		taskCategoryRegistry tasks.TaskCategoryRegistry
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
		HistoryConfig                    *HistoryConfig
		ESConfig                         *esclient.Config
		ESClient                         esclient.Client
		WorkerConfig                     *WorkerConfig
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

// newTemporal returns an instance that hosts full temporal in one process
func newTemporal(t *testing.T, params *TemporalParams) *temporalImpl {
	testDCClient := newTestDCClient(dynamicconfig.NewNoopClient())
	for k, v := range params.DynamicConfigOverrides {
		testDCClient.OverrideValueByKey(t, k, v)
	}
	impl := &temporalImpl{
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
		shutdownCh:                       make(chan struct{}),
		clusterNo:                        params.ClusterNo,
		esConfig:                         params.ESConfig,
		esClient:                         params.ESClient,
		archiverMetadata:                 params.ArchiverMetadata,
		archiverProvider:                 params.ArchiverProvider,
		historyConfig:                    params.HistoryConfig,
		workerConfig:                     params.WorkerConfig,
		mockAdminClient:                  params.MockAdminClient,
		namespaceReplicationTaskExecutor: params.NamespaceReplicationTaskExecutor,
		spanExporters:                    params.SpanExporters,
		tlsConfigProvider:                params.TLSConfigProvider,
		captureMetricsHandler:            params.CaptureMetricsHandler,
		dcClient:                         testDCClient,
		serviceFxOptions:                 params.ServiceFxOptions,
		taskCategoryRegistry:             params.TaskCategoryRegistry,
	}
	impl.overrideHistoryDynamicConfig(t, testDCClient)
	return impl
}

func (c *temporalImpl) enableWorker() bool {
	return c.workerConfig.StartWorkerAnyway || c.workerConfig.EnableArchiver || c.workerConfig.EnableReplicator
}

func (c *temporalImpl) Start() error {
	hosts := make(map[primitives.ServiceName]static.Hosts)
	hosts[primitives.FrontendService] = static.SingleLocalHost(c.FrontendGRPCAddress())
	hosts[primitives.MatchingService] = static.SingleLocalHost(c.MatchingGRPCServiceAddress())
	hosts[primitives.HistoryService] = static.Hosts{All: c.HistoryServiceAddresses()}
	if c.enableWorker() {
		hosts[primitives.WorkerService] = static.SingleLocalHost(c.WorkerGRPCServiceAddress())
	}

	// create temporal-system namespace, this must be created before starting
	// the services - so directly use the metadataManager to create this
	if err := c.createSystemNamespace(); err != nil {
		return err
	}

	var startWG sync.WaitGroup
	startWG.Add(2)
	go c.startHistory(hosts, &startWG)
	go c.startMatching(hosts, &startWG)
	startWG.Wait()

	startWG.Add(1)
	go c.startFrontend(hosts, &startWG)
	startWG.Wait()

	if c.enableWorker() {
		startWG.Add(1)
		go c.startWorker(hosts, &startWG)
		startWG.Wait()
	}

	return nil
}

func (c *temporalImpl) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var errs error

	if c.enableWorker() {
		c.shutdownWG.Add(1)
		errs = multierr.Combine(errs, c.workerApp.Stop(ctx))
	}

	c.shutdownWG.Add(3)

	if err := c.frontendApp.Stop(ctx); err != nil {
		return err
	}
	for _, historyApp := range c.historyApps {
		errs = multierr.Combine(errs, historyApp.Stop(ctx))
	}

	errs = multierr.Combine(errs, c.matchingApp.Stop(ctx))

	close(c.shutdownCh)
	c.shutdownWG.Wait()

	return errs
}

func (c *temporalImpl) FrontendGRPCAddress() string {
	switch c.clusterNo {
	case 0:
		return "127.0.0.1:7134"
	case 1:
		return "127.0.0.1:8134"
	case 2:
		return "127.0.0.1:9134"
	case 3:
		return "127.0.0.1:10134"
	default:
		return "127.0.0.1:7134"
	}
}

func (c *temporalImpl) FrontendHTTPAddress() string {
	host, port := c.FrontendHTTPHostPort()
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func (c *temporalImpl) FrontendHTTPHostPort() (string, int) {
	if host, port, err := net.SplitHostPort(c.FrontendGRPCAddress()); err != nil {
		panic(fmt.Errorf("Invalid gRPC frontend address: %w", err))
	} else if portNum, err := strconv.Atoi(port); err != nil {
		panic(fmt.Errorf("Invalid gRPC frontend port: %w", err))
	} else {
		return host, portNum + 10
	}
}

func (c *temporalImpl) HistoryServiceAddresses() []string {
	var hosts []string
	var startPort int
	switch c.clusterNo {
	case 0:
		startPort = 7231
	case 1:
		startPort = 8231
	case 2:
		startPort = 9231
	case 3:
		startPort = 10231
	default:
		startPort = 7231
	}
	for i := 0; i < c.historyConfig.NumHistoryHosts; i++ {
		port := startPort + i
		hosts = append(hosts, fmt.Sprintf("127.0.0.1:%v", port))
	}

	c.logger.Info("History hosts", tag.Addresses(hosts))
	return hosts
}

func (c *temporalImpl) MatchingGRPCServiceAddress() string {
	switch c.clusterNo {
	case 0:
		return "127.0.0.1:7136"
	case 1:
		return "127.0.0.1:8136"
	case 2:
		return "127.0.0.1:9136"
	case 3:
		return "127.0.0.1:10136"
	default:
		return "127.0.0.1:7136"
	}
}

func (c *temporalImpl) WorkerGRPCServiceAddress() string {
	switch c.clusterNo {
	case 0:
		return "127.0.0.1:7138"
	case 1:
		return "127.0.0.1:8138"
	case 2:
		return "127.0.0.1:9138"
	case 3:
		return "127.0.0.1:10138"
	default:
		return "127.0.0.1:7138"
	}
}

func (c *temporalImpl) OverrideDCValue(t *testing.T, setting dynamicconfig.GenericSetting, value any) {
	c.dcClient.OverrideValue(t, setting, value)
}

func (c *temporalImpl) GetAdminClient() adminservice.AdminServiceClient {
	return c.adminClient
}

func (c *temporalImpl) GetOperatorClient() operatorservice.OperatorServiceClient {
	return c.operatorClient
}

func (c *temporalImpl) GetFrontendClient() workflowservice.WorkflowServiceClient {
	return c.frontendClient
}

func (c *temporalImpl) GetHistoryClient() historyservice.HistoryServiceClient {
	return c.historyClient
}

func (c *temporalImpl) GetMatchingClient() matchingservice.MatchingServiceClient {
	// Note that this matching client does not do routing. But it doesn't matter since we have
	// only one matching node in testing.
	return c.matchingClient
}

func (c *temporalImpl) GetFrontendNamespaceRegistry() namespace.Registry {
	return c.frontendNamespaceRegistry
}

func (c *temporalImpl) startFrontend(
	hostsByService map[primitives.ServiceName]static.Hosts,
	startWG *sync.WaitGroup,
) {
	serviceName := primitives.FrontendService
	persistenceConfig, err := copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for history", tag.Error(err))
	}
	if c.esConfig != nil {
		esDataStoreName := "es-visibility"
		persistenceConfig.VisibilityStore = esDataStoreName
		persistenceConfig.DataStores[esDataStoreName] = config.DataStore{
			Elasticsearch: c.esConfig,
		}
	}

	var frontendService *frontend.Service
	var clientBean client.Bean
	var namespaceRegistry namespace.Registry
	var rpcFactory common.RPCFactory
	feApp := fx.New(
		fx.Supply(
			persistenceConfig,
			serviceName,
		),
		fx.Provide(c.frontendConfigProvider),
		fx.Provide(func() listenHostPort { return listenHostPort(c.FrontendGRPCAddress()) }),
		fx.Provide(func() httpPort {
			_, port := c.FrontendHTTPHostPort()
			return httpPort(port)
		}),
		fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
		fx.Provide(func() log.ThrottledLogger { return c.logger }),
		fx.Provide(func() resource.NamespaceLogger { return c.logger }),
		fx.Provide(c.newRPCFactory),
		static.MembershipModule(hostsByService),
		fx.Provide(func() *cluster.Config { return c.clusterMetadataConfig }),
		fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
		fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
		fx.Provide(sdkClientFactoryProvider),
		fx.Provide(c.GetMetricsHandler),
		fx.Provide(func() []grpc.UnaryServerInterceptor { return nil }),
		fx.Provide(func() authorization.Authorizer { return c }),
		fx.Provide(func() authorization.ClaimMapper { return c }),
		fx.Provide(func() authorization.JWTAudienceMapper { return nil }),
		fx.Provide(func() client.FactoryProvider { return client.NewFactoryProvider() }),
		fx.Provide(func() searchattribute.Mapper { return nil }),
		// Comment the line above and uncomment the line below to test with search attributes mapper.
		// fx.Provide(func() searchattribute.Mapper { return NewSearchAttributeTestMapper() }),
		fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
		fx.Provide(persistenceClient.FactoryProvider),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return c.abstractDataStoreFactory }),
		fx.Provide(func() visibility.VisibilityStoreFactory { return c.visibilityStoreFactory }),
		fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
		fx.Provide(func() log.Logger { return c.logger }),
		fx.Provide(resource.DefaultSnTaggedLoggerProvider),
		fx.Provide(func() *esclient.Config { return c.esConfig }),
		fx.Provide(func() esclient.Client { return c.esClient }),
		fx.Provide(c.GetTLSConfigProvider),
		fx.Provide(c.GetTaskCategoryRegistry),
		fx.Supply(c.spanExporters),
		temporal.ServiceTracingModule,
		frontend.Module,
		fx.Populate(&frontendService, &clientBean, &namespaceRegistry, &rpcFactory),
		temporal.FxLogAdapter,
		c.getFxOptionsForService(primitives.FrontendService),
	)
	err = feApp.Err()
	if err != nil {
		c.logger.Fatal("unable to construct frontend service", tag.Error(err))
	}

	if c.mockAdminClient != nil {
		if clientBean != nil {
			for serviceName, client := range c.mockAdminClient {
				clientBean.SetRemoteAdminClient(serviceName, client)
			}
		}
	}

	c.frontendApp = feApp
	c.frontendService = frontendService
	c.frontendNamespaceRegistry = namespaceRegistry
	connection := rpcFactory.CreateLocalFrontendGRPCConnection()
	c.frontendClient = NewFrontendClient(connection)
	c.adminClient = NewAdminClient(connection)
	c.operatorClient = operatorservice.NewOperatorServiceClient(connection)

	if err := feApp.Start(context.Background()); err != nil {
		c.logger.Fatal("unable to start frontend service", tag.Error(err))
	}

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *temporalImpl) startHistory(
	hostsByService map[primitives.ServiceName]static.Hosts,
	startWG *sync.WaitGroup,
) {
	serviceName := primitives.HistoryService
	allHosts := hostsByService[serviceName].All
	for _, host := range allHosts {
		hostMap := maps.Clone(hostsByService)
		historyHosts := hostMap[serviceName]
		historyHosts.Self = host
		hostMap[serviceName] = historyHosts

		persistenceConfig, err := copyPersistenceConfig(c.persistenceConfig)
		if err != nil {
			c.logger.Fatal("Failed to copy persistence config for history", tag.Error(err))
		}
		if c.esConfig != nil {
			esDataStoreName := "es-visibility"
			persistenceConfig.VisibilityStore = esDataStoreName
			persistenceConfig.DataStores[esDataStoreName] = config.DataStore{
				Elasticsearch: c.esConfig,
			}
		}

		var historyService *history.Service
		var clientBean client.Bean
		var namespaceRegistry namespace.Registry
		app := fx.New(
			fx.Supply(
				persistenceConfig,
				serviceName,
			),
			fx.Provide(c.GetMetricsHandler),
			fx.Provide(func() listenHostPort { return listenHostPort(host) }),
			fx.Provide(func() httpPort {
				_, port := c.FrontendHTTPHostPort()
				return httpPort(port)
			}),
			fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
			fx.Provide(func() log.ThrottledLogger { return c.logger }),
			fx.Provide(c.newRPCFactory),
			static.MembershipModule(hostMap),
			fx.Provide(func() *cluster.Config { return c.clusterMetadataConfig }),
			fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
			fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
			fx.Provide(sdkClientFactoryProvider),
			fx.Provide(func() client.FactoryProvider { return client.NewFactoryProvider() }),
			fx.Provide(func() searchattribute.Mapper { return nil }),
			// Comment the line above and uncomment the line below to test with search attributes mapper.
			// fx.Provide(func() searchattribute.Mapper { return NewSearchAttributeTestMapper() }),
			fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
			fx.Provide(persistenceClient.FactoryProvider),
			fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return c.abstractDataStoreFactory }),
			fx.Provide(func() visibility.VisibilityStoreFactory { return c.visibilityStoreFactory }),
			fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
			fx.Provide(func() log.Logger { return c.logger }),
			fx.Provide(resource.DefaultSnTaggedLoggerProvider),
			fx.Provide(func() *esclient.Config { return c.esConfig }),
			fx.Provide(func() esclient.Client { return c.esClient }),
			fx.Provide(workflow.NewTaskGeneratorProvider),
			fx.Provide(c.GetTLSConfigProvider),
			fx.Provide(c.GetTaskCategoryRegistry),
			fx.Supply(c.spanExporters),
			temporal.ServiceTracingModule,

			history.QueueModule,
			history.Module,
			replication.Module,
			fx.Populate(&historyService, &clientBean, &namespaceRegistry),
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.HistoryService),
		)
		err = app.Err()
		if err != nil {
			c.logger.Fatal("unable to construct history service", tag.Error(err))
		}

		if c.mockAdminClient != nil {
			if clientBean != nil {
				for serviceName, client := range c.mockAdminClient {
					clientBean.SetRemoteAdminClient(serviceName, client)
				}
			}
		}

		// TODO: this is not correct when there are multiple history hosts as later client will overwrite previous ones.
		// However current interface for getting history client doesn't specify which client it needs and the tests that use this API
		// depends on the fact that there's only one history host.
		// Need to change those tests and modify the interface for getting history client.
		historyConnection, err := rpc.Dial(allHosts[0], nil, c.logger)
		if err != nil {
			c.logger.Fatal("Failed to create connection for history", tag.Error(err))
		}

		c.historyApps = append(c.historyApps, app)
		c.historyClient = NewHistoryClient(historyConnection)
		c.historyServices = append(c.historyServices, historyService)
		c.historyNamespaceRegistries = append(c.historyNamespaceRegistries, namespaceRegistry)

		if err := app.Start(context.Background()); err != nil {
			c.logger.Fatal("unable to start history service", tag.Error(err))
		}
	}

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *temporalImpl) startMatching(
	hostsByService map[primitives.ServiceName]static.Hosts,
	startWG *sync.WaitGroup,
) {
	serviceName := primitives.MatchingService

	persistenceConfig, err := copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for matching", tag.Error(err))
	}
	if c.esConfig != nil {
		esDataStoreName := "es-visibility"
		persistenceConfig.VisibilityStore = esDataStoreName
		persistenceConfig.DataStores[esDataStoreName] = config.DataStore{
			Elasticsearch: c.esConfig,
		}
	}

	var matchingService *matching.Service
	var clientBean client.Bean
	var namespaceRegistry namespace.Registry
	app := fx.New(
		fx.Supply(
			persistenceConfig,
			serviceName,
		),
		fx.Provide(c.GetMetricsHandler),
		fx.Provide(func() listenHostPort { return listenHostPort(c.MatchingGRPCServiceAddress()) }),
		fx.Provide(func() httpPort {
			_, port := c.FrontendHTTPHostPort()
			return httpPort(port)
		}),
		fx.Provide(func() log.ThrottledLogger { return c.logger }),
		fx.Provide(c.newRPCFactory),
		static.MembershipModule(hostsByService),
		fx.Provide(func() *cluster.Config { return c.clusterMetadataConfig }),
		fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
		fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
		fx.Provide(func() client.FactoryProvider { return client.NewFactoryProvider() }),
		fx.Provide(func() searchattribute.Mapper { return nil }),
		fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
		fx.Provide(persistenceClient.FactoryProvider),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return c.abstractDataStoreFactory }),
		fx.Provide(func() visibility.VisibilityStoreFactory { return c.visibilityStoreFactory }),
		fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
		fx.Provide(func() *esclient.Config { return c.esConfig }),
		fx.Provide(func() esclient.Client { return c.esClient }),
		fx.Provide(c.GetTLSConfigProvider),
		fx.Provide(func() log.Logger { return c.logger }),
		fx.Provide(resource.DefaultSnTaggedLoggerProvider),
		fx.Provide(c.GetTaskCategoryRegistry),
		fx.Supply(c.spanExporters),
		temporal.ServiceTracingModule,
		matching.Module,
		fx.Populate(&matchingService, &clientBean, &namespaceRegistry),
		temporal.FxLogAdapter,
		c.getFxOptionsForService(primitives.MatchingService),
	)
	err = app.Err()
	if err != nil {
		c.logger.Fatal("unable to start matching service", tag.Error(err))
	}
	if c.mockAdminClient != nil {
		if clientBean != nil {
			for serviceName, client := range c.mockAdminClient {
				clientBean.SetRemoteAdminClient(serviceName, client)
			}
		}
	}

	matchingConnection, err := rpc.Dial(c.MatchingGRPCServiceAddress(), nil, c.logger)
	if err != nil {
		c.logger.Fatal("Failed to create connection for matching", tag.Error(err))
	}
	c.matchingClient = matchingservice.NewMatchingServiceClient(matchingConnection)
	c.matchingApp = app
	c.matchingService = matchingService
	c.matchingNamespaceRegistry = namespaceRegistry
	if err := app.Start(context.Background()); err != nil {
		c.logger.Fatal("unable to start matching service", tag.Error(err))
	}

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *temporalImpl) startWorker(
	hostsByService map[primitives.ServiceName]static.Hosts,
	startWG *sync.WaitGroup,
) {
	serviceName := primitives.WorkerService

	persistenceConfig, err := copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for history", tag.Error(err))
	}
	if c.esConfig != nil {
		esDataStoreName := "es-visibility"
		persistenceConfig.VisibilityStore = esDataStoreName
		persistenceConfig.DataStores[esDataStoreName] = config.DataStore{
			Elasticsearch: c.esConfig,
		}
	}

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

	var workerService *worker.Service
	var clientBean client.Bean
	var namespaceRegistry namespace.Registry
	app := fx.New(
		fx.Supply(
			persistenceConfig,
			serviceName,
		),
		fx.Provide(c.GetMetricsHandler),
		fx.Provide(func() listenHostPort { return listenHostPort(c.WorkerGRPCServiceAddress()) }),
		fx.Provide(func() httpPort {
			_, port := c.FrontendHTTPHostPort()
			return httpPort(port)
		}),
		fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
		fx.Provide(func() log.ThrottledLogger { return c.logger }),
		fx.Provide(c.newRPCFactory),
		static.MembershipModule(hostsByService),
		fx.Provide(func() *cluster.Config { return &clusterConfigCopy }),
		fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
		fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
		fx.Provide(sdkClientFactoryProvider),
		fx.Provide(func() client.FactoryProvider { return client.NewFactoryProvider() }),
		fx.Provide(func() searchattribute.Mapper { return nil }),
		fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
		fx.Provide(persistenceClient.FactoryProvider),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return c.abstractDataStoreFactory }),
		fx.Provide(func() visibility.VisibilityStoreFactory { return c.visibilityStoreFactory }),
		fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
		fx.Provide(func() log.Logger { return c.logger }),
		fx.Provide(resource.DefaultSnTaggedLoggerProvider),
		fx.Provide(func() esclient.Client { return c.esClient }),
		fx.Provide(func() *esclient.Config { return c.esConfig }),
		fx.Provide(c.GetTLSConfigProvider),
		fx.Provide(c.GetTaskCategoryRegistry),
		fx.Supply(c.spanExporters),
		temporal.ServiceTracingModule,
		worker.Module,
		fx.Populate(&workerService, &clientBean, &namespaceRegistry),
		temporal.FxLogAdapter,
		c.getFxOptionsForService(primitives.WorkerService),
	)
	err = app.Err()
	if err != nil {
		c.logger.Fatal("unable to start worker service", tag.Error(err))
	}

	c.workerApp = app
	c.workerService = workerService
	c.workerNamespaceRegistry = namespaceRegistry
	if err := app.Start(context.Background()); err != nil {
		c.logger.Fatal("unable to start worker service", tag.Error(err))
	}

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *temporalImpl) getFxOptionsForService(serviceName primitives.ServiceName) fx.Option {
	return fx.Options(c.serviceFxOptions[serviceName]...)
}

func (c *temporalImpl) createSystemNamespace() error {
	err := c.metadataMgr.InitializeSystemNamespaces(context.Background(), c.clusterMetadataConfig.CurrentClusterName)
	if err != nil {
		return fmt.Errorf("failed to create temporal-system namespace: %v", err)
	}
	return nil
}

func (c *temporalImpl) GetExecutionManager() persistence.ExecutionManager {
	return c.executionManager
}

func (c *temporalImpl) GetTLSConfigProvider() encryption.TLSConfigProvider {
	// If we just return this directly, the interface will be non-nil but the
	// pointer will be nil
	if c.tlsConfigProvider != nil {
		return c.tlsConfigProvider
	}
	return nil
}

func (c *temporalImpl) GetTaskCategoryRegistry() tasks.TaskCategoryRegistry {
	return c.taskCategoryRegistry
}

func (c *temporalImpl) GetMetricsHandler() metrics.Handler {
	if c.captureMetricsHandler != nil {
		return c.captureMetricsHandler
	}
	return metrics.NoopMetricsHandler
}

func (c *temporalImpl) frontendConfigProvider() *config.Config {
	// Set HTTP port and a test HTTP forwarded header
	_, httpPort := c.FrontendHTTPHostPort()
	return &config.Config{
		Services: map[string]config.Service{
			string(primitives.FrontendService): {
				RPC: config.RPC{
					HTTPPort: httpPort,
					HTTPAdditionalForwardedHeaders: []string{
						"this-header-forwarded",
						"this-header-prefix-forwarded-*",
					},
				},
			},
		},
	}
}

func (c *temporalImpl) overrideHistoryDynamicConfig(t *testing.T, client *dcClient) {
	if c.esConfig != nil {
		client.OverrideValue(t, dynamicconfig.SecondaryVisibilityWritingMode, visibility.SecondaryVisibilityWritingModeDual)
	}
	if c.historyConfig.HistoryCountLimitWarn != 0 {
		client.OverrideValue(t, dynamicconfig.HistoryCountLimitWarn, c.historyConfig.HistoryCountLimitWarn)
	}
	if c.historyConfig.HistoryCountLimitError != 0 {
		client.OverrideValue(t, dynamicconfig.HistoryCountLimitError, c.historyConfig.HistoryCountLimitError)
	}
	if c.historyConfig.HistorySizeLimitWarn != 0 {
		client.OverrideValue(t, dynamicconfig.HistorySizeLimitWarn, c.historyConfig.HistorySizeLimitWarn)
	}
	if c.historyConfig.HistorySizeLimitError != 0 {
		client.OverrideValue(t, dynamicconfig.HistorySizeLimitError, c.historyConfig.HistorySizeLimitError)
	}
	if c.historyConfig.BlobSizeLimitError != 0 {
		client.OverrideValue(t, dynamicconfig.BlobSizeLimitError, c.historyConfig.BlobSizeLimitError)
	}
	if c.historyConfig.BlobSizeLimitWarn != 0 {
		client.OverrideValue(t, dynamicconfig.BlobSizeLimitWarn, c.historyConfig.BlobSizeLimitWarn)
	}
	if c.historyConfig.MutableStateSizeLimitError != 0 {
		client.OverrideValue(t, dynamicconfig.MutableStateSizeLimitError, c.historyConfig.MutableStateSizeLimitError)
	}
	if c.historyConfig.MutableStateSizeLimitWarn != 0 {
		client.OverrideValue(t, dynamicconfig.MutableStateSizeLimitWarn, c.historyConfig.MutableStateSizeLimitWarn)
	}

	// For DeleteWorkflowExecution tests
	client.OverrideValue(t, dynamicconfig.TransferProcessorUpdateAckInterval, 1*time.Second)
	client.OverrideValue(t, dynamicconfig.VisibilityProcessorUpdateAckInterval, 1*time.Second)
}

func (c *temporalImpl) newRPCFactory(
	sn primitives.ServiceName,
	grpcHostPort listenHostPort,
	logger log.Logger,
	grpcResolver membership.GRPCResolver,
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
		membership.MakeResolverURL(primitives.FrontendService),
		membership.MakeResolverURL(primitives.FrontendService),
		int(httpPort),
		frontendTLSConfig,
		nil,
		monitor,
	), nil
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

// copyPersistenceConfig makes a deepcopy of persistence config.
// This is just a temp fix for the race condition of persistence config.
// The race condition happens because all the services are using the same datastore map in the config.
// Also all services will retry to modify the maxQPS field in the datastore during start up and use the modified maxQPS value to create a persistence factory.
func copyPersistenceConfig(pConfig config.Persistence) (config.Persistence, error) {
	copiedDataStores := make(map[string]config.DataStore)
	for name, value := range pConfig.DataStores {
		copiedDataStore := config.DataStore{}
		encodedDataStore, err := json.Marshal(value)
		if err != nil {
			return pConfig, err
		}

		if err = json.Unmarshal(encodedDataStore, &copiedDataStore); err != nil {
			return pConfig, err
		}
		copiedDataStores[name] = copiedDataStore
	}
	pConfig.DataStores = copiedDataStores
	return pConfig, nil
}

func sdkClientFactoryProvider(
	resolver membership.GRPCResolver,
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
		membership.MakeResolverURL(primitives.FrontendService),
		tlsConfig,
		metricsHandler,
		logger,
		dynamicconfig.WorkerStickyCacheSize.Get(dc),
	)
}
