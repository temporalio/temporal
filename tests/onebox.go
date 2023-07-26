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
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/fx"
	"go.uber.org/multierr"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"

	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"

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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/visibility"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/service/history"
	"go.temporal.io/server/service/history/replication"
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
	}

	// HistoryConfig contains configs for history service
	HistoryConfig struct {
		NumHistoryShards           int32
		NumHistoryHosts            int
		HistoryCountLimitError     int
		HistoryCountLimitWarn      int
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
	}

	listenHostPort string
)

// newTemporal returns an instance that hosts full temporal in one process
func newTemporal(params *TemporalParams) *temporalImpl {
	testDCClient := newTestDCClient(dynamicconfig.NewNoopClient())
	for k, v := range params.DynamicConfigOverrides {
		testDCClient.OverrideValue(k, v)
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
		dcClient:                         testDCClient,
	}
	impl.overrideHistoryDynamicConfig(testDCClient)
	return impl
}

func (c *temporalImpl) enableWorker() bool {
	return c.workerConfig.StartWorkerAnyway || c.workerConfig.EnableArchiver || c.workerConfig.EnableReplicator
}

func (c *temporalImpl) Start() error {
	hosts := make(map[primitives.ServiceName][]string)
	hosts[primitives.FrontendService] = []string{c.FrontendGRPCAddress()}
	hosts[primitives.MatchingService] = []string{c.MatchingGRPCServiceAddress()}
	hosts[primitives.HistoryService] = c.HistoryServiceAddress()
	if c.enableWorker() {
		hosts[primitives.WorkerService] = []string{c.WorkerGRPCServiceAddress()}
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

func (c *temporalImpl) HistoryServiceAddress() []string {
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

func (c *temporalImpl) startFrontend(hosts map[primitives.ServiceName][]string, startWG *sync.WaitGroup) {
	serviceName := primitives.FrontendService
	persistenceConfig, err := copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for history", tag.Error(err))
	}
	if c.esConfig != nil {
		esDataStoreName := "es-visibility"
		persistenceConfig.AdvancedVisibilityStore = esDataStoreName
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
		fx.Provide(func() listenHostPort { return listenHostPort(c.FrontendGRPCAddress()) }),
		fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
		fx.Provide(func() log.ThrottledLogger { return c.logger }),
		fx.Provide(func() resource.NamespaceLogger { return c.logger }),
		fx.Provide(newRPCFactoryImpl),
		fx.Provide(func() membership.Monitor {
			return newSimpleMonitor(hosts)
		}),
		fx.Provide(func() membership.HostInfoProvider {
			return newSimpleHostInfoProvider(serviceName, hosts)
		}),
		fx.Provide(func() *cluster.Config { return c.clusterMetadataConfig }),
		fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
		fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
		fx.Provide(sdkClientFactoryProvider),
		fx.Provide(func() metrics.Handler { return metrics.NoopMetricsHandler }),
		fx.Provide(func() []grpc.UnaryServerInterceptor { return nil }),
		fx.Provide(func() authorization.Authorizer { return nil }),
		fx.Provide(func() authorization.ClaimMapper { return nil }),
		fx.Provide(func() authorization.JWTAudienceMapper { return nil }),
		fx.Provide(func() client.FactoryProvider { return client.NewFactoryProvider() }),
		fx.Provide(func() searchattribute.Mapper { return nil }),
		// Comment the line above and uncomment the line below to test with search attributes mapper.
		// fx.Provide(func() searchattribute.Mapper { return NewSearchAttributeTestMapper() }),
		fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
		fx.Provide(persistenceClient.FactoryProvider),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return nil }),
		fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
		fx.Provide(func() log.Logger { return c.logger }),
		fx.Provide(resource.DefaultSnTaggedLoggerProvider),
		fx.Provide(func() *esclient.Config { return c.esConfig }),
		fx.Provide(func() esclient.Client { return c.esClient }),
		fx.Supply(c.spanExporters),
		temporal.ServiceTracingModule,
		frontend.Module,
		fx.Populate(&frontendService, &clientBean, &namespaceRegistry, &rpcFactory),
		temporal.FxLogAdapter,
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
	hosts map[primitives.ServiceName][]string,
	startWG *sync.WaitGroup,
) {
	serviceName := primitives.HistoryService
	for _, grpcPort := range c.HistoryServiceAddress() {
		persistenceConfig, err := copyPersistenceConfig(c.persistenceConfig)
		if err != nil {
			c.logger.Fatal("Failed to copy persistence config for history", tag.Error(err))
		}
		if c.esConfig != nil {
			esDataStoreName := "es-visibility"
			persistenceConfig.AdvancedVisibilityStore = esDataStoreName
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
			fx.Provide(func() metrics.Handler { return metrics.NoopMetricsHandler }),
			fx.Provide(func() listenHostPort { return listenHostPort(grpcPort) }),
			fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
			fx.Provide(func() log.ThrottledLogger { return c.logger }),
			fx.Provide(newRPCFactoryImpl),
			fx.Provide(func() membership.Monitor {
				return newSimpleMonitor(hosts)
			}),
			fx.Provide(func() membership.HostInfoProvider {
				return newSimpleHostInfoProvider(serviceName, hosts)
			}),
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
			fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return nil }),
			fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
			fx.Provide(func() log.Logger { return c.logger }),
			fx.Provide(resource.DefaultSnTaggedLoggerProvider),
			fx.Provide(func() *esclient.Config { return c.esConfig }),
			fx.Provide(func() esclient.Client { return c.esClient }),
			fx.Provide(workflow.NewTaskGeneratorProvider),
			fx.Supply(c.spanExporters),
			temporal.ServiceTracingModule,
			history.QueueModule,
			history.Module,
			replication.Module,
			fx.Populate(&historyService, &clientBean, &namespaceRegistry),
			temporal.FxLogAdapter,
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
		historyConnection, err := rpc.Dial(c.HistoryServiceAddress()[0], nil, c.logger)
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

func (c *temporalImpl) startMatching(hosts map[primitives.ServiceName][]string, startWG *sync.WaitGroup) {
	serviceName := primitives.MatchingService

	persistenceConfig, err := copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for matching", tag.Error(err))
	}
	if c.esConfig != nil {
		esDataStoreName := "es-visibility"
		persistenceConfig.AdvancedVisibilityStore = esDataStoreName
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
		fx.Provide(func() metrics.Handler { return metrics.NoopMetricsHandler }),
		fx.Provide(func() listenHostPort { return listenHostPort(c.MatchingGRPCServiceAddress()) }),
		fx.Provide(func() log.ThrottledLogger { return c.logger }),
		fx.Provide(newRPCFactoryImpl),
		fx.Provide(func() membership.Monitor {
			return newSimpleMonitor(hosts)
		}),
		fx.Provide(func() membership.HostInfoProvider {
			return newSimpleHostInfoProvider(serviceName, hosts)
		}),
		fx.Provide(func() *cluster.Config { return c.clusterMetadataConfig }),
		fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
		fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
		fx.Provide(func() client.FactoryProvider { return client.NewFactoryProvider() }),
		fx.Provide(func() searchattribute.Mapper { return nil }),
		fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
		fx.Provide(persistenceClient.FactoryProvider),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return nil }),
		fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
		fx.Provide(func() *esclient.Config { return c.esConfig }),
		fx.Provide(func() esclient.Client { return c.esClient }),
		fx.Provide(func() log.Logger { return c.logger }),
		fx.Provide(resource.DefaultSnTaggedLoggerProvider),
		fx.Supply(c.spanExporters),
		temporal.ServiceTracingModule,
		matching.Module,
		fx.Populate(&matchingService, &clientBean, &namespaceRegistry),
		temporal.FxLogAdapter,
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

func (c *temporalImpl) startWorker(hosts map[primitives.ServiceName][]string, startWG *sync.WaitGroup) {
	serviceName := primitives.WorkerService

	persistenceConfig, err := copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for history", tag.Error(err))
	}
	if c.esConfig != nil {
		esDataStoreName := "es-visibility"
		persistenceConfig.AdvancedVisibilityStore = esDataStoreName
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
		fx.Provide(func() metrics.Handler { return metrics.NoopMetricsHandler }),
		fx.Provide(func() listenHostPort { return listenHostPort(c.WorkerGRPCServiceAddress()) }),
		fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
		fx.Provide(func() log.ThrottledLogger { return c.logger }),
		fx.Provide(newRPCFactoryImpl),
		fx.Provide(func() membership.Monitor {
			return newSimpleMonitor(hosts)
		}),
		fx.Provide(func() membership.HostInfoProvider {
			return newSimpleHostInfoProvider(serviceName, hosts)
		}),
		fx.Provide(func() *cluster.Config { return &clusterConfigCopy }),
		fx.Provide(func() carchiver.ArchivalMetadata { return c.archiverMetadata }),
		fx.Provide(func() provider.ArchiverProvider { return c.archiverProvider }),
		fx.Provide(sdkClientFactoryProvider),
		fx.Provide(func() client.FactoryProvider { return client.NewFactoryProvider() }),
		fx.Provide(func() searchattribute.Mapper { return nil }),
		fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
		fx.Provide(persistenceClient.FactoryProvider),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return nil }),
		fx.Provide(func() dynamicconfig.Client { return c.dcClient }),
		fx.Provide(func() log.Logger { return c.logger }),
		fx.Provide(resource.DefaultSnTaggedLoggerProvider),
		fx.Provide(func() esclient.Client { return c.esClient }),
		fx.Provide(func() *esclient.Config { return c.esConfig }),
		fx.Supply(c.spanExporters),
		temporal.ServiceTracingModule,
		worker.Module,
		fx.Populate(&workerService, &clientBean, &namespaceRegistry),
		temporal.FxLogAdapter,
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

func (c *temporalImpl) overrideHistoryDynamicConfig(client *dcClient) {
	client.OverrideValue(dynamicconfig.ReplicationTaskProcessorStartWait, time.Nanosecond)

	if c.esConfig != nil {
		client.OverrideValue(dynamicconfig.AdvancedVisibilityWritingMode, visibility.SecondaryVisibilityWritingModeDual)
	}
	if c.historyConfig.HistoryCountLimitWarn != 0 {
		client.OverrideValue(dynamicconfig.HistoryCountLimitWarn, c.historyConfig.HistoryCountLimitWarn)
	}
	if c.historyConfig.HistoryCountLimitError != 0 {
		client.OverrideValue(dynamicconfig.HistoryCountLimitError, c.historyConfig.HistoryCountLimitError)
	}
	if c.historyConfig.BlobSizeLimitError != 0 {
		client.OverrideValue(dynamicconfig.BlobSizeLimitError, c.historyConfig.BlobSizeLimitError)
	}
	if c.historyConfig.BlobSizeLimitWarn != 0 {
		client.OverrideValue(dynamicconfig.BlobSizeLimitWarn, c.historyConfig.BlobSizeLimitWarn)
	}
	if c.historyConfig.MutableStateSizeLimitError != 0 {
		client.OverrideValue(dynamicconfig.MutableStateSizeLimitError, c.historyConfig.MutableStateSizeLimitError)
	}
	if c.historyConfig.MutableStateSizeLimitWarn != 0 {
		client.OverrideValue(dynamicconfig.MutableStateSizeLimitWarn, c.historyConfig.MutableStateSizeLimitWarn)
	}

	// For DeleteWorkflowExecution tests
	client.OverrideValue(dynamicconfig.TransferProcessorUpdateAckInterval, 1*time.Second)
	client.OverrideValue(dynamicconfig.VisibilityProcessorUpdateAckInterval, 1*time.Second)
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
) sdk.ClientFactory {
	return sdk.NewClientFactory(
		resolver.MakeURL(primitives.FrontendService),
		nil,
		metricsHandler,
		logger,
		dc.GetIntProperty(dynamicconfig.WorkerStickyCacheSize, 0),
	)
}

type rpcFactoryImpl struct {
	serviceName  primitives.ServiceName
	grpcHostPort string
	logger       log.Logger
	frontendURL  string

	sync.RWMutex
	listener net.Listener
}

func (c *rpcFactoryImpl) GetFrontendGRPCServerOptions() ([]grpc.ServerOption, error) {
	return nil, nil
}

func (c *rpcFactoryImpl) GetInternodeGRPCServerOptions() ([]grpc.ServerOption, error) {
	return nil, nil
}

func (c *rpcFactoryImpl) CreateRemoteFrontendGRPCConnection(hostName string) *grpc.ClientConn {
	return c.CreateGRPCConnection(hostName)
}

func (c *rpcFactoryImpl) CreateLocalFrontendGRPCConnection() *grpc.ClientConn {
	return c.CreateGRPCConnection(c.frontendURL)
}

func (c *rpcFactoryImpl) CreateInternodeGRPCConnection(hostName string) *grpc.ClientConn {
	return c.CreateGRPCConnection(hostName)
}

func newRPCFactoryImpl(sn primitives.ServiceName, grpcHostPort listenHostPort, logger log.Logger, resolver membership.GRPCResolver) common.RPCFactory {
	return &rpcFactoryImpl{
		serviceName:  sn,
		grpcHostPort: string(grpcHostPort),
		logger:       logger,
		frontendURL:  resolver.MakeURL(primitives.FrontendService),
	}
}

func (c *rpcFactoryImpl) GetGRPCListener() net.Listener {
	c.RLock()
	if c.listener != nil {
		c.RUnlock()
		return c.listener
	}
	c.RUnlock()

	c.Lock()
	defer c.Unlock()

	if c.listener == nil {
		var err error
		c.listener, err = net.Listen("tcp", c.grpcHostPort)
		if err != nil {
			c.logger.Fatal("Failed create gRPC listener", tag.Error(err), tag.Service(c.serviceName), tag.Address(c.grpcHostPort))
		}

		c.logger.Info("Created gRPC listener", tag.Service(c.serviceName), tag.Address(c.grpcHostPort))
	}

	return c.listener
}

// CreateGRPCConnection creates connection for gRPC calls
func (c *rpcFactoryImpl) CreateGRPCConnection(hostName string) *grpc.ClientConn {
	connection, err := rpc.Dial(hostName, nil, c.logger)
	if err != nil {
		c.logger.Fatal("Failed to create gRPC connection", tag.Error(err))
	}

	return connection
}

func newSimpleHostInfoProvider(serviceName primitives.ServiceName, hosts map[primitives.ServiceName][]string) membership.HostInfoProvider {
	hostInfo := membership.NewHostInfoFromAddress(hosts[serviceName][0])
	return membership.NewHostInfoProvider(hostInfo)
}
