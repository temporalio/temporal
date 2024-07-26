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
		// TODO: this is only used to refresh pernsworkermanager, we can get rid of this after
		// it uses dynamic config subscriptions.
		workerServices []*worker.Service

		fxApps []*fx.App

		frontendNamespaceRegistries []namespace.Registry

		// These are routing/load balancing clients but do not do retries:
		adminClient    adminservice.AdminServiceClient
		frontendClient workflowservice.WorkflowServiceClient
		operatorClient operatorservice.OperatorServiceClient
		historyClient  historyservice.HistoryServiceClient
		matchingClient matchingservice.MatchingServiceClient

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
		frontendConfig:                   params.FrontendConfig,
		historyConfig:                    params.HistoryConfig,
		matchingConfig:                   params.MatchingConfig,
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

	// set defaults
	impl.frontendConfig.NumFrontendHosts = max(1, impl.frontendConfig.NumFrontendHosts)
	impl.historyConfig.NumHistoryHosts = max(1, impl.historyConfig.NumHistoryHosts)
	impl.matchingConfig.NumMatchingHosts = max(1, impl.matchingConfig.NumMatchingHosts)
	impl.workerConfig.NumWorkers = max(1, impl.workerConfig.NumWorkers)

	impl.overrideHistoryDynamicConfig(t, testDCClient)
	return impl
}

func (c *temporalImpl) Start() error {
	hosts := make(map[primitives.ServiceName]static.Hosts)
	hosts[primitives.FrontendService] = static.Hosts{All: c.FrontendGRPCAddresses()}
	hosts[primitives.MatchingService] = static.Hosts{All: c.MatchingServiceAddresses()}
	hosts[primitives.HistoryService] = static.Hosts{All: c.HistoryServiceAddresses()}
	hosts[primitives.WorkerService] = static.Hosts{All: c.WorkerServiceAddresses()}

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

	startWG.Add(1)
	go c.startWorker(hosts, &startWG)
	startWG.Wait()

	return nil
}

func (c *temporalImpl) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c.shutdownWG.Add(len(c.fxApps))

	var errs []error
	for _, app := range c.fxApps {
		errs = append(errs, app.Stop(ctx))
	}

	close(c.shutdownCh)
	c.shutdownWG.Wait()

	return multierr.Combine(errs...)
}

func (c *temporalImpl) makeGRPCAddresses(num, basePort int) []string {
	hosts := make([]string, num)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("127.0.0.1:%d", basePort+1000*c.clusterNo+100*i)
	}
	return hosts
}

func (c *temporalImpl) FrontendGRPCAddresses() []string {
	return c.makeGRPCAddresses(c.frontendConfig.NumFrontendHosts, 7134)
}

func (c *temporalImpl) FrontendHTTPAddress() string {
	host, port := c.FrontendHTTPHostPort()
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func (c *temporalImpl) FrontendHTTPHostPort() (string, int) {
	addr0 := c.FrontendGRPCAddresses()[0]
	if host, port, err := net.SplitHostPort(addr0); err != nil {
		panic(fmt.Errorf("Invalid gRPC frontend address: %w", err))
	} else if portNum, err := strconv.Atoi(port); err != nil {
		panic(fmt.Errorf("Invalid gRPC frontend port: %w", err))
	} else {
		return host, portNum + 10
	}
}

func (c *temporalImpl) HistoryServiceAddresses() []string {
	return c.makeGRPCAddresses(c.historyConfig.NumHistoryHosts, 7132)
}

func (c *temporalImpl) MatchingServiceAddresses() []string {
	return c.makeGRPCAddresses(c.matchingConfig.NumMatchingHosts, 7136)
}

func (c *temporalImpl) WorkerServiceAddresses() []string {
	// Note that the worker does not actually listen on this port!
	// This is for identification in membership only.
	return c.makeGRPCAddresses(c.workerConfig.NumWorkers, 7138)
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
	return c.matchingClient
}

func (c *temporalImpl) GetFrontendNamespaceRegistries() []namespace.Registry {
	return c.frontendNamespaceRegistries
}

func (c *temporalImpl) setupMockAdminClient(clientBean client.Bean) {
	if c.mockAdminClient != nil && clientBean != nil {
		for serviceName, client := range c.mockAdminClient {
			clientBean.SetRemoteAdminClient(serviceName, client)
		}
	}
}

func (c *temporalImpl) copyPersistenceConfig() config.Persistence {
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

func (c *temporalImpl) startFrontend(
	hostsByService map[primitives.ServiceName]static.Hosts,
	startWG *sync.WaitGroup,
) {
	serviceName := primitives.FrontendService

	// steal these references from one frontend, it doesn't matter which
	var rpcFactory common.RPCFactory
	var historyRawClient resource.HistoryRawClient
	var matchingRawClient resource.MatchingRawClient

	for _, host := range hostsByService[serviceName].All {
		var namespaceRegistry namespace.Registry
		app := fx.New(
			fx.Invoke(c.setupMockAdminClient),
			fx.Supply(
				c.copyPersistenceConfig(),
				serviceName,
			),
			fx.Provide(c.frontendConfigProvider),
			fx.Provide(func() listenHostPort { return listenHostPort(host) }),
			fx.Provide(func() httpPort {
				_, port := c.FrontendHTTPHostPort()
				return httpPort(port)
			}),
			fx.Provide(func() config.DCRedirectionPolicy { return config.DCRedirectionPolicy{} }),
			fx.Provide(func() log.ThrottledLogger { return c.logger }),
			fx.Provide(func() resource.NamespaceLogger { return c.logger }),
			fx.Provide(c.newRPCFactory),
			static.MembershipModule(makeHostMap(hostsByService, serviceName, host)),
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
			fx.Populate(&namespaceRegistry, &rpcFactory, &historyRawClient, &matchingRawClient),
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.FrontendService),
		)
		err := app.Err()
		if err != nil {
			c.logger.Fatal("unable to construct frontend service", tag.Error(err))
		}

		c.fxApps = append(c.fxApps, app)
		c.frontendNamespaceRegistries = append(c.frontendNamespaceRegistries, namespaceRegistry)

		if err := app.Start(context.Background()); err != nil {
			c.logger.Fatal("unable to start frontend service", tag.Error(err))
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

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *temporalImpl) startHistory(
	hostsByService map[primitives.ServiceName]static.Hosts,
	startWG *sync.WaitGroup,
) {
	serviceName := primitives.HistoryService

	for _, host := range hostsByService[serviceName].All {
		app := fx.New(
			fx.Invoke(c.setupMockAdminClient),
			fx.Supply(
				c.copyPersistenceConfig(),
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
			static.MembershipModule(makeHostMap(hostsByService, serviceName, host)),
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
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.HistoryService),
		)
		err := app.Err()
		if err != nil {
			c.logger.Fatal("unable to construct history service", tag.Error(err))
		}
		c.fxApps = append(c.fxApps, app)
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

	for _, host := range hostsByService[serviceName].All {
		app := fx.New(
			fx.Invoke(c.setupMockAdminClient),
			fx.Supply(
				c.copyPersistenceConfig(),
				serviceName,
			),
			fx.Provide(c.GetMetricsHandler),
			fx.Provide(func() listenHostPort { return listenHostPort(host) }),
			fx.Provide(func() httpPort {
				_, port := c.FrontendHTTPHostPort()
				return httpPort(port)
			}),
			fx.Provide(func() log.ThrottledLogger { return c.logger }),
			fx.Provide(c.newRPCFactory),
			static.MembershipModule(makeHostMap(hostsByService, serviceName, host)),
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
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.MatchingService),
		)
		err := app.Err()
		if err != nil {
			c.logger.Fatal("unable to start matching service", tag.Error(err))
		}
		c.fxApps = append(c.fxApps, app)
		if err := app.Start(context.Background()); err != nil {
			c.logger.Fatal("unable to start matching service", tag.Error(err))
		}
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

	for _, host := range hostsByService[serviceName].All {
		var workerService *worker.Service
		app := fx.New(
			fx.Invoke(c.setupMockAdminClient),
			fx.Supply(
				c.copyPersistenceConfig(),
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
			static.MembershipModule(makeHostMap(hostsByService, serviceName, host)),
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
			fx.Populate(&workerService),
			temporal.FxLogAdapter,
			c.getFxOptionsForService(primitives.WorkerService),
		)
		err := app.Err()
		if err != nil {
			c.logger.Fatal("unable to start worker service", tag.Error(err))
		}

		c.fxApps = append(c.fxApps, app)
		c.workerServices = append(c.workerServices, workerService)
		if err := app.Start(context.Background()); err != nil {
			c.logger.Fatal("unable to start worker service", tag.Error(err))
		}
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

func makeHostMap(
	hostsByService map[primitives.ServiceName]static.Hosts,
	serviceName primitives.ServiceName,
	self string,
) map[primitives.ServiceName]static.Hosts {
	hostMap := maps.Clone(hostsByService)
	hosts := hostMap[serviceName]
	hosts.Self = self
	hostMap[serviceName] = hosts
	return hostMap
}
