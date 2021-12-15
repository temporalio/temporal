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

package host

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber/tchannel-go"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
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
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/frontend"
	"go.temporal.io/server/service/history"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/service/worker"
	"go.temporal.io/server/service/worker/archiver"
	"go.temporal.io/server/service/worker/replicator"
)

// Temporal hosts all of temporal services in one process
type Temporal interface {
	Start() error
	Stop()
	GetAdminClient() adminservice.AdminServiceClient
	GetFrontendClient() workflowservice.WorkflowServiceClient
	GetHistoryClient() historyservice.HistoryServiceClient
	GetExecutionManager() persistence.ExecutionManager
	RefreshNamespaceCache()
}

type (
	temporalImpl struct {
		frontendService *frontend.Service
		matchingService *matching.Service
		historyServices []*history.Service
		workerService   resource.Resource

		frontendApp *fx.App
		matchingApp *fx.App
		historyApps []*fx.App

		matchingNamespaceRegistry  namespace.Registry
		frontendNamespaceRegistry  namespace.Registry
		historyNamespaceRegistries []namespace.Registry
		workerNamespaceRegistry    namespace.Registry

		adminClient                      adminservice.AdminServiceClient
		frontendClient                   workflowservice.WorkflowServiceClient
		historyClient                    historyservice.HistoryServiceClient
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
		replicator                       *replicator.Replicator
		clientWorker                     archiver.ClientWorker
		archiverMetadata                 carchiver.ArchivalMetadata
		archiverProvider                 provider.ArchiverProvider
		historyConfig                    *HistoryConfig
		esConfig                         *esclient.Config
		esClient                         esclient.Client
		workerConfig                     *WorkerConfig
		mockAdminClient                  map[string]adminservice.AdminServiceClient
		namespaceReplicationTaskExecutor namespace.ReplicationTaskExecutor
	}

	// HistoryConfig contains configs for history service
	HistoryConfig struct {
		NumHistoryShards       int32
		NumHistoryHosts        int
		HistoryCountLimitError int
		HistoryCountLimitWarn  int
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
	}

	membershipFactoryImpl struct {
		serviceName string
		hosts       map[string][]string
	}
)

// NewTemporal returns an instance that hosts full temporal in one process
func NewTemporal(params *TemporalParams) *temporalImpl {
	return &temporalImpl{
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
	}
}

func (c *temporalImpl) enableWorker() bool {
	return c.workerConfig.EnableArchiver || c.workerConfig.EnableReplicator
}

func (c *temporalImpl) Start() error {
	hosts := make(map[string][]string)
	hosts[common.FrontendServiceName] = []string{c.FrontendGRPCAddress()}
	hosts[common.MatchingServiceName] = []string{c.MatchingGRPCServiceAddress()}
	hosts[common.HistoryServiceName] = c.HistoryServiceAddress(3)
	if c.enableWorker() {
		hosts[common.WorkerServiceName] = []string{c.WorkerGRPCServiceAddress()}
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

func (c *temporalImpl) Stop() {
	if c.enableWorker() {
		c.shutdownWG.Add(4)
		c.workerService.Stop()
	} else {
		c.shutdownWG.Add(3)
	}
	c.frontendApp.Stop(context.Background())
	for _, historyApp := range c.historyApps {
		historyApp.Stop(context.Background())
	}
	c.matchingApp.Stop(context.Background())
	if c.workerConfig.EnableReplicator {
		c.replicator.Stop()
	}
	if c.workerConfig.EnableArchiver {
		c.clientWorker.Stop()
	}
	close(c.shutdownCh)
	c.shutdownWG.Wait()
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

func (c *temporalImpl) FrontendRingpopAddress() string {
	switch c.clusterNo {
	case 0:
		return "127.0.0.1:7124"
	case 1:
		return "127.0.0.1:8124"
	case 2:
		return "127.0.0.1:9124"
	case 3:
		return "127.0.0.1:10124"
	default:
		return "127.0.0.1:7124"
	}
}

// penultimatePortDigit: 2 - ringpop, 3 - gRPC
func (c *temporalImpl) HistoryServiceAddress(penultimatePortDigit int) []string {
	var hosts []string
	startPort := penultimatePortDigit * 10
	switch c.clusterNo {
	case 0:
		startPort += 7201
	case 1:
		startPort += 8201
	case 2:
		startPort += 9201
	case 3:
		startPort += 10201
	default:
		startPort += 7201
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

func (c *temporalImpl) MatchingServiceRingpopAddress() string {
	switch c.clusterNo {
	case 0:
		return "127.0.0.1:7126"
	case 1:
		return "127.0.0.1:8126"
	case 2:
		return "127.0.0.1:9126"
	case 3:
		return "127.0.0.1:10126"
	default:
		return "127.0.0.1:7126"
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

func (c *temporalImpl) WorkerServiceRingpopAddress() string {
	switch c.clusterNo {
	case 0:
		return "127.0.0.1:7128"
	case 1:
		return "127.0.0.1:8128"
	case 2:
		return "127.0.0.1:9128"
	case 3:
		return "127.0.0.1:10128"
	default:
		return "127.0.0.1:7128"
	}
}

func (c *temporalImpl) GetAdminClient() adminservice.AdminServiceClient {
	return c.adminClient
}

func (c *temporalImpl) GetFrontendClient() workflowservice.WorkflowServiceClient {
	return c.frontendClient
}

func (c *temporalImpl) GetHistoryClient() historyservice.HistoryServiceClient {
	return c.historyClient
}

func (c *temporalImpl) startFrontend(hosts map[string][]string, startWG *sync.WaitGroup) {
	params := &resource.BootstrapParams{}
	params.DCRedirectionPolicy = config.DCRedirectionPolicy{}
	params.Name = common.FrontendServiceName
	params.ThrottledLogger = c.logger
	params.RPCFactory = newRPCFactoryImpl(common.FrontendServiceName, c.FrontendGRPCAddress(), c.FrontendRingpopAddress(),
		c.logger)
	tallyScope := tally.NewTestScope(common.FrontendServiceName, make(map[string]string))
	params.MembershipFactoryInitializer = func(x persistenceClient.Bean, y log.Logger) (resource.MembershipMonitorFactory, error) {
		return newMembershipFactory(params.Name, hosts), nil
	}
	params.ClusterMetadataConfig = c.clusterMetadataConfig
	params.MetricsClient = metrics.NewClient(&metrics.ClientConfig{}, tallyScope, metrics.GetMetricsServiceIdx(params.Name, c.logger))
	params.ArchivalMetadata = c.archiverMetadata
	params.ArchiverProvider = c.archiverProvider

	var err error
	params.PersistenceConfig, err = copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for frontend", tag.Error(err))
	}

	if c.esConfig != nil {
		esDataStoreName := "es-visibility"
		params.PersistenceConfig.AdvancedVisibilityStore = esDataStoreName
		params.PersistenceConfig.DataStores[esDataStoreName] = config.DataStore{
			Elasticsearch: c.esConfig,
		}
	}

	params.SdkClientFactory = sdk.NewClientFactory(
		c.FrontendGRPCAddress(),
		nil,
		sdk.NewMetricHandler(params.MetricsClient.UserScope()),
	)

	stoppedCh := make(chan struct{})
	var frontendService *frontend.Service
	var clientBean client.Bean
	var namespaceRegistry namespace.Registry
	feApp := fx.New(
		fx.Supply(
			params,
			stoppedCh,
			params.ClusterMetadataConfig,
			params.PersistenceConfig,
		),
		fx.Provide(func() []grpc.UnaryServerInterceptor { return nil }),
		fx.Provide(func() authorization.Authorizer { return nil }),
		fx.Provide(func() authorization.ClaimMapper { return nil }),
		fx.Provide(func() authorization.JWTAudienceMapper { return nil }),
		fx.Provide(func() client.FactoryProvider { return client.NewFactoryProvider() }),
		fx.Provide(func() searchattribute.Mapper { return nil }),
		fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return nil }),
		fx.Provide(func() dynamicconfig.Client { return newIntegrationConfigClient(dynamicconfig.NewNoopClient()) }),
		fx.Provide(func() log.Logger { return c.logger }),
		fx.Provide(func() *esclient.Config { return c.esConfig }),
		fx.Provide(func() esclient.Client { return c.esClient }),
		frontend.Module,
		fx.Populate(&frontendService, &clientBean, &namespaceRegistry),
		fx.NopLogger,
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
	connection := params.RPCFactory.CreateFrontendGRPCConnection(c.FrontendGRPCAddress())
	c.frontendClient = NewFrontendClient(connection)
	c.adminClient = NewAdminClient(connection)

	feApp.Start(context.Background())

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *temporalImpl) startHistory(
	hosts map[string][]string,
	startWG *sync.WaitGroup,
) {
	membershipPorts := c.HistoryServiceAddress(2)
	for i, grpcPort := range c.HistoryServiceAddress(3) {
		params := &resource.BootstrapParams{}
		params.Name = common.HistoryServiceName
		params.ThrottledLogger = c.logger
		params.RPCFactory = newRPCFactoryImpl(common.HistoryServiceName, grpcPort, membershipPorts[i], c.logger)
		tallyMetricsScope := tally.NewTestScope(common.HistoryServiceName, make(map[string]string))
		params.MembershipFactoryInitializer = func(x persistenceClient.Bean, y log.Logger) (resource.MembershipMonitorFactory, error) {
			return newMembershipFactory(params.Name, hosts), nil
		}
		params.ClusterMetadataConfig = c.clusterMetadataConfig
		params.MetricsClient = metrics.NewClient(&metrics.ClientConfig{}, tallyMetricsScope, metrics.GetMetricsServiceIdx(params.Name, c.logger))
		integrationClient := newIntegrationConfigClient(dynamicconfig.NewNoopClient())
		c.overrideHistoryDynamicConfig(integrationClient)

		params.SdkClientFactory = sdk.NewClientFactory(
			c.FrontendGRPCAddress(),
			nil,
			sdk.NewMetricHandler(params.MetricsClient.UserScope()),
		)

		params.ArchivalMetadata = c.archiverMetadata
		params.ArchiverProvider = c.archiverProvider

		var err error
		params.PersistenceConfig, err = copyPersistenceConfig(c.persistenceConfig)
		if err != nil {
			c.logger.Fatal("Failed to copy persistence config for history", tag.Error(err))
		}

		if c.esConfig != nil {
			esDataStoreName := "es-visibility"
			params.PersistenceConfig.AdvancedVisibilityStore = esDataStoreName
			params.PersistenceConfig.DataStores[esDataStoreName] = config.DataStore{
				Elasticsearch: c.esConfig,
			}
		}

		stoppedCh := make(chan struct{})
		var historyService *history.Service
		var clientBean client.Bean
		var namespaceRegistry namespace.Registry
		app := fx.New(
			fx.Supply(
				params,
				stoppedCh,
				integrationClient,
				params.ClusterMetadataConfig,
				params.PersistenceConfig,
			),
			fx.Provide(func() client.FactoryProvider { return client.NewFactoryProvider() }),
			fx.Provide(func() searchattribute.Mapper { return nil }),
			fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
			fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return nil }),
			fx.Provide(func() dynamicconfig.Client { return integrationClient }),
			fx.Provide(func() log.Logger { return c.logger }),
			fx.Provide(func() *esclient.Config { return c.esConfig }),
			fx.Provide(func() esclient.Client { return c.esClient }),
			history.Module,
			fx.Populate(&historyService, &clientBean, &namespaceRegistry),
			fx.NopLogger)
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
		historyConnection, err := rpc.Dial(c.HistoryServiceAddress(3)[0], nil, c.logger)
		if err != nil {
			c.logger.Fatal("Failed to create connection for history", tag.Error(err))
		}

		c.historyApps = append(c.historyApps, app)
		c.historyClient = NewHistoryClient(historyConnection)
		c.historyServices = append(c.historyServices, historyService)
		c.historyNamespaceRegistries = append(c.historyNamespaceRegistries, namespaceRegistry)

		app.Start(context.Background())
	}

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *temporalImpl) startMatching(hosts map[string][]string, startWG *sync.WaitGroup) {
	params := &resource.BootstrapParams{}
	params.Name = common.MatchingServiceName
	params.ThrottledLogger = c.logger
	params.RPCFactory = newRPCFactoryImpl(common.MatchingServiceName, c.MatchingGRPCServiceAddress(), c.MatchingServiceRingpopAddress(), c.logger)
	tallyMetricsScope := tally.NewTestScope(common.MatchingServiceName, make(map[string]string))
	params.MembershipFactoryInitializer = func(x persistenceClient.Bean, y log.Logger) (resource.MembershipMonitorFactory, error) {
		return newMembershipFactory(params.Name, hosts), nil
	}
	params.ClusterMetadataConfig = c.clusterMetadataConfig
	params.MetricsClient = metrics.NewClient(&metrics.ClientConfig{}, tallyMetricsScope, metrics.GetMetricsServiceIdx(params.Name, c.logger))
	params.ArchivalMetadata = c.archiverMetadata
	params.ArchiverProvider = c.archiverProvider

	var err error
	params.PersistenceConfig, err = copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for matching", tag.Error(err))
	}

	stoppedCh := make(chan struct{})
	var matchingService *matching.Service
	var clientBean client.Bean
	var namespaceRegistry namespace.Registry
	app := fx.New(
		fx.Supply(
			stoppedCh,
			params,
			params.ClusterMetadataConfig,
		),
		fx.Provide(func() client.FactoryProvider { return client.NewFactoryProvider() }),
		fx.Provide(func() searchattribute.Mapper { return nil }),
		fx.Provide(func() resolver.ServiceResolver { return resolver.NewNoopResolver() }),
		fx.Provide(func() persistenceClient.AbstractDataStoreFactory { return nil }),
		fx.Provide(func() dynamicconfig.Client { return newIntegrationConfigClient(dynamicconfig.NewNoopClient()) }),
		fx.Provide(func() log.Logger { return c.logger }),
		matching.Module,
		fx.Populate(&matchingService, &clientBean, &namespaceRegistry),
		fx.NopLogger,
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
	c.matchingApp = app
	c.matchingService = matchingService
	c.matchingNamespaceRegistry = namespaceRegistry
	app.Start(context.Background())

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *temporalImpl) startWorker(hosts map[string][]string, startWG *sync.WaitGroup) {

	params := &resource.BootstrapParams{}
	params.Name = common.WorkerServiceName
	params.ThrottledLogger = c.logger
	params.RPCFactory = newRPCFactoryImpl(common.WorkerServiceName, c.WorkerGRPCServiceAddress(), c.WorkerServiceRingpopAddress(), c.logger)
	tallyMetricsScope := tally.NewTestScope(common.WorkerServiceName, make(map[string]string))
	params.MembershipFactoryInitializer = func(x persistenceClient.Bean, y log.Logger) (resource.MembershipMonitorFactory, error) {
		return newMembershipFactory(params.Name, hosts), nil
	}
	params.ClusterMetadataConfig = c.clusterMetadataConfig
	params.MetricsClient = metrics.NewClient(&metrics.ClientConfig{}, tallyMetricsScope, metrics.GetMetricsServiceIdx(params.Name, c.logger))
	params.ArchivalMetadata = c.archiverMetadata
	params.ArchiverProvider = c.archiverProvider

	var err error
	params.PersistenceConfig, err = copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for worker", tag.Error(err))
	}

	params.SdkClientFactory = sdk.NewClientFactory(
		c.FrontendGRPCAddress(),
		nil,
		sdk.NewMetricHandler(params.MetricsClient.UserScope()),
	)

	dcClient := newIntegrationConfigClient(dynamicconfig.NewNoopClient())
	service, err := resource.New(
		c.logger,
		params,
		common.WorkerServiceName,
		dcClient,
		dynamicconfig.GetIntPropertyFn(5000),
		dynamicconfig.GetIntPropertyFn(5000),
		dynamicconfig.GetIntPropertyFn(10000),
		nil,
		resolver.NewNoopResolver(),
		nil,
	)

	if err != nil {
		c.logger.Fatal("unable to create worker service", tag.Error(err))
	}
	c.workerService = service
	service.Start()

	clusterMetadata := cluster.NewMetadataFromConfig(c.clusterMetadataConfig, c.clusterMetadataMgr, c.logger)
	var replicatorNamespaceCache namespace.Registry
	if c.workerConfig.EnableReplicator {
		metadataManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgr, service.GetMetricsClient(), c.logger)
		replicatorNamespaceCache = namespace.NewRegistry(metadataManager, clusterMetadata.IsGlobalNamespaceEnabled(), service.GetMetricsClient(), service.GetLogger())
		replicatorNamespaceCache.Start()
		c.startWorkerReplicator(service, clusterMetadata)
	}

	var clientWorkerNamespaceCache namespace.Registry
	if c.workerConfig.EnableArchiver {
		metadataProxyManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgr, service.GetMetricsClient(), c.logger)
		clientWorkerNamespaceCache = namespace.NewRegistry(metadataProxyManager, clusterMetadata.IsGlobalNamespaceEnabled(), service.GetMetricsClient(), service.GetLogger())
		clientWorkerNamespaceCache.Start()
		c.startWorkerClientWorker(params, service, clientWorkerNamespaceCache, dcClient)
	}

	startWG.Done()
	<-c.shutdownCh
	if c.workerConfig.EnableReplicator {
		replicatorNamespaceCache.Stop()
	}
	if c.workerConfig.EnableArchiver {
		clientWorkerNamespaceCache.Stop()
	}
	c.shutdownWG.Done()
}

func (c *temporalImpl) startWorkerReplicator(service resource.Resource, clusterMetadata cluster.Metadata) {
	serviceResolver, err := service.GetMembershipMonitor().GetResolver(common.WorkerServiceName)
	if err != nil {
		c.logger.Fatal("Fail to start replicator when start worker", tag.Error(err))
	}
	c.replicator = replicator.NewReplicator(
		clusterMetadata,
		service.GetClientBean(),
		c.logger,
		service.GetMetricsClient(),
		service.GetHostInfo(),
		serviceResolver,
		c.namespaceReplicationQueue,
		c.namespaceReplicationTaskExecutor,
	)
	c.replicator.Start()
}

func (c *temporalImpl) startWorkerClientWorker(params *resource.BootstrapParams, service resource.Resource, namespaceRegistry namespace.Registry, dcClient dynamicconfig.Client) {
	workerConfig := worker.NewConfig(
		c.logger,
		dcClient,
		params,
	)
	workerConfig.ArchiverConfig.ArchiverConcurrency = dynamicconfig.GetIntPropertyFn(10)

	systemSdkClient, err := params.SdkClientFactory.NewSystemClient(c.logger)
	if err != nil {
		c.logger.Fatal("Unable to create SDK client", tag.Error(err))
	}

	bc := &archiver.BootstrapContainer{
		SdkSystemClient:  systemSdkClient,
		MetricsClient:    service.GetMetricsClient(),
		Logger:           c.logger,
		HistoryV2Manager: c.executionManager,
		NamespaceCache:   namespaceRegistry,
		Config:           workerConfig.ArchiverConfig,
		ArchiverProvider: c.archiverProvider,
	}
	c.clientWorker = archiver.NewClientWorker(bc)
	if err := c.clientWorker.Start(); err != nil {
		c.clientWorker.Stop()
		c.logger.Fatal("Fail to start archiver when start worker", tag.Error(err))
	}
}

func (c *temporalImpl) createSystemNamespace() error {
	err := c.metadataMgr.InitializeSystemNamespaces(c.clusterMetadataConfig.CurrentClusterName)
	if err != nil {
		return fmt.Errorf("failed to create temporal-system namespace: %v", err)
	}
	return nil
}

func (c *temporalImpl) GetExecutionManager() persistence.ExecutionManager {
	return c.executionManager
}

func (c *temporalImpl) overrideHistoryDynamicConfig(client *dynamicClient) {
	client.OverrideValue(dynamicconfig.ReplicationTaskProcessorStartWait, time.Nanosecond)

	if c.workerConfig.EnableIndexer {
		client.OverrideValue(dynamicconfig.AdvancedVisibilityWritingMode, visibility.AdvancedVisibilityWritingModeDual)
	}
	if c.historyConfig.HistoryCountLimitWarn != 0 {
		client.OverrideValue(dynamicconfig.HistoryCountLimitWarn, c.historyConfig.HistoryCountLimitWarn)
	}
	if c.historyConfig.HistoryCountLimitError != 0 {
		client.OverrideValue(dynamicconfig.HistoryCountLimitError, c.historyConfig.HistoryCountLimitError)
	}
}

func (c *temporalImpl) RefreshNamespaceCache() {
	c.frontendNamespaceRegistry.Refresh()
	c.matchingNamespaceRegistry.Refresh()
	for _, r := range c.historyNamespaceRegistries {
		r.Refresh()
	}
	if c.workerNamespaceRegistry != nil {
		c.workerNamespaceRegistry.Refresh()
	}
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

func newMembershipFactory(serviceName string, hosts map[string][]string) resource.MembershipMonitorFactory {
	return &membershipFactoryImpl{
		serviceName: serviceName,
		hosts:       hosts,
	}
}

func (p *membershipFactoryImpl) GetMembershipMonitor() (membership.Monitor, error) {
	return newSimpleMonitor(p.serviceName, p.hosts), nil
}

type rpcFactoryImpl struct {
	serviceName        string
	ringpopServiceName string
	grpcHostPort       string
	ringpopHostPort    string
	logger             log.Logger

	sync.Mutex
	listener       net.Listener
	ringpopChannel *tchannel.Channel
	serverCfg      config.GroupTLS
}

func (c *rpcFactoryImpl) GetFrontendGRPCServerOptions() ([]grpc.ServerOption, error) {
	return nil, nil
}

func (c *rpcFactoryImpl) GetInternodeGRPCServerOptions() ([]grpc.ServerOption, error) {
	return nil, nil
}

func (c *rpcFactoryImpl) CreateFrontendGRPCConnection(hostName string) *grpc.ClientConn {
	return c.CreateGRPCConnection(hostName)
}

func (c *rpcFactoryImpl) CreateInternodeGRPCConnection(hostName string) *grpc.ClientConn {
	return c.CreateGRPCConnection(hostName)
}

func newRPCFactoryImpl(sName, grpcHostPort, ringpopHostPort string, logger log.Logger) common.RPCFactory {
	return &rpcFactoryImpl{
		serviceName:     sName,
		grpcHostPort:    grpcHostPort,
		ringpopHostPort: ringpopHostPort,
		logger:          logger,
	}
}

func (c *rpcFactoryImpl) GetGRPCListener() net.Listener {
	if c.listener != nil {
		return c.listener
	}

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

func (c *rpcFactoryImpl) GetRingpopChannel() *tchannel.Channel {
	if c.ringpopChannel != nil {
		return c.ringpopChannel
	}

	c.Lock()
	defer c.Unlock()

	if c.ringpopChannel == nil {
		ringpopServiceName := fmt.Sprintf("%v-ringpop", c.serviceName)

		var err error
		c.ringpopChannel, err = tchannel.NewChannel(ringpopServiceName, nil)
		if err != nil {
			c.logger.Fatal("Failed to create ringpop TChannel", tag.Error(err))
		}

		err = c.ringpopChannel.ListenAndServe(c.ringpopHostPort)
		if err != nil {
			c.logger.Fatal("Failed to start ringpop listener", tag.Error(err), tag.Address(c.ringpopHostPort))
		}
	}

	return c.ringpopChannel
}

// CreateGRPCConnection creates connection for gRPC calls
func (c *rpcFactoryImpl) CreateGRPCConnection(hostName string) *grpc.ClientConn {
	connection, err := rpc.Dial(hostName, nil, c.logger)
	if err != nil {
		c.logger.Fatal("Failed to create gRPC connection", tag.Error(err))
	}

	return connection
}
