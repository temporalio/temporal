// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/admin/adminserviceclient"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/.gen/go/history/historyserviceclient"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	frontendclient "github.com/uber/cadence/client/frontend"
	"github.com/uber/cadence/common"
	carchiver "github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/elasticsearch"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/frontend"
	"github.com/uber/cadence/service/history"
	"github.com/uber/cadence/service/matching"
	"github.com/uber/cadence/service/worker"
	"github.com/uber/cadence/service/worker/archiver"
	"github.com/uber/cadence/service/worker/indexer"
	"github.com/uber/cadence/service/worker/replicator"
	cwsc "go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/tchannel"
)

// Cadence hosts all of cadence services in one process
type Cadence interface {
	Start() error
	Stop()
	GetAdminClient() adminserviceclient.Interface
	GetFrontendClient() workflowserviceclient.Interface
	FrontendAddress() string
	GetFrontendService() service.Service
	GetHistoryClient() historyserviceclient.Interface
}

type (
	cadenceImpl struct {
		adminHandler           *frontend.AdminHandler
		frontendHandler        *frontend.WorkflowHandler
		matchingHandler        *matching.Handler
		historyHandlers        []*history.Handler
		logger                 log.Logger
		clusterMetadata        cluster.Metadata
		persistenceConfig      config.Persistence
		dispatcherProvider     client.DispatcherProvider
		messagingClient        messaging.Client
		metadataMgr            persistence.MetadataManager
		shardMgr               persistence.ShardManager
		historyV2Mgr           persistence.HistoryV2Manager
		taskMgr                persistence.TaskManager
		visibilityMgr          persistence.VisibilityManager
		executionMgrFactory    persistence.ExecutionManagerFactory
		domainReplicationQueue persistence.DomainReplicationQueue
		shutdownCh             chan struct{}
		shutdownWG             sync.WaitGroup
		frontEndService        service.Service
		historyService         service.Service
		clusterNo              int // cluster number
		replicator             *replicator.Replicator
		clientWorker           archiver.ClientWorker
		indexer                *indexer.Indexer
		enbaleNDC              bool
		archiverMetadata       carchiver.ArchivalMetadata
		archiverProvider       provider.ArchiverProvider
		historyConfig          *HistoryConfig
		esConfig               *elasticsearch.Config
		esClient               elasticsearch.Client
		workerConfig           *WorkerConfig
		mockFrontendClient     map[string]frontendclient.Client
	}

	// HistoryConfig contains configs for history service
	HistoryConfig struct {
		NumHistoryShards       int
		NumHistoryHosts        int
		HistoryCountLimitError int
		HistoryCountLimitWarn  int
	}

	// CadenceParams contains everything needed to bootstrap Cadence
	CadenceParams struct {
		ClusterMetadata               cluster.Metadata
		PersistenceConfig             config.Persistence
		DispatcherProvider            client.DispatcherProvider
		MessagingClient               messaging.Client
		MetadataMgr                   persistence.MetadataManager
		ShardMgr                      persistence.ShardManager
		HistoryV2Mgr                  persistence.HistoryV2Manager
		ExecutionMgrFactory           persistence.ExecutionManagerFactory
		TaskMgr                       persistence.TaskManager
		VisibilityMgr                 persistence.VisibilityManager
		Logger                        log.Logger
		ClusterNo                     int
		EnableNDC                     bool
		ArchiverMetadata              carchiver.ArchivalMetadata
		ArchiverProvider              provider.ArchiverProvider
		EnableReadHistoryFromArchival bool
		HistoryConfig                 *HistoryConfig
		ESConfig                      *elasticsearch.Config
		ESClient                      elasticsearch.Client
		WorkerConfig                  *WorkerConfig
		MockFrontendClient            map[string]frontendclient.Client
		DomainReplicationQueue        persistence.DomainReplicationQueue
	}

	membershipFactoryImpl struct {
		serviceName string
		hosts       map[string][]string
	}
)

// NewCadence returns an instance that hosts full cadence in one process
func NewCadence(params *CadenceParams) Cadence {
	return &cadenceImpl{
		logger:                 params.Logger,
		clusterMetadata:        params.ClusterMetadata,
		persistenceConfig:      params.PersistenceConfig,
		dispatcherProvider:     params.DispatcherProvider,
		messagingClient:        params.MessagingClient,
		metadataMgr:            params.MetadataMgr,
		visibilityMgr:          params.VisibilityMgr,
		shardMgr:               params.ShardMgr,
		historyV2Mgr:           params.HistoryV2Mgr,
		taskMgr:                params.TaskMgr,
		executionMgrFactory:    params.ExecutionMgrFactory,
		domainReplicationQueue: params.DomainReplicationQueue,
		shutdownCh:             make(chan struct{}),
		clusterNo:              params.ClusterNo,
		enbaleNDC:              params.EnableNDC,
		esConfig:               params.ESConfig,
		esClient:               params.ESClient,
		archiverMetadata:       params.ArchiverMetadata,
		archiverProvider:       params.ArchiverProvider,
		historyConfig:          params.HistoryConfig,
		workerConfig:           params.WorkerConfig,
		mockFrontendClient:     params.MockFrontendClient,
	}
}

func (c *cadenceImpl) enableWorker() bool {
	return c.workerConfig.EnableArchiver || c.workerConfig.EnableIndexer || c.workerConfig.EnableReplicator
}

func (c *cadenceImpl) Start() error {
	hosts := make(map[string][]string)
	hosts[common.FrontendServiceName] = []string{c.FrontendAddress()}
	hosts[common.MatchingServiceName] = []string{c.MatchingServiceAddress()}
	hosts[common.HistoryServiceName] = c.HistoryServiceAddress()
	if c.enableWorker() {
		hosts[common.WorkerServiceName] = []string{c.WorkerServiceAddress()}
	}

	// create cadence-system domain, this must be created before starting
	// the services - so directly use the metadataManager to create this
	if err := c.createSystemDomain(); err != nil {
		return err
	}

	var startWG sync.WaitGroup
	startWG.Add(2)
	go c.startHistory(hosts, &startWG, c.enbaleNDC)
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

func (c *cadenceImpl) Stop() {
	if c.enableWorker() {
		c.shutdownWG.Add(4)
	} else {
		c.shutdownWG.Add(3)
	}
	c.frontendHandler.Stop()
	c.adminHandler.Stop()
	for _, historyHandler := range c.historyHandlers {
		historyHandler.Stop()
	}
	c.matchingHandler.Stop()
	if c.workerConfig.EnableReplicator {
		c.replicator.Stop()
	}
	if c.workerConfig.EnableArchiver {
		c.clientWorker.Stop()
	}
	close(c.shutdownCh)
	c.shutdownWG.Wait()
}

func (c *cadenceImpl) FrontendAddress() string {
	switch c.clusterNo {
	case 0:
		return "127.0.0.1:7104"
	case 1:
		return "127.0.0.1:8104"
	case 2:
		return "127.0.0.1:9104"
	case 3:
		return "127.0.0.1:10104"
	default:
		return "127.0.0.1:7104"
	}
}

func (c *cadenceImpl) FrontendPProfPort() int {
	switch c.clusterNo {
	case 0:
		return 7105
	case 1:
		return 8105
	case 2:
		return 9105
	case 3:
		return 10105
	default:
		return 7105
	}
}

func (c *cadenceImpl) HistoryServiceAddress() []string {
	hosts := []string{}
	startPort := 7201
	switch c.clusterNo {
	case 0:
		startPort = 7201
	case 1:
		startPort = 8201
	case 2:
		startPort = 9201
	case 3:
		startPort = 10201
	default:
		startPort = 7201
	}
	for i := 0; i < c.historyConfig.NumHistoryHosts; i++ {
		port := startPort + i
		hosts = append(hosts, fmt.Sprintf("127.0.0.1:%v", port))
	}

	c.logger.Info("History hosts", tag.Addresses(hosts))
	return hosts
}

func (c *cadenceImpl) HistoryPProfPort() []int {
	ports := []int{}
	startPort := 7301
	switch c.clusterNo {
	case 0:
		startPort = 7301
	case 1:
		startPort = 8301
	case 2:
		startPort = 9301
	case 3:
		startPort = 10301
	default:
		startPort = 7301
	}
	for i := 0; i < c.historyConfig.NumHistoryHosts; i++ {
		port := startPort + i
		ports = append(ports, port)
	}

	c.logger.Info("History pprof ports", tag.Value(ports))
	return ports
}

func (c *cadenceImpl) MatchingServiceAddress() string {
	switch c.clusterNo {
	case 0:
		return "127.0.0.1:7106"
	case 1:
		return "127.0.0.1:8106"
	case 2:
		return "127.0.0.1:9106"
	case 3:
		return "127.0.0.1:10106"
	default:
		return "127.0.0.1:7106"
	}
}

func (c *cadenceImpl) MatchingPProfPort() int {
	switch c.clusterNo {
	case 0:
		return 7107
	case 1:
		return 8107
	case 2:
		return 9107
	case 3:
		return 10107
	default:
		return 7107
	}
}

func (c *cadenceImpl) WorkerServiceAddress() string {
	switch c.clusterNo {
	case 0:
		return "127.0.0.1:7108"
	case 1:
		return "127.0.0.1:8108"
	case 2:
		return "127.0.0.1:9108"
	case 3:
		return "127.0.0.1:10108"
	default:
		return "127.0.0.1:7108"
	}
}

func (c *cadenceImpl) WorkerPProfPort() int {
	switch c.clusterNo {
	case 0:
		return 7109
	case 1:
		return 8109
	case 2:
		return 9109
	case 3:
		return 10109
	default:
		return 7109
	}
}

func (c *cadenceImpl) GetAdminClient() adminserviceclient.Interface {
	return NewAdminClient(c.frontEndService.GetDispatcher())
}

func (c *cadenceImpl) GetFrontendClient() workflowserviceclient.Interface {
	return NewFrontendClient(c.frontEndService.GetDispatcher())
}

// For integration tests to get hold of FE instance.
func (c *cadenceImpl) GetFrontendService() service.Service {
	return c.frontEndService
}

func (c *cadenceImpl) GetHistoryClient() historyserviceclient.Interface {
	return NewHistoryClient(c.historyService.GetDispatcher())
}

func (c *cadenceImpl) startFrontend(hosts map[string][]string, startWG *sync.WaitGroup) {
	params := new(service.BootstrapParams)
	params.DCRedirectionPolicy = config.DCRedirectionPolicy{}
	params.Name = common.FrontendServiceName
	params.Logger = c.logger
	params.ThrottledLogger = c.logger
	params.PProfInitializer = newPProfInitializerImpl(c.logger, c.FrontendPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.FrontendServiceName, c.FrontendAddress(), c.logger)
	params.MetricScope = tally.NewTestScope(common.FrontendServiceName, make(map[string]string))
	params.MembershipFactory = newMembershipFactory(params.Name, hosts)
	params.ClusterMetadata = c.clusterMetadata
	params.DispatcherProvider = c.dispatcherProvider
	params.MessagingClient = c.messagingClient
	params.PersistenceConfig = c.persistenceConfig
	params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, c.logger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())
	params.ArchivalMetadata = c.archiverMetadata
	params.ArchiverProvider = c.archiverProvider

	var replicationMessageSink messaging.Producer
	var err error
	if c.workerConfig.EnableReplicator {
		replicationMessageSink = c.domainReplicationQueue
	} else {
		replicationMessageSink = &mocks.KafkaProducer{}
		replicationMessageSink.(*mocks.KafkaProducer).On("Publish", mock.Anything).Return(nil)
	}

	c.frontEndService = service.New(params)

	domainCache := cache.NewDomainCache(c.metadataMgr, c.clusterMetadata, c.frontEndService.GetMetricsClient(), c.logger)
	c.adminHandler = frontend.NewAdminHandler(
		c.frontEndService, c.historyConfig.NumHistoryShards, domainCache, c.historyV2Mgr, params)
	c.adminHandler.RegisterHandler()

	dc := dynamicconfig.NewCollection(params.DynamicConfig, c.logger)
	frontendConfig := frontend.NewConfig(dc, c.historyConfig.NumHistoryShards, c.esConfig != nil)

	historyArchiverBootstrapContainer := &carchiver.HistoryBootstrapContainer{
		HistoryV2Manager: c.historyV2Mgr,
		Logger:           c.logger,
		MetricsClient:    c.frontEndService.GetMetricsClient(),
		ClusterMetadata:  c.clusterMetadata,
		DomainCache:      domainCache,
	}
	visibilityArchiverBootstrapContainer := &carchiver.VisibilityBootstrapContainer{
		Logger:          c.logger,
		MetricsClient:   c.frontEndService.GetMetricsClient(),
		ClusterMetadata: c.clusterMetadata,
		DomainCache:     domainCache,
	}
	err = c.archiverProvider.RegisterBootstrapContainer(common.FrontendServiceName, historyArchiverBootstrapContainer, visibilityArchiverBootstrapContainer)
	if err != nil {
		c.logger.Fatal("Failed to register archiver bootstrap container for frontend service", tag.Error(err))
	}

	c.frontendHandler = frontend.NewWorkflowHandler(
		c.frontEndService,
		frontendConfig,
		c.metadataMgr,
		c.historyV2Mgr,
		c.visibilityMgr,
		replicationMessageSink,
		c.domainReplicationQueue,
		domainCache)
	dcRedirectionHandler := frontend.NewDCRedirectionHandler(c.frontendHandler, params.DCRedirectionPolicy)
	dcRedirectionHandler.RegisterHandler()

	// must start base service first
	c.frontEndService.Start()
	if c.mockFrontendClient != nil {
		clientBean := c.frontEndService.GetClientBean()
		if clientBean != nil {
			for serviceName, client := range c.mockFrontendClient {
				clientBean.SetRemoteFrontendClient(serviceName, client)
			}
		}
	}

	err = c.adminHandler.Start()
	if err != nil {
		c.logger.Fatal("Failed to start admin", tag.Error(err))
	}
	err = dcRedirectionHandler.Start()
	if err != nil {
		c.logger.Fatal("Failed to start frontend", tag.Error(err))
	}

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startHistory(
	hosts map[string][]string,
	startWG *sync.WaitGroup,
	enableNDC bool,
) {

	pprofPorts := c.HistoryPProfPort()
	for i, hostport := range c.HistoryServiceAddress() {
		params := new(service.BootstrapParams)
		params.Name = common.HistoryServiceName
		params.Logger = c.logger
		params.ThrottledLogger = c.logger
		params.PProfInitializer = newPProfInitializerImpl(c.logger, pprofPorts[i])
		params.RPCFactory = newRPCFactoryImpl(common.HistoryServiceName, hostport, c.logger)
		params.MetricScope = tally.NewTestScope(common.HistoryServiceName, make(map[string]string))
		params.MembershipFactory = newMembershipFactory(params.Name, hosts)
		params.ClusterMetadata = c.clusterMetadata
		params.DispatcherProvider = c.dispatcherProvider
		params.MessagingClient = c.messagingClient
		params.PersistenceConfig = c.persistenceConfig
		params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, c.logger))
		params.DynamicConfig = dynamicconfig.NewNopClient()
		dispatcher, err := params.DispatcherProvider.Get(common.FrontendServiceName, c.FrontendAddress())
		if err != nil {
			c.logger.Fatal("Failed to get dispatcher for frontend", tag.Error(err))
		}
		params.PublicClient = cwsc.New(dispatcher.ClientConfig(common.FrontendServiceName))
		params.ArchivalMetadata = c.archiverMetadata
		params.ArchiverProvider = c.archiverProvider

		service := service.New(params)
		c.historyService = service
		hConfig := c.historyConfig
		historyConfig := history.NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, c.logger),
			hConfig.NumHistoryShards, config.StoreTypeCassandra, params.PersistenceConfig.IsAdvancedVisibilityConfigExist())
		historyConfig.HistoryMgrNumConns = dynamicconfig.GetIntPropertyFn(hConfig.NumHistoryShards)
		historyConfig.ExecutionMgrNumConns = dynamicconfig.GetIntPropertyFn(hConfig.NumHistoryShards)
		historyConfig.DecisionHeartbeatTimeout = dynamicconfig.GetDurationPropertyFnFilteredByDomain(time.Second * 5)
		historyConfig.TimerProcessorHistoryArchivalSizeLimit = dynamicconfig.GetIntPropertyFn(5 * 1024)
		historyConfig.EnableNDC = dynamicconfig.GetBoolPropertyFnFilteredByDomain(enableNDC)

		if c.workerConfig.EnableIndexer {
			historyConfig.AdvancedVisibilityWritingMode = dynamicconfig.GetStringPropertyFn(common.AdvancedVisibilityWritingModeDual)
		}
		if hConfig.HistoryCountLimitWarn != 0 {
			historyConfig.HistoryCountLimitWarn = dynamicconfig.GetIntPropertyFilteredByDomain(hConfig.HistoryCountLimitWarn)
		}
		if hConfig.HistoryCountLimitError != 0 {
			historyConfig.HistoryCountLimitError = dynamicconfig.GetIntPropertyFilteredByDomain(hConfig.HistoryCountLimitError)
		}
		domainCache := cache.NewDomainCache(c.metadataMgr, c.clusterMetadata, service.GetMetricsClient(), c.logger)

		historyArchiverBootstrapContainer := &carchiver.HistoryBootstrapContainer{
			HistoryV2Manager: c.historyV2Mgr,
			Logger:           c.logger,
			MetricsClient:    service.GetMetricsClient(),
			ClusterMetadata:  c.clusterMetadata,
			DomainCache:      domainCache,
		}
		visibilityArchiverBootstrapContainer := &carchiver.VisibilityBootstrapContainer{
			Logger:          c.logger,
			MetricsClient:   service.GetMetricsClient(),
			ClusterMetadata: c.clusterMetadata,
			DomainCache:     domainCache,
		}
		err = c.archiverProvider.RegisterBootstrapContainer(common.HistoryServiceName, historyArchiverBootstrapContainer, visibilityArchiverBootstrapContainer)
		if err != nil {
			c.logger.Fatal("Failed to register archiver bootstrap container for history service", tag.Error(err))
		}

		handler := history.NewHandler(service, historyConfig, c.shardMgr, c.metadataMgr,
			c.visibilityMgr, c.historyV2Mgr, c.executionMgrFactory, domainCache, params.PublicClient)
		handler.RegisterHandler()

		service.Start()
		if c.mockFrontendClient != nil {
			clientBean := service.GetClientBean()
			if clientBean != nil {
				for serviceName, client := range c.mockFrontendClient {
					clientBean.SetRemoteFrontendClient(serviceName, client)
				}
			}
		}

		err = handler.Start()
		if err != nil {
			c.logger.Fatal("Failed to start history", tag.Error(err))
		}

		c.historyHandlers = append(c.historyHandlers, handler)
	}
	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startMatching(hosts map[string][]string, startWG *sync.WaitGroup) {

	params := new(service.BootstrapParams)
	params.Name = common.MatchingServiceName
	params.Logger = c.logger
	params.ThrottledLogger = c.logger
	params.PProfInitializer = newPProfInitializerImpl(c.logger, c.MatchingPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.MatchingServiceName, c.MatchingServiceAddress(), c.logger)
	params.MetricScope = tally.NewTestScope(common.MatchingServiceName, make(map[string]string))
	params.MembershipFactory = newMembershipFactory(params.Name, hosts)
	params.ClusterMetadata = c.clusterMetadata
	params.DispatcherProvider = c.dispatcherProvider
	params.PersistenceConfig = c.persistenceConfig
	params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, c.logger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())

	service := service.New(params)
	c.matchingHandler = matching.NewHandler(
		service, matching.NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, c.logger)), c.taskMgr, c.metadataMgr,
	)
	c.matchingHandler.RegisterHandler()

	service.Start()
	if c.mockFrontendClient != nil {
		clientBean := service.GetClientBean()
		if clientBean != nil {
			for serviceName, client := range c.mockFrontendClient {
				clientBean.SetRemoteFrontendClient(serviceName, client)
			}
		}
	}

	err := c.matchingHandler.Start()
	if err != nil {
		c.logger.Fatal("Failed to start history", tag.Error(err))
	}

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startWorker(hosts map[string][]string, startWG *sync.WaitGroup) {
	params := new(service.BootstrapParams)
	params.Name = common.WorkerServiceName
	params.Logger = c.logger
	params.ThrottledLogger = c.logger
	params.PProfInitializer = newPProfInitializerImpl(c.logger, c.WorkerPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.WorkerServiceName, c.WorkerServiceAddress(), c.logger)
	params.MetricScope = tally.NewTestScope(common.WorkerServiceName, make(map[string]string))
	params.MembershipFactory = newMembershipFactory(params.Name, hosts)
	params.ClusterMetadata = c.clusterMetadata
	params.DispatcherProvider = c.dispatcherProvider
	params.PersistenceConfig = c.persistenceConfig
	params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, c.logger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())
	params.ArchivalMetadata = c.archiverMetadata
	params.ArchiverProvider = c.archiverProvider

	dispatcher, err := params.DispatcherProvider.Get(common.FrontendServiceName, c.FrontendAddress())
	if err != nil {
		c.logger.Fatal("Failed to get dispatcher for frontend", tag.Error(err))
	}
	params.PublicClient = cwsc.New(dispatcher.ClientConfig(common.FrontendServiceName))
	service := service.New(params)
	service.Start()

	var replicatorDomainCache cache.DomainCache
	if c.workerConfig.EnableReplicator {
		metadataManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgr, service.GetMetricsClient(), c.logger)
		replicatorDomainCache = cache.NewDomainCache(metadataManager, params.ClusterMetadata, service.GetMetricsClient(), service.GetLogger())
		replicatorDomainCache.Start()
		c.startWorkerReplicator(params, service, replicatorDomainCache)
	}

	var clientWorkerDomainCache cache.DomainCache
	if c.workerConfig.EnableArchiver {
		metadataProxyManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgr, service.GetMetricsClient(), c.logger)
		clientWorkerDomainCache = cache.NewDomainCache(metadataProxyManager, params.ClusterMetadata, service.GetMetricsClient(), service.GetLogger())
		clientWorkerDomainCache.Start()
		c.startWorkerClientWorker(params, service, clientWorkerDomainCache)
	}

	if c.workerConfig.EnableIndexer {
		c.startWorkerIndexer(params, service)
	}

	startWG.Done()
	<-c.shutdownCh
	if c.workerConfig.EnableReplicator {
		replicatorDomainCache.Stop()
	}
	if c.workerConfig.EnableArchiver {
		clientWorkerDomainCache.Stop()
	}
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startWorkerReplicator(params *service.BootstrapParams, service service.Service, domainCache cache.DomainCache) {
	metadataManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgr, service.GetMetricsClient(), c.logger)
	workerConfig := worker.NewConfig(params)
	workerConfig.ReplicationCfg.ReplicatorMessageConcurrency = dynamicconfig.GetIntPropertyFn(10)
	serviceResolver, err := service.GetMembershipMonitor().GetResolver(common.WorkerServiceName)
	if err != nil {
		c.logger.Fatal("Fail to start replicator when start worker", tag.Error(err))
	}
	c.replicator = replicator.NewReplicator(
		c.clusterMetadata,
		metadataManager,
		domainCache,
		service.GetClientBean(),
		workerConfig.ReplicationCfg,
		c.messagingClient,
		c.logger,
		service.GetMetricsClient(),
		service.GetHostInfo(),
		serviceResolver,
	)
	if err := c.replicator.Start(); err != nil {
		c.replicator.Stop()
		c.logger.Fatal("Fail to start replicator when start worker", tag.Error(err))
	}
}

func (c *cadenceImpl) startWorkerClientWorker(params *service.BootstrapParams, service service.Service, domainCache cache.DomainCache) {
	workerConfig := worker.NewConfig(params)
	workerConfig.ArchiverConfig.ArchiverConcurrency = dynamicconfig.GetIntPropertyFn(10)
	historyArchiverBootstrapContainer := &carchiver.HistoryBootstrapContainer{
		HistoryV2Manager: c.historyV2Mgr,
		Logger:           c.logger,
		MetricsClient:    service.GetMetricsClient(),
		ClusterMetadata:  c.clusterMetadata,
		DomainCache:      domainCache,
	}
	err := c.archiverProvider.RegisterBootstrapContainer(common.WorkerServiceName, historyArchiverBootstrapContainer, &carchiver.VisibilityBootstrapContainer{})
	if err != nil {
		c.logger.Fatal("Failed to register archiver bootstrap container for worker service", tag.Error(err))
	}

	bc := &archiver.BootstrapContainer{
		PublicClient:     params.PublicClient,
		MetricsClient:    service.GetMetricsClient(),
		Logger:           c.logger,
		HistoryV2Manager: c.historyV2Mgr,
		DomainCache:      domainCache,
		Config:           workerConfig.ArchiverConfig,
		ArchiverProvider: c.archiverProvider,
	}
	c.clientWorker = archiver.NewClientWorker(bc)
	if err := c.clientWorker.Start(); err != nil {
		c.clientWorker.Stop()
		c.logger.Fatal("Fail to start archiver when start worker", tag.Error(err))
	}
}

func (c *cadenceImpl) startWorkerIndexer(params *service.BootstrapParams, service service.Service) {
	params.DynamicConfig.UpdateValue(dynamicconfig.AdvancedVisibilityWritingMode, common.AdvancedVisibilityWritingModeDual)
	workerConfig := worker.NewConfig(params)
	c.indexer = indexer.NewIndexer(
		workerConfig.IndexerCfg,
		c.messagingClient,
		c.esClient,
		c.esConfig,
		c.logger,
		service.GetMetricsClient())
	if err := c.indexer.Start(); err != nil {
		c.indexer.Stop()
		c.logger.Fatal("Fail to start indexer when start worker", tag.Error(err))
	}
}

func (c *cadenceImpl) createSystemDomain() error {

	_, err := c.metadataMgr.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        "cadence-system",
			Status:      persistence.DomainStatusRegistered,
			Description: "Cadence system domain",
		},
		Config: &persistence.DomainConfig{
			Retention:                1,
			HistoryArchivalStatus:    shared.ArchivalStatusDisabled,
			VisibilityArchivalStatus: shared.ArchivalStatusDisabled,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
		FailoverVersion:   common.EmptyVersion,
	})
	if err != nil {
		if _, ok := err.(*shared.DomainAlreadyExistsError); ok {
			return nil
		}
		return fmt.Errorf("failed to create cadence-system domain: %v", err)
	}
	return nil
}

func newMembershipFactory(serviceName string, hosts map[string][]string) service.MembershipMonitorFactory {
	return &membershipFactoryImpl{
		serviceName: serviceName,
		hosts:       hosts,
	}
}

func (p *membershipFactoryImpl) Create(dispatcher *yarpc.Dispatcher) (membership.Monitor, error) {
	return newSimpleMonitor(p.serviceName, p.hosts), nil
}

func newPProfInitializerImpl(logger log.Logger, port int) common.PProfInitializer {
	return &config.PProfInitializerImpl{
		PProf: &config.PProf{
			Port: port,
		},
		Logger: logger,
	}
}

type rpcFactoryImpl struct {
	ch          *tchannel.ChannelTransport
	serviceName string
	hostPort    string
	logger      log.Logger
}

func newRPCFactoryImpl(sName string, hostPort string, logger log.Logger) common.RPCFactory {
	return &rpcFactoryImpl{
		serviceName: sName,
		hostPort:    hostPort,
		logger:      logger,
	}
}

func (c *rpcFactoryImpl) CreateDispatcher() *yarpc.Dispatcher {
	// Setup dispatcher for onebox
	var err error
	c.ch, err = tchannel.NewChannelTransport(
		tchannel.ServiceName(c.serviceName), tchannel.ListenAddr(c.hostPort))
	if err != nil {
		c.logger.Fatal("Failed to create transport channel", tag.Error(err))
	}
	return yarpc.NewDispatcher(yarpc.Config{
		Name:     c.serviceName,
		Inbounds: yarpc.Inbounds{c.ch.NewInbound()},
		// For integration tests to generate client out of the same outbound.
		Outbounds: yarpc.Outbounds{
			c.serviceName: {Unary: c.ch.NewSingleOutbound(c.hostPort)},
		},
		InboundMiddleware: yarpc.InboundMiddleware{
			Unary: &versionMiddleware{},
		},
	})
}

type versionMiddleware struct {
}

func (vm *versionMiddleware) Handle(ctx context.Context, req *transport.Request, resw transport.ResponseWriter, h transport.UnaryHandler) error {
	req.Headers = req.Headers.With(common.LibraryVersionHeaderName, "1.0.0").With(common.FeatureVersionHeaderName, "1.0.0").With(common.ClientImplHeaderName, "uber-go")
	return h.Handle(ctx, req, resw)
}

func (c *rpcFactoryImpl) CreateDispatcherForOutbound(
	callerName, serviceName, hostName string) *yarpc.Dispatcher {
	// Setup dispatcher(outbound) for onebox
	d := yarpc.NewDispatcher(yarpc.Config{
		Name: callerName,
		Outbounds: yarpc.Outbounds{
			serviceName: {Unary: c.ch.NewSingleOutbound(hostName)},
		},
	})
	if err := d.Start(); err != nil {
		c.logger.Fatal("Failed to create outbound transport channel", tag.Error(err))
	}
	return d
}
