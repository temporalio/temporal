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

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/admin/adminserviceclient"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceserver"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore"
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

const archivalBlobSize = 5 * 1024 // 5KB

// Cadence hosts all of cadence services in one process
type Cadence interface {
	Start() error
	Stop()
	GetAdminClient() adminserviceclient.Interface
	GetFrontendClient() workflowserviceclient.Interface
	FrontendAddress() string
	GetFrontendService() service.Service
}

type (
	cadenceImpl struct {
		initLock            sync.Mutex
		adminHandler        *frontend.AdminHandler
		frontendHandler     *frontend.WorkflowHandler
		matchingHandler     *matching.Handler
		historyHandlers     []*history.Handler
		logger              log.Logger
		clusterMetadata     cluster.Metadata
		persistenceConfig   config.Persistence
		dispatcherProvider  client.DispatcherProvider
		messagingClient     messaging.Client
		metadataMgr         persistence.MetadataManager
		metadataMgrV2       persistence.MetadataManager
		shardMgr            persistence.ShardManager
		historyMgr          persistence.HistoryManager
		historyV2Mgr        persistence.HistoryV2Manager
		taskMgr             persistence.TaskManager
		visibilityMgr       persistence.VisibilityManager
		executionMgrFactory persistence.ExecutionManagerFactory
		shutdownCh          chan struct{}
		shutdownWG          sync.WaitGroup
		frontEndService     service.Service
		clusterNo           int // cluster number
		replicator          *replicator.Replicator
		clientWorker        archiver.ClientWorker
		indexer             *indexer.Indexer
		enableEventsV2      bool
		blobstoreClient     blobstore.Client
		historyConfig       *HistoryConfig
		esConfig            *elasticsearch.Config
		esClient            elasticsearch.Client
		workerConfig        *WorkerConfig
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
		MetadataMgrV2                 persistence.MetadataManager
		ShardMgr                      persistence.ShardManager
		HistoryMgr                    persistence.HistoryManager
		HistoryV2Mgr                  persistence.HistoryV2Manager
		ExecutionMgrFactory           persistence.ExecutionManagerFactory
		TaskMgr                       persistence.TaskManager
		VisibilityMgr                 persistence.VisibilityManager
		Logger                        log.Logger
		ClusterNo                     int
		EnableEventsV2                bool
		Blobstore                     blobstore.Client
		EnableReadHistoryFromArchival bool
		HistoryConfig                 *HistoryConfig
		ESConfig                      *elasticsearch.Config
		ESClient                      elasticsearch.Client
		WorkerConfig                  *WorkerConfig
	}

	membershipFactoryImpl struct {
		serviceName string
		hosts       map[string][]string
	}
)

// NewCadence returns an instance that hosts full cadence in one process
func NewCadence(params *CadenceParams) Cadence {
	return &cadenceImpl{
		logger:              params.Logger,
		clusterMetadata:     params.ClusterMetadata,
		persistenceConfig:   params.PersistenceConfig,
		dispatcherProvider:  params.DispatcherProvider,
		messagingClient:     params.MessagingClient,
		metadataMgr:         params.MetadataMgr,
		metadataMgrV2:       params.MetadataMgrV2,
		visibilityMgr:       params.VisibilityMgr,
		shardMgr:            params.ShardMgr,
		historyMgr:          params.HistoryMgr,
		historyV2Mgr:        params.HistoryV2Mgr,
		taskMgr:             params.TaskMgr,
		executionMgrFactory: params.ExecutionMgrFactory,
		shutdownCh:          make(chan struct{}),
		clusterNo:           params.ClusterNo,
		enableEventsV2:      params.EnableEventsV2,
		esConfig:            params.ESConfig,
		esClient:            params.ESClient,
		blobstoreClient:     params.Blobstore,
		historyConfig:       params.HistoryConfig,
		workerConfig:        params.WorkerConfig,
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
	go c.startHistory(hosts, &startWG, c.enableEventsV2)
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
	if c.clusterNo != 0 {
		return cluster.TestAlternativeClusterFrontendAddress
	}
	return cluster.TestCurrentClusterFrontendAddress
}

func (c *cadenceImpl) FrontendPProfPort() int {
	if c.clusterNo != 0 {
		return 8105
	}
	return 7105
}

func (c *cadenceImpl) HistoryServiceAddress() []string {
	hosts := []string{}
	startPort := 7200
	if c.clusterNo != 0 {
		startPort = 8200
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
	startPort := 7300
	if c.clusterNo != 0 {
		startPort = 8300
	}
	for i := 0; i < c.historyConfig.NumHistoryHosts; i++ {
		port := startPort + i
		ports = append(ports, port)
	}

	c.logger.Info("History pprof ports", tag.Value(ports))
	return ports
}

func (c *cadenceImpl) MatchingServiceAddress() string {
	if c.clusterNo != 0 {
		return "127.0.0.1:8106"
	}
	return "127.0.0.1:7106"
}

func (c *cadenceImpl) MatchingPProfPort() int {
	if c.clusterNo != 0 {
		return 8107
	}
	return 7107
}

func (c *cadenceImpl) WorkerServiceAddress() string {
	if c.clusterNo != 0 {
		return "127.0.0.1:8108"
	}
	return "127.0.0.1:7108"
}

func (c *cadenceImpl) WorkerPProfPort() int {
	if c.clusterNo != 0 {
		return 8109
	}
	return 7109
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
	params.BlobstoreClient = c.blobstoreClient
	params.PersistenceConfig = c.persistenceConfig
	params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, c.logger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())

	// TODO when cross DC is public, remove this temporary override
	var kafkaProducer messaging.Producer
	var err error
	if c.workerConfig.EnableReplicator {
		kafkaProducer, err = c.messagingClient.NewProducerWithClusterName(c.clusterMetadata.GetCurrentClusterName())
		if err != nil {
			c.logger.Fatal("Failed to create kafka producer when start frontend", tag.Error(err))
		}
	} else {
		kafkaProducer = &mocks.KafkaProducer{}
		kafkaProducer.(*mocks.KafkaProducer).On("Publish", mock.Anything).Return(nil)
	}

	c.initLock.Lock()
	c.frontEndService = service.New(params)
	c.adminHandler = frontend.NewAdminHandler(
		c.frontEndService, c.historyConfig.NumHistoryShards, c.metadataMgr, c.historyMgr, c.historyV2Mgr)
	dc := dynamicconfig.NewCollection(params.DynamicConfig, c.logger)
	frontendConfig := frontend.NewConfig(dc, c.historyConfig.NumHistoryShards, c.workerConfig.EnableIndexer, true)
	c.frontendHandler = frontend.NewWorkflowHandler(
		c.frontEndService, frontendConfig, c.metadataMgr, c.historyMgr, c.historyV2Mgr,
		c.visibilityMgr, kafkaProducer, params.BlobstoreClient)
	err = c.frontendHandler.Start()
	if err != nil {
		c.logger.Fatal("Failed to start frontend", tag.Error(err))
	}
	dcRedirectionHandler := frontend.NewDCRedirectionHandler(c.frontendHandler, params.DCRedirectionPolicy)
	c.frontEndService.GetDispatcher().Register(workflowserviceserver.New(dcRedirectionHandler))
	err = c.adminHandler.Start()
	if err != nil {
		c.logger.Fatal("Failed to start admin", tag.Error(err))
	}
	c.initLock.Unlock()

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startHistory(hosts map[string][]string, startWG *sync.WaitGroup, enableEventsV2 bool) {

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

		c.initLock.Lock()
		service := service.New(params)
		hConfig := c.historyConfig
		historyConfig := history.NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, c.logger), hConfig.NumHistoryShards, c.workerConfig.EnableIndexer, config.StoreTypeCassandra)
		historyConfig.HistoryMgrNumConns = dynamicconfig.GetIntPropertyFn(hConfig.NumHistoryShards)
		historyConfig.ExecutionMgrNumConns = dynamicconfig.GetIntPropertyFn(hConfig.NumHistoryShards)
		historyConfig.EnableEventsV2 = dynamicconfig.GetBoolPropertyFnFilteredByDomain(enableEventsV2)
		if hConfig.HistoryCountLimitWarn != 0 {
			historyConfig.HistoryCountLimitWarn = dynamicconfig.GetIntPropertyFilteredByDomain(hConfig.HistoryCountLimitWarn)
		}
		if hConfig.HistoryCountLimitError != 0 {
			historyConfig.HistoryCountLimitError = dynamicconfig.GetIntPropertyFilteredByDomain(hConfig.HistoryCountLimitError)
		}
		handler := history.NewHandler(service, historyConfig, c.shardMgr, c.metadataMgr,
			c.visibilityMgr, c.historyMgr, c.historyV2Mgr, c.executionMgrFactory, params.PublicClient)
		handler.Start()
		c.initLock.Unlock()

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

	c.initLock.Lock()
	service := service.New(params)
	c.matchingHandler = matching.NewHandler(
		service, matching.NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, c.logger)), c.taskMgr, c.metadataMgr,
	)
	c.matchingHandler.Start()
	c.initLock.Unlock()

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

	dispatcher, err := params.DispatcherProvider.Get(common.FrontendServiceName, c.FrontendAddress())
	if err != nil {
		c.logger.Fatal("Failed to get dispatcher for frontend", tag.Error(err))
	}
	params.PublicClient = cwsc.New(dispatcher.ClientConfig(common.FrontendServiceName))
	c.initLock.Lock()
	service := service.New(params)
	service.Start()

	var replicatorDomainCache cache.DomainCache
	if c.workerConfig.EnableReplicator {
		metadataManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgrV2, service.GetMetricsClient(), c.logger)
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

	c.initLock.Unlock()

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
	metadataManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgrV2, service.GetMetricsClient(), c.logger)
	workerConfig := worker.NewConfig(params)
	workerConfig.ReplicationCfg.ReplicatorMessageConcurrency = dynamicconfig.GetIntPropertyFn(10)
	c.replicator = replicator.NewReplicator(
		c.clusterMetadata,
		metadataManager,
		domainCache,
		service.GetClientBean(),
		workerConfig.ReplicationCfg,
		c.messagingClient,
		c.logger,
		service.GetMetricsClient())
	if err := c.replicator.Start(); err != nil {
		c.replicator.Stop()
		c.logger.Fatal("Fail to start replicator when start worker", tag.Error(err))
	}
}

func (c *cadenceImpl) startWorkerClientWorker(params *service.BootstrapParams, service service.Service, domainCache cache.DomainCache) {
	blobstoreClient := blobstore.NewRetryableClient(
		blobstore.NewMetricClient(c.blobstoreClient, service.GetMetricsClient()),
		c.blobstoreClient.GetRetryPolicy(),
		c.blobstoreClient.IsRetryableError)
	workerConfig := worker.NewConfig(params)
	workerConfig.ArchiverConfig.ArchiverConcurrency = dynamicconfig.GetIntPropertyFn(10)
	workerConfig.ArchiverConfig.TargetArchivalBlobSize = dynamicconfig.GetIntPropertyFilteredByDomain(archivalBlobSize)
	bc := &archiver.BootstrapContainer{
		PublicClient:     params.PublicClient,
		MetricsClient:    service.GetMetricsClient(),
		Logger:           c.logger,
		ClusterMetadata:  service.GetClusterMetadata(),
		HistoryManager:   c.historyMgr,
		HistoryV2Manager: c.historyV2Mgr,
		Blobstore:        blobstoreClient,
		DomainCache:      domainCache,
		Config:           workerConfig.ArchiverConfig,
	}
	c.clientWorker = archiver.NewClientWorker(bc)
	if err := c.clientWorker.Start(); err != nil {
		c.clientWorker.Stop()
		c.logger.Fatal("Fail to start archiver when start worker", tag.Error(err))
	}
}

func (c *cadenceImpl) startWorkerIndexer(params *service.BootstrapParams, service service.Service) {
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
	if c.metadataMgrV2 == nil {
		return nil
	}
	_, err := c.metadataMgrV2.CreateDomain(&persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          uuid.New(),
			Name:        "cadence-system",
			Status:      persistence.DomainStatusRegistered,
			Description: "Cadence system domain",
		},
		Config: &persistence.DomainConfig{
			Retention:      1,
			ArchivalStatus: shared.ArchivalStatusDisabled,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
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
