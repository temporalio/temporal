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
	"fmt"

	"sync"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/admin/adminserviceclient"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/.gen/go/cadence/workflowserviceserver"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/logging"
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
	"github.com/uber/cadence/service/worker/replicator"
	cwsc "go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
)

const rpAppNamePrefix string = "cadence"
const maxRpJoinTimeout = 30 * time.Second

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
		initLock                sync.Mutex
		adminHandler            *frontend.AdminHandler
		frontendHandler         *frontend.WorkflowHandler
		matchingHandler         *matching.Handler
		historyHandlers         []*history.Handler
		barkLogger              bark.Logger
		logger                  log.Logger
		clusterMetadata         cluster.Metadata
		persistenceConfig       config.Persistence
		dispatcherProvider      client.DispatcherProvider
		messagingClient         messaging.Client
		metadataMgr             persistence.MetadataManager
		metadataMgrV2           persistence.MetadataManager
		shardMgr                persistence.ShardManager
		historyMgr              persistence.HistoryManager
		historyV2Mgr            persistence.HistoryV2Manager
		taskMgr                 persistence.TaskManager
		visibilityMgr           persistence.VisibilityManager
		executionMgrFactory     persistence.ExecutionManagerFactory
		shutdownCh              chan struct{}
		shutdownWG              sync.WaitGroup
		frontEndService         service.Service
		clusterNo               int // cluster number
		replicator              *replicator.Replicator
		clientWorker            archiver.ClientWorker
		enableWorkerService     bool // tmp flag used to tell if onebox should create worker service
		enableEventsV2          bool
		enableVisibilityToKafka bool
		blobstoreClient         blobstore.Client
		historyConfig           *HistoryConfig
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
		BarkLogger                    bark.Logger
		Logger                        log.Logger
		ClusterNo                     int
		EnableWorker                  bool
		EnableEventsV2                bool
		EnableVisibilityToKafka       bool
		Blobstore                     blobstore.Client
		EnableReadHistoryFromArchival bool
		HistoryConfig                 *HistoryConfig
	}

	membershipFactoryImpl struct {
		serviceName string
		hosts       map[string][]string
	}
)

// NewCadence returns an instance that hosts full cadence in one process
func NewCadence(params *CadenceParams) Cadence {
	return &cadenceImpl{
		barkLogger:              params.BarkLogger,
		logger:                  params.Logger,
		clusterMetadata:         params.ClusterMetadata,
		persistenceConfig:       params.PersistenceConfig,
		dispatcherProvider:      params.DispatcherProvider,
		messagingClient:         params.MessagingClient,
		metadataMgr:             params.MetadataMgr,
		metadataMgrV2:           params.MetadataMgrV2,
		visibilityMgr:           params.VisibilityMgr,
		shardMgr:                params.ShardMgr,
		historyMgr:              params.HistoryMgr,
		historyV2Mgr:            params.HistoryV2Mgr,
		taskMgr:                 params.TaskMgr,
		executionMgrFactory:     params.ExecutionMgrFactory,
		shutdownCh:              make(chan struct{}),
		clusterNo:               params.ClusterNo,
		enableWorkerService:     params.EnableWorker,
		enableEventsV2:          params.EnableEventsV2,
		enableVisibilityToKafka: params.EnableVisibilityToKafka,
		blobstoreClient:         params.Blobstore,
		historyConfig:           params.HistoryConfig,
	}
}

func (c *cadenceImpl) enableWorker() bool {
	return c.enableWorkerService
}

func (c *cadenceImpl) Start() error {
	hosts := make(map[string][]string)
	hosts[common.FrontendServiceName] = []string{c.FrontendAddress()}
	hosts[common.MatchingServiceName] = []string{c.MatchingServiceAddress()}
	hosts[common.HistoryServiceName] = c.HistoryServiceAddress()
	if c.enableWorker() {
		hosts[common.WorkerServiceName] = []string{c.WorkerServiceAddress()}
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
	if c.enableWorker() {
		c.replicator.Stop()
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

	c.barkLogger.Infof("History hosts: %v", hosts)
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

	c.barkLogger.Infof("History pprof ports: %v", ports)
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
	params.BarkLogger = c.barkLogger
	params.Logger = c.logger
	params.ThrottledBarkLogger = logging.NewThrottledLogger(c.barkLogger, func(...dynamicconfig.FilterOption) int { return 10 })
	params.PProfInitializer = newPProfInitializerImpl(c.barkLogger, c.FrontendPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.FrontendServiceName, c.FrontendAddress(), c.barkLogger)
	params.MetricScope = tally.NewTestScope(common.FrontendServiceName, make(map[string]string))
	params.MembershipFactory = newMembershipFactory(params.Name, hosts)
	params.ClusterMetadata = c.clusterMetadata
	params.DispatcherProvider = c.dispatcherProvider
	params.MessagingClient = c.messagingClient
	params.BlobstoreClient = c.blobstoreClient
	params.PersistenceConfig = c.persistenceConfig
	params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, params.BarkLogger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())

	// TODO when cross DC is public, remove this temporary override
	var kafkaProducer messaging.Producer
	var err error
	if c.enableWorker() {
		kafkaProducer, err = c.messagingClient.NewProducerWithClusterName(c.clusterMetadata.GetCurrentClusterName())
		if err != nil {
			c.barkLogger.WithField("error", err).Fatal("Failed to create kafka producer when start frontend")
		}
	} else {
		kafkaProducer = &mocks.KafkaProducer{}
		kafkaProducer.(*mocks.KafkaProducer).On("Publish", mock.Anything).Return(nil)
	}

	c.initLock.Lock()
	c.frontEndService = service.New(params)
	c.adminHandler = frontend.NewAdminHandler(
		c.frontEndService, c.historyConfig.NumHistoryShards, c.metadataMgr, c.historyMgr, c.historyV2Mgr)
	frontendConfig := frontend.NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, c.barkLogger), c.historyConfig.NumHistoryShards, false)
	c.frontendHandler = frontend.NewWorkflowHandler(
		c.frontEndService, frontendConfig, c.metadataMgr, c.historyMgr, c.historyV2Mgr,
		c.visibilityMgr, kafkaProducer, params.BlobstoreClient)
	err = c.frontendHandler.Start()
	if err != nil {
		c.barkLogger.WithField("error", err).Fatal("Failed to start frontend")
	}
	dcRedirectionHandler := frontend.NewDCRedirectionHandler(c.frontendHandler, params.DCRedirectionPolicy)
	c.frontEndService.GetDispatcher().Register(workflowserviceserver.New(dcRedirectionHandler))
	err = c.adminHandler.Start()
	if err != nil {
		c.barkLogger.WithField("error", err).Fatal("Failed to start admin")
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
		params.BarkLogger = c.barkLogger
		params.Logger = c.logger
		params.ThrottledBarkLogger = logging.NewThrottledLogger(c.barkLogger, func(...dynamicconfig.FilterOption) int { return 10 })
		params.PProfInitializer = newPProfInitializerImpl(c.barkLogger, pprofPorts[i])
		params.RPCFactory = newRPCFactoryImpl(common.HistoryServiceName, hostport, c.barkLogger)
		params.MetricScope = tally.NewTestScope(common.HistoryServiceName, make(map[string]string))
		params.MembershipFactory = newMembershipFactory(params.Name, hosts)
		params.ClusterMetadata = c.clusterMetadata
		params.DispatcherProvider = c.dispatcherProvider
		params.MessagingClient = c.messagingClient
		params.PersistenceConfig = c.persistenceConfig
		params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, params.BarkLogger))
		params.DynamicConfig = dynamicconfig.NewNopClient()
		dispatcher, err := params.DispatcherProvider.Get(common.FrontendServiceName, c.FrontendAddress())
		if err != nil {
			c.barkLogger.WithField("error", err).Fatal("Failed to get dispatcher for frontend")
		}
		params.PublicClient = cwsc.New(dispatcher.ClientConfig(common.FrontendServiceName))

		c.initLock.Lock()
		service := service.New(params)
		hConfig := c.historyConfig
		historyConfig := history.NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, c.barkLogger), hConfig.NumHistoryShards, c.enableVisibilityToKafka, config.StoreTypeCassandra)
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
	params.BarkLogger = c.barkLogger
	params.Logger = c.logger
	params.ThrottledBarkLogger = logging.NewThrottledLogger(c.barkLogger, func(...dynamicconfig.FilterOption) int { return 10 })
	params.PProfInitializer = newPProfInitializerImpl(c.barkLogger, c.MatchingPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.MatchingServiceName, c.MatchingServiceAddress(), c.barkLogger)
	params.MetricScope = tally.NewTestScope(common.MatchingServiceName, make(map[string]string))
	params.MembershipFactory = newMembershipFactory(params.Name, hosts)
	params.ClusterMetadata = c.clusterMetadata
	params.DispatcherProvider = c.dispatcherProvider
	params.PersistenceConfig = c.persistenceConfig
	params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, params.BarkLogger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())

	c.initLock.Lock()
	service := service.New(params)
	c.matchingHandler = matching.NewHandler(
		service, matching.NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, c.barkLogger)), c.taskMgr, c.metadataMgr,
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
	params.BarkLogger = c.barkLogger
	params.Logger = c.logger
	params.PProfInitializer = newPProfInitializerImpl(c.barkLogger, c.WorkerPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.WorkerServiceName, c.WorkerServiceAddress(), c.barkLogger)
	params.MetricScope = tally.NewTestScope(common.WorkerServiceName, make(map[string]string))
	params.MembershipFactory = newMembershipFactory(params.Name, hosts)
	params.ClusterMetadata = c.clusterMetadata
	params.DispatcherProvider = c.dispatcherProvider
	params.PersistenceConfig = c.persistenceConfig
	params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, params.BarkLogger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())

	dispatcher, err := params.DispatcherProvider.Get(common.FrontendServiceName, c.FrontendAddress())
	if err != nil {
		c.barkLogger.WithField("error", err).Fatal("Failed to get dispatcher for frontend")
	}
	params.PublicClient = cwsc.New(dispatcher.ClientConfig(common.FrontendServiceName))
	c.initLock.Lock()
	service := service.New(params)
	service.Start()

	metadataManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgrV2, service.GetMetricsClient(), c.barkLogger)
	replicatorDomainCache := cache.NewDomainCache(metadataManager, params.ClusterMetadata, service.GetMetricsClient(), service.GetBarkLogger())
	replicatorDomainCache.Start()
	c.startWorkerReplicator(params, service, replicatorDomainCache)

	metadataProxyManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgr, service.GetMetricsClient(), c.barkLogger)
	clientWorkerDomainCache := cache.NewDomainCache(metadataProxyManager, params.ClusterMetadata, service.GetMetricsClient(), service.GetBarkLogger())
	clientWorkerDomainCache.Start()
	c.startWorkerClientWorker(params, service, clientWorkerDomainCache)
	c.initLock.Unlock()

	startWG.Done()
	<-c.shutdownCh
	replicatorDomainCache.Stop()
	clientWorkerDomainCache.Stop()
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startWorkerReplicator(params *service.BootstrapParams, service service.Service, domainCache cache.DomainCache) {
	metadataManager := persistence.NewMetadataPersistenceMetricsClient(c.metadataMgrV2, service.GetMetricsClient(), c.barkLogger)
	workerConfig := worker.NewConfig(params)
	workerConfig.ReplicationCfg.ReplicatorMessageConcurrency = dynamicconfig.GetIntPropertyFn(10)
	c.replicator = replicator.NewReplicator(
		c.clusterMetadata,
		metadataManager,
		domainCache,
		service.GetClientBean(),
		workerConfig.ReplicationCfg,
		c.messagingClient,
		c.barkLogger,
		service.GetMetricsClient())
	if err := c.replicator.Start(); err != nil {
		c.replicator.Stop()
		c.barkLogger.WithField("error", err).Fatal("Fail to start replicator when start worker")
	}
}

func (c *cadenceImpl) startWorkerClientWorker(params *service.BootstrapParams, service service.Service, domainCache cache.DomainCache) {
	blobstoreClient := blobstore.NewRetryableClient(
		blobstore.NewMetricClient(c.blobstoreClient, service.GetMetricsClient()),
		c.blobstoreClient.GetRetryPolicy(),
		c.blobstoreClient.IsRetryableError)
	workerConfig := worker.NewConfig(params)
	workerConfig.ArchiverConfig.ArchiverConcurrency = dynamicconfig.GetIntPropertyFn(10)
	bc := &archiver.BootstrapContainer{
		PublicClient:     params.PublicClient,
		MetricsClient:    service.GetMetricsClient(),
		Logger:           c.barkLogger,
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
		c.barkLogger.WithField("error", err).Fatal("Fail to start archiver when start worker")
	}
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

func newPProfInitializerImpl(logger bark.Logger, port int) common.PProfInitializer {
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
	logger      bark.Logger
}

func newRPCFactoryImpl(sName string, hostPort string, logger bark.Logger) common.RPCFactory {
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
		c.logger.WithField("error", err).Fatal("Failed to create transport channel")
	}
	return yarpc.NewDispatcher(yarpc.Config{
		Name:     c.serviceName,
		Inbounds: yarpc.Inbounds{c.ch.NewInbound()},
		// For integration tests to generate client out of the same outbound.
		Outbounds: yarpc.Outbounds{
			c.serviceName: {Unary: c.ch.NewSingleOutbound(c.hostPort)},
		},
	})
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
		c.logger.WithField("error", err).Fatal("Failed to create outbound transport channel")
	}
	return d
}
