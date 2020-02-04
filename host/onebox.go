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
	"encoding/json"
	"fmt"
	"net"
	"sync"

	adminClient "github.com/temporalio/temporal/client/admin"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"go.temporal.io/temporal-proto/workflowservice"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	yarpcgrpc "go.uber.org/yarpc/transport/grpc"
	"go.uber.org/yarpc/transport/tchannel"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/go/history/historyserviceclient"
	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/common"
	carchiver "github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/authorization"
	"github.com/temporalio/temporal/common/cache"
	cc "github.com/temporalio/temporal/common/client"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/elasticsearch"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service"
	"github.com/temporalio/temporal/common/service/config"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
	"github.com/temporalio/temporal/service/frontend"
	"github.com/temporalio/temporal/service/history"
	"github.com/temporalio/temporal/service/matching"
	"github.com/temporalio/temporal/service/worker"
	"github.com/temporalio/temporal/service/worker/archiver"
	"github.com/temporalio/temporal/service/worker/indexer"
	"github.com/temporalio/temporal/service/worker/replicator"
)

// Cadence hosts all of cadence services in one process
type Cadence interface {
	Start() error
	Stop()
	GetAdminClient() adminservice.AdminServiceClient
	GetFrontendClient() workflowservice.WorkflowServiceClient
	FrontendAddress() string
	GetHistoryClient() historyserviceclient.Interface
	GetExecutionManagerFactory() persistence.ExecutionManagerFactory
}

type (
	cadenceImpl struct {
		frontendService common.Daemon
		matchingService common.Daemon
		historyServices []common.Daemon

		adminClient            adminservice.AdminServiceClient
		frontendClient         workflowservice.WorkflowServiceClient
		historyClient          historyserviceclient.Interface
		logger                 log.Logger
		clusterMetadata        cluster.Metadata
		persistenceConfig      config.Persistence
		dispatcherProvider     client.DispatcherProvider
		messagingClient        messaging.Client
		metadataMgr            persistence.MetadataManager
		shardMgr               persistence.ShardManager
		historyV2Mgr           persistence.HistoryManager
		taskMgr                persistence.TaskManager
		visibilityMgr          persistence.VisibilityManager
		executionMgrFactory    persistence.ExecutionManagerFactory
		domainReplicationQueue persistence.DomainReplicationQueue
		shutdownCh             chan struct{}
		shutdownWG             sync.WaitGroup
		clusterNo              int // cluster number
		replicator             *replicator.Replicator
		clientWorker           archiver.ClientWorker
		indexer                *indexer.Indexer
		enableNDC              bool
		archiverMetadata       carchiver.ArchivalMetadata
		archiverProvider       provider.ArchiverProvider
		historyConfig          *HistoryConfig
		esConfig               *elasticsearch.Config
		esClient               elasticsearch.Client
		workerConfig           *WorkerConfig
		mockAdminClient        map[string]adminClient.Client
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
		HistoryV2Mgr                  persistence.HistoryManager
		ExecutionMgrFactory           persistence.ExecutionManagerFactory
		TaskMgr                       persistence.TaskManager
		VisibilityMgr                 persistence.VisibilityManager
		domainReplicationQueue        persistence.DomainReplicationQueue
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
		MockAdminClient               map[string]adminClient.Client
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
		domainReplicationQueue: params.domainReplicationQueue,
		shutdownCh:             make(chan struct{}),
		clusterNo:              params.ClusterNo,
		enableNDC:              params.EnableNDC,
		esConfig:               params.ESConfig,
		esClient:               params.ESClient,
		archiverMetadata:       params.ArchiverMetadata,
		archiverProvider:       params.ArchiverProvider,
		historyConfig:          params.HistoryConfig,
		workerConfig:           params.WorkerConfig,
		mockAdminClient:        params.MockAdminClient,
	}
}

func (c *cadenceImpl) enableWorker() bool {
	return c.workerConfig.EnableArchiver || c.workerConfig.EnableIndexer || c.workerConfig.EnableReplicator
}

func (c *cadenceImpl) Start() error {
	hosts := make(map[string][]string)
	hosts[common.FrontendServiceName] = []string{c.FrontendGRPCAddress()}
	hosts[common.MatchingServiceName] = []string{c.MatchingServiceAddress()}
	hosts[common.HistoryServiceName] = c.HistoryServiceAddress(0)
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

func (c *cadenceImpl) Stop() {
	if c.enableWorker() {
		c.shutdownWG.Add(4)
	} else {
		c.shutdownWG.Add(3)
	}
	c.frontendService.Stop()
	for _, historyService := range c.historyServices {
		historyService.Stop()
	}
	c.matchingService.Stop()
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

func (c *cadenceImpl) FrontendGRPCAddress() string {
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

func (c *cadenceImpl) FrontendRingpopAddress() string {
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

func (c *cadenceImpl) HistoryServiceAddress(penultimatePortDigit int) []string {
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

func (c *cadenceImpl) HistoryPProfPort() []int {
	var ports []int
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

func (c *cadenceImpl) MatchingGRPCServiceAddress() string {
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

func (c *cadenceImpl) MatchingServiceRingpopAddress() string {
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

func (c *cadenceImpl) WorkerGRPCServiceAddress() string {
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

func (c *cadenceImpl) WorkerServiceRingpopAddress() string {
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

func (c *cadenceImpl) GetAdminClient() adminservice.AdminServiceClient {
	return c.adminClient
}

func (c *cadenceImpl) GetFrontendClient() workflowservice.WorkflowServiceClient {
	return c.frontendClient
}

func (c *cadenceImpl) GetHistoryClient() historyserviceclient.Interface {
	return c.historyClient
}

func (c *cadenceImpl) startFrontend(hosts map[string][]string, startWG *sync.WaitGroup) {
	params := new(service.BootstrapParams)
	params.DCRedirectionPolicy = config.DCRedirectionPolicy{}
	params.Name = common.FrontendServiceName
	params.Logger = c.logger
	params.ThrottledLogger = c.logger
	params.PProfInitializer = newPProfInitializerImpl(c.logger, c.FrontendPProfPort())
	params.RPCFactory = newRPCFactoryImpl(common.FrontendServiceName, c.FrontendAddress(), c.FrontendGRPCAddress(), c.FrontendRingpopAddress(),
		c.logger)
	params.MetricScope = tally.NewTestScope(common.FrontendServiceName, make(map[string]string))
	params.MembershipFactory = newMembershipFactory(params.Name, hosts)
	params.ClusterMetadata = c.clusterMetadata
	params.DispatcherProvider = c.dispatcherProvider
	params.MessagingClient = c.messagingClient
	params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, c.logger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())
	params.ArchivalMetadata = c.archiverMetadata
	params.ArchiverProvider = c.archiverProvider
	params.ESConfig = c.esConfig
	params.ESClient = c.esClient
	params.Authorizer = authorization.NewNopAuthorizer()

	var err error
	params.PersistenceConfig, err = copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for frontend", tag.Error(err))
	}

	if c.esConfig != nil {
		esDataStoreName := "es-visibility"
		params.PersistenceConfig.AdvancedVisibilityStore = esDataStoreName
		params.PersistenceConfig.DataStores[esDataStoreName] = config.DataStore{
			ElasticSearch: c.esConfig,
		}
	}

	frontendService, err := frontend.NewService(params)
	if err != nil {
		params.Logger.Fatal("unable to start frontend service", tag.Error(err))
	}

	if c.mockAdminClient != nil {
		clientBean := frontendService.GetClientBean()
		if clientBean != nil {
			for serviceName, client := range c.mockAdminClient {
				clientBean.SetRemoteAdminClient(serviceName, client)
			}
		}
	}

	c.frontendService = frontendService
	connection := params.RPCFactory.CreateGRPCConnection(c.FrontendGRPCAddress())
	c.frontendClient = NewFrontendClient(connection)
	c.adminClient = NewAdminClient(connection)
	go frontendService.Start()

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startHistory(
	hosts map[string][]string,
	startWG *sync.WaitGroup,
) {
	pprofPorts := c.HistoryPProfPort()
	ringpopPorts := c.HistoryServiceAddress(2)
	grpcPorts := c.HistoryServiceAddress(3)
	for i, hostport := range c.HistoryServiceAddress(0) {
		params := new(service.BootstrapParams)
		params.Name = common.HistoryServiceName
		params.Logger = c.logger
		params.ThrottledLogger = c.logger
		params.PProfInitializer = newPProfInitializerImpl(c.logger, pprofPorts[i])
		params.RPCFactory = newRPCFactoryImpl(common.HistoryServiceName, hostport, grpcPorts[i], ringpopPorts[i], c.logger)
		params.MetricScope = tally.NewTestScope(common.HistoryServiceName, make(map[string]string))
		params.MembershipFactory = newMembershipFactory(params.Name, hosts)
		params.ClusterMetadata = c.clusterMetadata
		params.DispatcherProvider = c.dispatcherProvider
		params.MessagingClient = c.messagingClient
		params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, c.logger))
		integrationClient := newIntegrationConfigClient(dynamicconfig.NewNopClient())
		c.overrideHistoryDynamicConfig(integrationClient)
		params.DynamicConfig = integrationClient

		connection, err := grpc.Dial(c.FrontendGRPCAddress(), grpc.WithInsecure())
		if err != nil {
			c.logger.Fatal("Failed to create connection for history", tag.Error(err))
		}
		params.PublicClient = workflowservice.NewWorkflowServiceClient(connection)

		params.ArchivalMetadata = c.archiverMetadata
		params.ArchiverProvider = c.archiverProvider
		params.ESConfig = c.esConfig
		params.ESClient = c.esClient

		params.PersistenceConfig, err = copyPersistenceConfig(c.persistenceConfig)
		if err != nil {
			c.logger.Fatal("Failed to copy persistence config for history", tag.Error(err))
		}

		if c.esConfig != nil {
			esDataStoreName := "es-visibility"
			params.PersistenceConfig.AdvancedVisibilityStore = esDataStoreName
			params.PersistenceConfig.DataStores[esDataStoreName] = config.DataStore{
				ElasticSearch: c.esConfig,
			}
		}

		historyService, err := history.NewService(params)
		if err != nil {
			params.Logger.Fatal("unable to start history service", tag.Error(err))
		}

		if c.mockAdminClient != nil {
			clientBean := historyService.GetClientBean()
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
		c.historyClient = NewHistoryClient(historyService.GetDispatcher())
		c.historyServices = append(c.historyServices, historyService)

		go historyService.Start()
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
	params.RPCFactory = newRPCFactoryImpl(common.MatchingServiceName, c.MatchingServiceAddress(), c.MatchingGRPCServiceAddress(),
		c.MatchingServiceRingpopAddress(), c.logger)
	params.MetricScope = tally.NewTestScope(common.MatchingServiceName, make(map[string]string))
	params.MembershipFactory = newMembershipFactory(params.Name, hosts)
	params.ClusterMetadata = c.clusterMetadata
	params.DispatcherProvider = c.dispatcherProvider
	params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, c.logger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())
	params.ArchivalMetadata = c.archiverMetadata
	params.ArchiverProvider = c.archiverProvider

	var err error
	params.PersistenceConfig, err = copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for matching", tag.Error(err))
	}

	matchingService, err := matching.NewService(params)
	if err != nil {
		params.Logger.Fatal("unable to start matching service", tag.Error(err))
	}
	if c.mockAdminClient != nil {
		clientBean := matchingService.GetClientBean()
		if clientBean != nil {
			for serviceName, client := range c.mockAdminClient {
				clientBean.SetRemoteAdminClient(serviceName, client)
			}
		}
	}
	c.matchingService = matchingService
	go c.matchingService.Start()

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
	params.RPCFactory = newRPCFactoryImpl(common.WorkerServiceName, c.WorkerServiceAddress(), c.WorkerGRPCServiceAddress(),
		c.WorkerServiceRingpopAddress(), c.logger)
	params.MetricScope = tally.NewTestScope(common.WorkerServiceName, make(map[string]string))
	params.MembershipFactory = newMembershipFactory(params.Name, hosts)
	params.ClusterMetadata = c.clusterMetadata
	params.DispatcherProvider = c.dispatcherProvider
	params.MetricsClient = metrics.NewClient(params.MetricScope, service.GetMetricsServiceIdx(params.Name, c.logger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())
	params.ArchivalMetadata = c.archiverMetadata
	params.ArchiverProvider = c.archiverProvider

	var err error
	params.PersistenceConfig, err = copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for worker", tag.Error(err))
	}

	connection, err := grpc.Dial(c.FrontendGRPCAddress(), grpc.WithInsecure())
	if err != nil {
		c.logger.Fatal("Failed to create connection for worker", tag.Error(err))
	}
	params.PublicClient = workflowservice.NewWorkflowServiceClient(connection)

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
		c.domainReplicationQueue,
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

func (c *cadenceImpl) GetExecutionManagerFactory() persistence.ExecutionManagerFactory {
	return c.executionMgrFactory
}

func (c *cadenceImpl) overrideHistoryDynamicConfig(client *dynamicClient) {
	client.OverrideValue(dynamicconfig.HistoryMgrNumConns, c.historyConfig.NumHistoryShards)
	client.OverrideValue(dynamicconfig.ExecutionMgrNumConns, c.historyConfig.NumHistoryShards)
	client.OverrideValue(dynamicconfig.EnableNDC, c.enableNDC)

	if c.workerConfig.EnableIndexer {
		client.OverrideValue(dynamicconfig.AdvancedVisibilityWritingMode, common.AdvancedVisibilityWritingModeDual)
	}
	if c.historyConfig.HistoryCountLimitWarn != 0 {
		client.OverrideValue(dynamicconfig.HistoryCountLimitWarn, c.historyConfig.HistoryCountLimitWarn)
	}
	if c.historyConfig.HistoryCountLimitError != 0 {
		client.OverrideValue(dynamicconfig.HistoryCountLimitError, c.historyConfig.HistoryCountLimitError)
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

func newMembershipFactory(serviceName string, hosts map[string][]string) service.MembershipMonitorFactory {
	return &membershipFactoryImpl{
		serviceName: serviceName,
		hosts:       hosts,
	}
}

func (p *membershipFactoryImpl) GetMembershipMonitor() (membership.Monitor, error) {
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
	ch                 *tchannel.ChannelTransport
	serviceName        string
	ringpopServiceName string
	hostPort           string
	grpcHostPort       string
	ringpopHostPort    string
	logger             log.Logger

	sync.Mutex
	dispatcher        *yarpc.Dispatcher
	listener          net.Listener
	ringpopDispatcher *yarpc.Dispatcher
}

func newRPCFactoryImpl(sName, hostPort, grpcHostPort, ringpopAddress string, logger log.Logger) common.RPCFactory {
	return &rpcFactoryImpl{
		serviceName:     sName,
		hostPort:        hostPort,
		grpcHostPort:    grpcHostPort,
		ringpopHostPort: ringpopAddress,
		logger:          logger,
	}
}

func (c *rpcFactoryImpl) GetGRPCDispatcher() *yarpc.Dispatcher {
	l, err := net.Listen("tcp", c.grpcHostPort)
	if err != nil {
		c.logger.Fatal("Failed create a gRPC listener", tag.Error(err), tag.Address(c.grpcHostPort))
	}

	t := yarpcgrpc.NewTransport()

	return yarpc.NewDispatcher(yarpc.Config{
		Name:     c.serviceName,
		Inbounds: yarpc.Inbounds{t.NewInbound(l)},
		Outbounds: yarpc.Outbounds{
			c.serviceName: {Unary: t.NewSingleOutbound(c.grpcHostPort)},
		},
		InboundMiddleware: yarpc.InboundMiddleware{
			Unary: &versionMiddleware{},
		},
	})
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

func (c *rpcFactoryImpl) GetTChannelDispatcher() *yarpc.Dispatcher {
	c.Lock()
	defer c.Unlock()

	if c.dispatcher != nil {
		return c.dispatcher
	}

	c.dispatcher = c.createTChannelDispatcher(c.serviceName, c.hostPort, true)
	return c.dispatcher
}

func (c *rpcFactoryImpl) GetRingpopDispatcher() *yarpc.Dispatcher {
	c.Lock()
	defer c.Unlock()

	if c.ringpopDispatcher != nil {
		return c.ringpopDispatcher
	}

	ringpopServiceName := fmt.Sprintf("%v-ringpop", c.serviceName)
	c.ringpopDispatcher = c.createTChannelDispatcher(ringpopServiceName, c.ringpopHostPort, false)
	return c.ringpopDispatcher
}

func (c *rpcFactoryImpl) createTChannelDispatcher(serviceName string, hostPort string, createOutbound bool) *yarpc.Dispatcher {
	// Setup dispatcher for onebox
	var err error
	c.ch, err = tchannel.NewChannelTransport(
		tchannel.ServiceName(serviceName), tchannel.ListenAddr(hostPort))
	if err != nil {
		c.logger.Fatal("Failed to create transport channel", tag.Error(err))
	}

	var outbounds yarpc.Outbounds
	if createOutbound {
		outbounds = yarpc.Outbounds{
			c.serviceName: {Unary: c.ch.NewSingleOutbound(hostPort)},
		}
	}
	return yarpc.NewDispatcher(yarpc.Config{
		Name:     serviceName,
		Inbounds: yarpc.Inbounds{c.ch.NewInbound()},
		// For integration tests to generate client out of the same outbound.
		Outbounds: outbounds,
		InboundMiddleware: yarpc.InboundMiddleware{
			Unary: &versionMiddleware{},
		},
	})
}

type versionMiddleware struct {
}

func (vm *versionMiddleware) Handle(ctx context.Context, req *transport.Request, resw transport.ResponseWriter, h transport.UnaryHandler) error {
	req.Headers = req.Headers.With(common.LibraryVersionHeaderName, "1.0.0").With(common.FeatureVersionHeaderName, cc.GoWorkerConsistentQueryVersion).With(common.ClientImplHeaderName, cc.GoSDK)
	return h.Handle(ctx, req, resw)
}

func (c *rpcFactoryImpl) CreateTChannelDispatcherForOutbound(callerName, serviceName, hostName string) *yarpc.Dispatcher {
	// Setup dispatcher(outbound) for onebox
	return c.createDispatcherForOutbound(c.ch.NewSingleOutbound(hostName), callerName, serviceName, "TChannel")
}

// CreateGRPCDispatcherForOutbound creates a dispatcher for outbound connection
func (c *rpcFactoryImpl) CreateGRPCDispatcherForOutbound(callerName, serviceName, hostName string) *yarpc.Dispatcher {
	return c.createDispatcherForOutbound(yarpcgrpc.NewTransport().NewSingleOutbound(hostName), callerName, serviceName, "gRPC")
}

// CreateGRPCConnection creates connection for gRPC calls
func (c *rpcFactoryImpl) CreateGRPCConnection(hostName string) *grpc.ClientConn {
	connection, err := grpc.Dial(hostName, grpc.WithInsecure())
	if err != nil {
		c.logger.Fatal("Failed to create gRPC connection", tag.Error(err))
	}

	return connection
}

func (c *rpcFactoryImpl) createDispatcherForOutbound(unaryOutbound transport.UnaryOutbound, callerName, serviceName, transportType string) *yarpc.Dispatcher {
	d := yarpc.NewDispatcher(yarpc.Config{
		Name: callerName,
		Outbounds: yarpc.Outbounds{
			serviceName: {Unary: unaryOutbound},
		},
	})

	if err := d.Start(); err != nil {
		c.logger.Fatal("Failed to start outbound dispatcher", tag.Error(err), tag.TransportType(transportType))
	}
	return d
}
