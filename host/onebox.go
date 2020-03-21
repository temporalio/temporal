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
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"
	sdkclient "go.temporal.io/temporal/client"
	"google.golang.org/grpc"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	adminClient "github.com/temporalio/temporal/client/admin"
	"github.com/temporalio/temporal/common"
	carchiver "github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/authorization"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/domain"
	"github.com/temporalio/temporal/common/elasticsearch"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	persistenceClient "github.com/temporalio/temporal/common/persistence/client"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/rpc"
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
	GetHistoryClient() historyservice.HistoryServiceClient
	GetExecutionManagerFactory() persistence.ExecutionManagerFactory
}

type (
	cadenceImpl struct {
		frontendService common.Daemon
		matchingService common.Daemon
		historyServices []common.Daemon

		adminClient                   adminservice.AdminServiceClient
		frontendClient                workflowservice.WorkflowServiceClient
		historyClient                 historyservice.HistoryServiceClient
		logger                        log.Logger
		clusterMetadata               cluster.Metadata
		persistenceConfig             config.Persistence
		messagingClient               messaging.Client
		metadataMgr                   persistence.MetadataManager
		shardMgr                      persistence.ShardManager
		historyV2Mgr                  persistence.HistoryManager
		taskMgr                       persistence.TaskManager
		visibilityMgr                 persistence.VisibilityManager
		executionMgrFactory           persistence.ExecutionManagerFactory
		domainReplicationQueue        persistence.DomainReplicationQueue
		shutdownCh                    chan struct{}
		shutdownWG                    sync.WaitGroup
		clusterNo                     int // cluster number
		replicator                    *replicator.Replicator
		clientWorker                  archiver.ClientWorker
		indexer                       *indexer.Indexer
		enableNDC                     bool
		archiverMetadata              carchiver.ArchivalMetadata
		archiverProvider              provider.ArchiverProvider
		historyConfig                 *HistoryConfig
		esConfig                      *elasticsearch.Config
		esClient                      elasticsearch.Client
		workerConfig                  *WorkerConfig
		mockAdminClient               map[string]adminClient.Client
		domainReplicationTaskExecutor domain.ReplicationTaskExecutor
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
		MessagingClient               messaging.Client
		MetadataMgr                   persistence.MetadataManager
		ShardMgr                      persistence.ShardManager
		HistoryV2Mgr                  persistence.HistoryManager
		ExecutionMgrFactory           persistence.ExecutionManagerFactory
		TaskMgr                       persistence.TaskManager
		VisibilityMgr                 persistence.VisibilityManager
		DomainReplicationQueue        persistence.DomainReplicationQueue
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
		DomainReplicationTaskExecutor domain.ReplicationTaskExecutor
	}

	membershipFactoryImpl struct {
		serviceName string
		hosts       map[string][]string
	}
)

// NewCadence returns an instance that hosts full cadence in one process
func NewCadence(params *CadenceParams) Cadence {
	return &cadenceImpl{
		logger:                        params.Logger,
		clusterMetadata:               params.ClusterMetadata,
		persistenceConfig:             params.PersistenceConfig,
		messagingClient:               params.MessagingClient,
		metadataMgr:                   params.MetadataMgr,
		visibilityMgr:                 params.VisibilityMgr,
		shardMgr:                      params.ShardMgr,
		historyV2Mgr:                  params.HistoryV2Mgr,
		taskMgr:                       params.TaskMgr,
		executionMgrFactory:           params.ExecutionMgrFactory,
		domainReplicationQueue:        params.DomainReplicationQueue,
		shutdownCh:                    make(chan struct{}),
		clusterNo:                     params.ClusterNo,
		enableNDC:                     params.EnableNDC,
		esConfig:                      params.ESConfig,
		esClient:                      params.ESClient,
		archiverMetadata:              params.ArchiverMetadata,
		archiverProvider:              params.ArchiverProvider,
		historyConfig:                 params.HistoryConfig,
		workerConfig:                  params.WorkerConfig,
		mockAdminClient:               params.MockAdminClient,
		domainReplicationTaskExecutor: params.DomainReplicationTaskExecutor,
	}
}

func (c *cadenceImpl) enableWorker() bool {
	return c.workerConfig.EnableArchiver || c.workerConfig.EnableIndexer || c.workerConfig.EnableReplicator
}

func (c *cadenceImpl) Start() error {
	hosts := make(map[string][]string)
	hosts[common.FrontendServiceName] = []string{c.FrontendGRPCAddress()}
	hosts[common.MatchingServiceName] = []string{c.MatchingGRPCServiceAddress()}
	hosts[common.HistoryServiceName] = c.HistoryServiceAddress(3)
	if c.enableWorker() {
		hosts[common.WorkerServiceName] = []string{c.WorkerGRPCServiceAddress()}
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

// penultimatePortDigit: 2 - ringpop, 3 - gRPC
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

func (c *cadenceImpl) GetAdminClient() adminservice.AdminServiceClient {
	return c.adminClient
}

func (c *cadenceImpl) GetFrontendClient() workflowservice.WorkflowServiceClient {
	return c.frontendClient
}

func (c *cadenceImpl) GetHistoryClient() historyservice.HistoryServiceClient {
	return c.historyClient
}

func (c *cadenceImpl) startFrontend(hosts map[string][]string, startWG *sync.WaitGroup) {
	params := new(resource.BootstrapParams)
	params.DCRedirectionPolicy = config.DCRedirectionPolicy{}
	params.Name = common.FrontendServiceName
	params.Logger = c.logger
	params.ThrottledLogger = c.logger
	params.RPCFactory = newRPCFactoryImpl(common.FrontendServiceName, c.FrontendGRPCAddress(), c.FrontendRingpopAddress(),
		c.logger)
	params.MetricScope = tally.NewTestScope(common.FrontendServiceName, make(map[string]string))
	params.MembershipFactoryInitializer = func(x persistenceClient.Bean, y log.Logger) (resource.MembershipMonitorFactory, error) {
		return newMembershipFactory(params.Name, hosts), nil
	}
	params.ClusterMetadata = c.clusterMetadata
	params.MessagingClient = c.messagingClient
	params.MetricsClient = metrics.NewClient(params.MetricScope, metrics.GetMetricsServiceIdx(params.Name, c.logger))
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
	membershipPorts := c.HistoryServiceAddress(2)
	for i, grpcPort := range c.HistoryServiceAddress(3) {
		params := new(resource.BootstrapParams)
		params.Name = common.HistoryServiceName
		params.Logger = c.logger
		params.ThrottledLogger = c.logger
		params.RPCFactory = newRPCFactoryImpl(common.HistoryServiceName, grpcPort, membershipPorts[i], c.logger)
		params.MetricScope = tally.NewTestScope(common.HistoryServiceName, make(map[string]string))
		params.MembershipFactoryInitializer = func(x persistenceClient.Bean, y log.Logger) (resource.MembershipMonitorFactory, error) {
			return newMembershipFactory(params.Name, hosts), nil
		}
		params.ClusterMetadata = c.clusterMetadata
		params.MessagingClient = c.messagingClient
		params.MetricsClient = metrics.NewClient(params.MetricScope, metrics.GetMetricsServiceIdx(params.Name, c.logger))
		integrationClient := newIntegrationConfigClient(dynamicconfig.NewNopClient())
		c.overrideHistoryDynamicConfig(integrationClient)
		params.DynamicConfig = integrationClient

		var err error
		params.PublicClient, err = sdkclient.NewClient(sdkclient.Options{
			HostPort:     c.FrontendGRPCAddress(),
			DomainName:   common.SystemLocalDomainName,
			MetricsScope: params.MetricScope,
		})
		if err != nil {
			c.logger.Fatal("Failed to create client for history", tag.Error(err))
		}

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
		historyConnection, err := rpc.Dial(c.HistoryServiceAddress(3)[0])
		if err != nil {
			c.logger.Fatal("Failed to create connection for history", tag.Error(err))
		}

		c.historyClient = NewHistoryClient(historyConnection)
		c.historyServices = append(c.historyServices, historyService)

		go historyService.Start()
	}

	startWG.Done()
	<-c.shutdownCh
	c.shutdownWG.Done()
}

func (c *cadenceImpl) startMatching(hosts map[string][]string, startWG *sync.WaitGroup) {

	params := new(resource.BootstrapParams)
	params.Name = common.MatchingServiceName
	params.Logger = c.logger
	params.ThrottledLogger = c.logger
	params.RPCFactory = newRPCFactoryImpl(common.MatchingServiceName, c.MatchingGRPCServiceAddress(), c.MatchingServiceRingpopAddress(), c.logger)
	params.MetricScope = tally.NewTestScope(common.MatchingServiceName, make(map[string]string))
	params.MembershipFactoryInitializer = func(x persistenceClient.Bean, y log.Logger) (resource.MembershipMonitorFactory, error) {
		return newMembershipFactory(params.Name, hosts), nil
	}
	params.ClusterMetadata = c.clusterMetadata
	params.MetricsClient = metrics.NewClient(params.MetricScope, metrics.GetMetricsServiceIdx(params.Name, c.logger))
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
	params := new(resource.BootstrapParams)
	params.Name = common.WorkerServiceName
	params.Logger = c.logger
	params.ThrottledLogger = c.logger
	params.RPCFactory = newRPCFactoryImpl(common.WorkerServiceName, c.WorkerGRPCServiceAddress(), c.WorkerServiceRingpopAddress(), c.logger)
	params.MetricScope = tally.NewTestScope(common.WorkerServiceName, make(map[string]string))
	params.MembershipFactoryInitializer = func(x persistenceClient.Bean, y log.Logger) (resource.MembershipMonitorFactory, error) {
		return newMembershipFactory(params.Name, hosts), nil
	}
	params.ClusterMetadata = c.clusterMetadata
	params.MetricsClient = metrics.NewClient(params.MetricScope, metrics.GetMetricsServiceIdx(params.Name, c.logger))
	params.DynamicConfig = newIntegrationConfigClient(dynamicconfig.NewNopClient())
	params.ArchivalMetadata = c.archiverMetadata
	params.ArchiverProvider = c.archiverProvider

	var err error
	params.PersistenceConfig, err = copyPersistenceConfig(c.persistenceConfig)
	if err != nil {
		c.logger.Fatal("Failed to copy persistence config for worker", tag.Error(err))
	}

	params.PublicClient, err = sdkclient.NewClient(sdkclient.Options{
		HostPort:     c.FrontendGRPCAddress(),
		DomainName:   common.SystemLocalDomainName,
		MetricsScope: params.MetricScope,
	})
	if err != nil {
		c.logger.Fatal("Failed to create client for worker", tag.Error(err))
	}

	service, err := resource.New(
		params,
		common.WorkerServiceName,
		dynamicconfig.GetIntPropertyFn(5000),
		dynamicconfig.GetIntPropertyFn(10000),
		func(
			persistenceBean persistenceClient.Bean,
			logger log.Logger,
		) (persistence.VisibilityManager, error) {
			return persistenceBean.GetVisibilityManager(), nil
		},
	)
	if err != nil {
		params.Logger.Fatal("unable to create worker service", tag.Error(err))
	}

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

func (c *cadenceImpl) startWorkerReplicator(params *resource.BootstrapParams, service resource.Resource, domainCache cache.DomainCache) {
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
		c.domainReplicationTaskExecutor,
	)
	if err := c.replicator.Start(); err != nil {
		c.replicator.Stop()
		c.logger.Fatal("Fail to start replicator when start worker", tag.Error(err))
	}
}

func (c *cadenceImpl) startWorkerClientWorker(params *resource.BootstrapParams, service resource.Resource, domainCache cache.DomainCache) {
	workerConfig := worker.NewConfig(params)
	workerConfig.ArchiverConfig.ArchiverConcurrency = dynamicconfig.GetIntPropertyFn(10)

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

func (c *cadenceImpl) startWorkerIndexer(params *resource.BootstrapParams, service resource.Resource) {
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
		c.logger.Fatal("Fail to start indexer when start worker", tag.Error(err))
		c.indexer.Stop()
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
			HistoryArchivalStatus:    enums.ArchivalStatusDisabled,
			VisibilityArchivalStatus: enums.ArchivalStatusDisabled,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{},
		FailoverVersion:   common.EmptyVersion,
	})
	if err != nil {
		if _, ok := err.(*serviceerror.DomainAlreadyExists); ok {
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
	connection, err := rpc.Dial(hostName)
	if err != nil {
		c.logger.Fatal("Failed to create gRPC connection", tag.Error(err))
	}

	return connection
}
