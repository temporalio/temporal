// Copyright (c) 2019 Uber Technologies, Inc.
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

package resource

import (
	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go"
	sdkclient "go.temporal.io/temporal/client"

	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/client/admin"
	"github.com/temporalio/temporal/client/frontend"
	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/membership"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	persistenceClient "github.com/temporalio/temporal/common/persistence/client"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (

	// VisibilityManagerInitializer is the function each service should implement
	// for visibility manager initialization
	VisibilityManagerInitializer func(
		persistenceBean persistenceClient.Bean,
		logger log.Logger,
	) (persistence.VisibilityManager, error)

	// Impl contains all common resources shared across frontend / matching / history / worker
	Impl struct {
		status int32

		// static infos

		numShards       int
		serviceName     string
		hostName        string
		hostInfo        *membership.HostInfo
		metricsScope    tally.Scope
		clusterMetadata cluster.Metadata

		// other common resources

		domainCache       cache.DomainCache
		timeSource        clock.TimeSource
		payloadSerializer persistence.PayloadSerializer
		metricsClient     metrics.Client
		messagingClient   messaging.Client
		archivalMetadata  archiver.ArchivalMetadata
		archiverProvider  provider.ArchiverProvider

		// membership infos

		membershipMonitor       membership.Monitor
		frontendServiceResolver membership.ServiceResolver
		matchingServiceResolver membership.ServiceResolver
		historyServiceResolver  membership.ServiceResolver
		workerServiceResolver   membership.ServiceResolver

		// internal services clients

		sdkClient         sdkclient.Client
		frontendRawClient frontend.Client
		frontendClient    frontend.Client
		matchingRawClient matching.Client
		matchingClient    matching.Client
		historyRawClient  history.Client
		historyClient     history.Client
		clientBean        client.Bean

		// persistence clients

		persistenceBean persistenceClient.Bean
		visibilityMgr   persistence.VisibilityManager

		// loggers

		logger          log.Logger
		throttledLogger log.Logger

		// for registering handlers
		grpcListener net.Listener

		// for ringpop listener
		ringpopChannel *tchannel.Channel

		// internal vars
		runtimeMetricsReporter *metrics.RuntimeMetricsReporter
		rpcFactory             common.RPCFactory
	}
)

var _ Resource = (*Impl)(nil)

// New create a new resource containing common dependencies
func New(
	params *BootstrapParams,
	serviceName string,
	persistenceMaxQPS dynamicconfig.IntPropertyFn,
	throttledLoggerMaxRPS dynamicconfig.IntPropertyFn,
	visibilityManagerInitializer VisibilityManagerInitializer,
) (impl *Impl, retError error) {

	logger := params.Logger.WithTags(tag.Service(serviceName))
	throttledLogger := loggerimpl.NewThrottledLogger(logger, throttledLoggerMaxRPS)

	numShards := params.PersistenceConfig.NumHistoryShards
	hostName, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	grpcListener := params.RPCFactory.GetGRPCListener()

	ringpopChannel := params.RPCFactory.GetRingpopChannel()

	persistenceBean, err := persistenceClient.NewBeanFromFactory(persistenceClient.NewFactory(
		&params.PersistenceConfig,
		persistenceMaxQPS,
		params.AbstractDatastoreFactory,
		params.ClusterMetadata.GetCurrentClusterName(),
		params.MetricsClient,
		logger,
	))
	if err != nil {
		return nil, err
	}

	membershipFactory, err := params.MembershipFactoryInitializer(persistenceBean, logger)
	if err != nil {
		return nil, err
	}

	membershipMonitor, err := membershipFactory.GetMembershipMonitor()
	if err != nil {
		return nil, err
	}

	dynamicCollection := dynamicconfig.NewCollection(params.DynamicConfig, logger)
	clientBean, err := client.NewClientBean(
		client.NewRPCClientFactory(
			params.RPCFactory,
			membershipMonitor,
			params.MetricsClient,
			dynamicCollection,
			numShards,
			logger,
		),
		params.ClusterMetadata,
	)
	if err != nil {
		return nil, err
	}

	frontendServiceResolver, err := membershipMonitor.GetResolver(common.FrontendServiceName)
	if err != nil {
		return nil, err
	}

	matchingServiceResolver, err := membershipMonitor.GetResolver(common.MatchingServiceName)
	if err != nil {
		return nil, err
	}

	historyServiceResolver, err := membershipMonitor.GetResolver(common.HistoryServiceName)
	if err != nil {
		return nil, err
	}

	workerServiceResolver, err := membershipMonitor.GetResolver(common.WorkerServiceName)
	if err != nil {
		return nil, err
	}

	visibilityMgr, err := visibilityManagerInitializer(
		persistenceBean,
		logger,
	)
	if err != nil {
		return nil, err
	}

	domainCache := cache.NewDomainCache(
		persistenceBean.GetMetadataManager(),
		params.ClusterMetadata,
		params.MetricsClient,
		logger,
	)

	frontendRawClient := clientBean.GetFrontendClient()
	frontendClient := frontend.NewRetryableClient(
		frontendRawClient,
		common.CreateFrontendServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)

	matchingRawClient, err := clientBean.GetMatchingClient(domainCache.GetDomainName)
	if err != nil {
		return nil, err
	}
	matchingClient := matching.NewRetryableClient(
		matchingRawClient,
		common.CreateMatchingServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)

	historyRawClient := clientBean.GetHistoryClient()
	historyClient := history.NewRetryableClient(
		historyRawClient,
		common.CreateHistoryServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)

	historyArchiverBootstrapContainer := &archiver.HistoryBootstrapContainer{
		HistoryV2Manager: persistenceBean.GetHistoryManager(),
		Logger:           logger,
		MetricsClient:    params.MetricsClient,
		ClusterMetadata:  params.ClusterMetadata,
		DomainCache:      domainCache,
	}
	visibilityArchiverBootstrapContainer := &archiver.VisibilityBootstrapContainer{
		Logger:          logger,
		MetricsClient:   params.MetricsClient,
		ClusterMetadata: params.ClusterMetadata,
		DomainCache:     domainCache,
	}
	if err := params.ArchiverProvider.RegisterBootstrapContainer(
		serviceName,
		historyArchiverBootstrapContainer,
		visibilityArchiverBootstrapContainer,
	); err != nil {
		return nil, err
	}

	impl = &Impl{
		status: common.DaemonStatusInitialized,

		// static infos

		numShards:       numShards,
		serviceName:     params.Name,
		hostName:        hostName,
		metricsScope:    params.MetricScope,
		clusterMetadata: params.ClusterMetadata,

		// other common resources

		domainCache:       domainCache,
		timeSource:        clock.NewRealTimeSource(),
		payloadSerializer: persistence.NewPayloadSerializer(),
		metricsClient:     params.MetricsClient,
		messagingClient:   params.MessagingClient,
		archivalMetadata:  params.ArchivalMetadata,
		archiverProvider:  params.ArchiverProvider,

		// membership infos

		membershipMonitor:       membershipMonitor,
		frontendServiceResolver: frontendServiceResolver,
		matchingServiceResolver: matchingServiceResolver,
		historyServiceResolver:  historyServiceResolver,
		workerServiceResolver:   workerServiceResolver,

		// internal services clients

		sdkClient:         params.PublicClient,
		frontendRawClient: frontendRawClient,
		frontendClient:    frontendClient,
		matchingRawClient: matchingRawClient,
		matchingClient:    matchingClient,
		historyRawClient:  historyRawClient,
		historyClient:     historyClient,
		clientBean:        clientBean,

		// persistence clients

		persistenceBean: persistenceBean,
		visibilityMgr:   visibilityMgr,

		// loggers

		logger:          logger,
		throttledLogger: throttledLogger,

		// for registering grpc handlers
		grpcListener: grpcListener,

		// for ringpop listener
		ringpopChannel: ringpopChannel,

		// internal vars
		runtimeMetricsReporter: metrics.NewRuntimeMetricsReporter(
			params.MetricScope,
			time.Minute,
			logger,
			params.InstanceID,
		),
		rpcFactory: params.RPCFactory,
	}
	return impl, nil
}

// Start start all resources
func (h *Impl) Start() {

	if !atomic.CompareAndSwapInt32(
		&h.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	h.metricsScope.Counter(metrics.RestartCount).Inc(1)
	h.runtimeMetricsReporter.Start()

	h.membershipMonitor.Start()
	h.domainCache.Start()

	hostInfo, err := h.membershipMonitor.WhoAmI()
	if err != nil {
		h.logger.WithTags(tag.Error(err)).Fatal("fail to get host info from membership monitor")
	}
	h.hostInfo = hostInfo

	// The service is now started up
	h.logger.Info("Service resources started", tag.Address(hostInfo.GetAddress()))
	// seed the random generator once for this service
	rand.Seed(time.Now().UTC().UnixNano())
}

// Stop stops all resources
func (h *Impl) Stop() {

	if !atomic.CompareAndSwapInt32(
		&h.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	h.domainCache.Stop()
	h.membershipMonitor.Stop()
	h.ringpopChannel.Close()
	if err := h.grpcListener.Close(); err != nil {
		h.logger.WithTags(tag.Error(err)).Error("failed to close gRPC listener")
	}
	h.runtimeMetricsReporter.Stop()
	h.persistenceBean.Close()
	h.visibilityMgr.Close()
}

// GetServiceName return service name
func (h *Impl) GetServiceName() string {
	return h.serviceName
}

// GetHostName return host name
func (h *Impl) GetHostName() string {
	return h.hostName
}

// GetHostInfo return host info
func (h *Impl) GetHostInfo() *membership.HostInfo {
	return h.hostInfo
}

// GetClusterMetadata return cluster metadata
func (h *Impl) GetClusterMetadata() cluster.Metadata {
	return h.clusterMetadata
}

// other common resources

// GetDomainCache return domain cache
func (h *Impl) GetDomainCache() cache.DomainCache {
	return h.domainCache
}

// GetTimeSource return time source
func (h *Impl) GetTimeSource() clock.TimeSource {
	return h.timeSource
}

// GetPayloadSerializer return binary payload serializer
func (h *Impl) GetPayloadSerializer() persistence.PayloadSerializer {
	return h.payloadSerializer
}

// GetMetricsClient return metrics client
func (h *Impl) GetMetricsClient() metrics.Client {
	return h.metricsClient
}

// GetMessagingClient return messaging client
func (h *Impl) GetMessagingClient() messaging.Client {
	return h.messagingClient
}

// GetArchivalMetadata return archival metadata
func (h *Impl) GetArchivalMetadata() archiver.ArchivalMetadata {
	return h.archivalMetadata
}

// GetArchiverProvider return archival provider
func (h *Impl) GetArchiverProvider() provider.ArchiverProvider {
	return h.archiverProvider
}

// membership infos

// GetMembershipMonitor return the membership monitor
func (h *Impl) GetMembershipMonitor() membership.Monitor {
	return h.membershipMonitor
}

// GetFrontendServiceResolver return frontend service resolver
func (h *Impl) GetFrontendServiceResolver() membership.ServiceResolver {
	return h.frontendServiceResolver
}

// GetMatchingServiceResolver return matching service resolver
func (h *Impl) GetMatchingServiceResolver() membership.ServiceResolver {
	return h.matchingServiceResolver
}

// GetHistoryServiceResolver return history service resolver
func (h *Impl) GetHistoryServiceResolver() membership.ServiceResolver {
	return h.historyServiceResolver
}

// GetWorkerServiceResolver return worker service resolver
func (h *Impl) GetWorkerServiceResolver() membership.ServiceResolver {
	return h.workerServiceResolver
}

// internal services clients

// GetSDKClient return sdk client
func (h *Impl) GetSDKClient() sdkclient.Client {
	return h.sdkClient
}

// GetFrontendRawClient return frontend client without retry policy
func (h *Impl) GetFrontendRawClient() frontend.Client {
	return h.frontendRawClient
}

// GetFrontendClient return frontend client with retry policy
func (h *Impl) GetFrontendClient() frontend.Client {
	return h.frontendClient
}

// GetMatchingRawClient return matching client without retry policy
func (h *Impl) GetMatchingRawClient() matching.Client {
	return h.matchingRawClient
}

// GetMatchingClient return matching client with retry policy
func (h *Impl) GetMatchingClient() matching.Client {
	return h.matchingClient
}

// GetHistoryRawClient return history client without retry policy
func (h *Impl) GetHistoryRawClient() history.Client {
	return h.historyRawClient
}

// GetHistoryClient return history client with retry policy
func (h *Impl) GetHistoryClient() history.Client {
	return h.historyClient
}

// GetRemoteAdminClient return remote admin client for given cluster name
func (h *Impl) GetRemoteAdminClient(
	cluster string,
) admin.Client {

	return h.clientBean.GetRemoteAdminClient(cluster)
}

// GetRemoteFrontendClient return remote frontend client for given cluster name
func (h *Impl) GetRemoteFrontendClient(
	cluster string,
) frontend.Client {

	return h.clientBean.GetRemoteFrontendClient(cluster)
}

// GetClientBean return RPC client bean
func (h *Impl) GetClientBean() client.Bean {
	return h.clientBean
}

// persistence clients

// GetMetadataManager return metadata manager
func (h *Impl) GetMetadataManager() persistence.MetadataManager {
	return h.persistenceBean.GetMetadataManager()
}

// GetClusterMetadataManager return metadata manager
func (h *Impl) GetClusterMetadataManager() persistence.ClusterMetadataManager {
	return h.persistenceBean.GetClusterMetadataManager()
}

// GetTaskManager return task manager
func (h *Impl) GetTaskManager() persistence.TaskManager {
	return h.persistenceBean.GetTaskManager()
}

// GetVisibilityManager return visibility manager
func (h *Impl) GetVisibilityManager() persistence.VisibilityManager {
	return h.visibilityMgr
}

// GetDomainReplicationQueue return domain replication queue
func (h *Impl) GetDomainReplicationQueue() persistence.DomainReplicationQueue {
	return h.persistenceBean.GetDomainReplicationQueue()
}

// GetShardManager return shard manager
func (h *Impl) GetShardManager() persistence.ShardManager {
	return h.persistenceBean.GetShardManager()
}

// GetHistoryManager return history manager
func (h *Impl) GetHistoryManager() persistence.HistoryManager {
	return h.persistenceBean.GetHistoryManager()
}

// GetExecutionManager return execution manager for given shard ID
func (h *Impl) GetExecutionManager(
	shardID int,
) (persistence.ExecutionManager, error) {

	return h.persistenceBean.GetExecutionManager(shardID)
}

// GetPersistenceBean return persistence bean
func (h *Impl) GetPersistenceBean() persistenceClient.Bean {
	return h.persistenceBean
}

// loggers

// GetLogger return logger
func (h *Impl) GetLogger() log.Logger {
	return h.logger
}

// GetThrottledLogger return throttled logger
func (h *Impl) GetThrottledLogger() log.Logger {
	return h.throttledLogger
}

// GetGRPCListener return GRPC listener, used for registering handlers
func (h *Impl) GetGRPCListener() net.Listener {
	return h.grpcListener
}
