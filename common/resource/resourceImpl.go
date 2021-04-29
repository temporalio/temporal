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

package resource

import (
	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
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

		numShards       int32
		serviceName     string
		hostName        string
		hostInfo        *membership.HostInfo
		metricsScope    tally.Scope
		clusterMetadata cluster.Metadata

		// other common resources

		namespaceCache    cache.NamespaceCache
		timeSource        clock.TimeSource
		payloadSerializer persistence.PayloadSerializer
		metricsClient     metrics.Client
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
		frontendRawClient workflowservice.WorkflowServiceClient
		frontendClient    workflowservice.WorkflowServiceClient
		matchingRawClient matchingservice.MatchingServiceClient
		matchingClient    matchingservice.MatchingServiceClient
		historyRawClient  historyservice.HistoryServiceClient
		historyClient     historyservice.HistoryServiceClient
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
	persistenceGlobalMaxQPS dynamicconfig.IntPropertyFn,
	throttledLoggerMaxRPS dynamicconfig.IntPropertyFn,
	visibilityManagerInitializer VisibilityManagerInitializer,
) (impl *Impl, retError error) {

	logger := log.With(params.Logger, tag.Service(serviceName))
	throttledLogger := log.NewThrottledLogger(logger,
		func() float64 { return float64(throttledLoggerMaxRPS()) })

	numShards := params.PersistenceConfig.NumHistoryShards
	hostName, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	grpcListener := params.RPCFactory.GetGRPCListener()

	ringpopChannel := params.RPCFactory.GetRingpopChannel()

	persistenceBean, err := persistenceClient.NewBeanFromFactory(persistenceClient.NewFactory(
		&params.PersistenceConfig,
		params.PersistenceServiceResolver,
		func(...dynamicconfig.FilterOption) int {
			if persistenceGlobalMaxQPS() > 0 {
				// TODO: We have a bootstrap issue to correctly find memberCount.  Membership relies on
				// persistence to bootstrap membership ring, so we cannot have persistence rely on membership
				// as it will cause circular dependency.
				// ringSize, err := membershipMonitor.GetMemberCount(serviceName)
				// if err == nil && ringSize > 0 {
				// 	avgQuota := common.MaxInt(persistenceGlobalMaxQPS()/ringSize, 1)
				// 	return common.MinInt(avgQuota, persistenceMaxQPS())
				// }
			}
			return persistenceMaxQPS()
		},
		params.AbstractDatastoreFactory,
		params.ClusterMetadataConfig.CurrentClusterName,
		params.MetricsClient,
		logger,
	))
	if err != nil {
		return nil, err
	}

	clusterMetadata := cluster.NewMetadata(
		params.ClusterMetadataConfig.EnableGlobalNamespace,
		params.ClusterMetadataConfig.FailoverVersionIncrement,
		params.ClusterMetadataConfig.MasterClusterName,
		params.ClusterMetadataConfig.CurrentClusterName,
		params.ClusterMetadataConfig.ClusterInformation,
	)

	membershipFactory, err := params.MembershipFactoryInitializer(persistenceBean, logger)
	if err != nil {
		return nil, err
	}

	membershipMonitor, err := membershipFactory.GetMembershipMonitor()
	if err != nil {
		return nil, err
	}

	dynamicCollection := dynamicconfig.NewCollection(params.DynamicConfigClient, logger)
	clientBean, err := client.NewClientBean(
		client.NewRPCClientFactory(
			params.RPCFactory,
			membershipMonitor,
			params.MetricsClient,
			dynamicCollection,
			numShards,
			logger,
		),
		clusterMetadata,
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

	namespaceCache := cache.NewNamespaceCache(
		persistenceBean.GetMetadataManager(),
		clusterMetadata,
		params.MetricsClient,
		logger,
	)

	frontendRawClient := clientBean.GetFrontendClient()
	frontendClient := frontend.NewRetryableClient(
		frontendRawClient,
		common.CreateFrontendServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)

	matchingRawClient, err := clientBean.GetMatchingClient(namespaceCache.GetNamespaceName)
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
		ClusterMetadata:  clusterMetadata,
		NamespaceCache:   namespaceCache,
	}
	visibilityArchiverBootstrapContainer := &archiver.VisibilityBootstrapContainer{
		Logger:          logger,
		MetricsClient:   params.MetricsClient,
		ClusterMetadata: clusterMetadata,
		NamespaceCache:  namespaceCache,
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
		metricsScope:    params.MetricsScope,
		clusterMetadata: clusterMetadata,

		// other common resources

		namespaceCache:    namespaceCache,
		timeSource:        clock.NewRealTimeSource(),
		payloadSerializer: persistence.NewPayloadSerializer(),
		metricsClient:     params.MetricsClient,
		archivalMetadata:  params.ArchivalMetadata,
		archiverProvider:  params.ArchiverProvider,

		// membership infos

		membershipMonitor:       membershipMonitor,
		frontendServiceResolver: frontendServiceResolver,
		matchingServiceResolver: matchingServiceResolver,
		historyServiceResolver:  historyServiceResolver,
		workerServiceResolver:   workerServiceResolver,

		// internal services clients

		sdkClient:         params.SdkClient,
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
			params.MetricsScope,
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
	h.namespaceCache.Start()

	hostInfo, err := h.membershipMonitor.WhoAmI()
	if err != nil {
		h.logger.Fatal("fail to get host info from membership monitor", tag.Error(err))
	}
	h.hostInfo = hostInfo

	// The service is now started up
	h.logger.Info("Service resources started", tag.Address(hostInfo.GetAddress()))
	// seed the random generator once for this service
	rand.Seed(time.Now().UnixNano())
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

	h.namespaceCache.Stop()
	h.membershipMonitor.Stop()
	h.ringpopChannel.Close()
	h.runtimeMetricsReporter.Stop()
	h.persistenceBean.Close()
	if h.visibilityMgr != nil {
		h.visibilityMgr.Close()
	}
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

// GetNamespaceCache return namespace cache
func (h *Impl) GetNamespaceCache() cache.NamespaceCache {
	return h.namespaceCache
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
func (h *Impl) GetFrontendRawClient() workflowservice.WorkflowServiceClient {
	return h.frontendRawClient
}

// GetFrontendClient return frontend client with retry policy
func (h *Impl) GetFrontendClient() workflowservice.WorkflowServiceClient {
	return h.frontendClient
}

// GetMatchingRawClient return matching client without retry policy
func (h *Impl) GetMatchingRawClient() matchingservice.MatchingServiceClient {
	return h.matchingRawClient
}

// GetMatchingClient return matching client with retry policy
func (h *Impl) GetMatchingClient() matchingservice.MatchingServiceClient {
	return h.matchingClient
}

// GetHistoryRawClient return history client without retry policy
func (h *Impl) GetHistoryRawClient() historyservice.HistoryServiceClient {
	return h.historyRawClient
}

// GetHistoryClient return history client with retry policy
func (h *Impl) GetHistoryClient() historyservice.HistoryServiceClient {
	return h.historyClient
}

// GetRemoteAdminClient return remote admin client for given cluster name
func (h *Impl) GetRemoteAdminClient(
	cluster string,
) adminservice.AdminServiceClient {

	return h.clientBean.GetRemoteAdminClient(cluster)
}

// GetRemoteFrontendClient return remote frontend client for given cluster name
func (h *Impl) GetRemoteFrontendClient(
	cluster string,
) workflowservice.WorkflowServiceClient {

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

// GetNamespaceReplicationQueue return namespace replication queue
func (h *Impl) GetNamespaceReplicationQueue() persistence.NamespaceReplicationQueue {
	return h.persistenceBean.GetNamespaceReplicationQueue()
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
	shardID int32,
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
