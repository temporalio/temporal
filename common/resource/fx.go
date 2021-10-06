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
	"net"
	"os"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.uber.org/fx"

	"go.temporal.io/server/client"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/clock"
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
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

type (
	SnTaggedLogger       log.Logger
	ThrottledLogger      log.Logger
	ThrottledLoggerRpsFn quotas.RateFn
	ServiceName          string
	HostName             string
	InstanceID           string
)

var Module = fx.Options(
	fx.Provide(SnTaggedLoggerProvider),
	fx.Provide(ThrottledLoggerProvider),
	fx.Provide(PersistenceConfigProvider),
	fx.Provide(MetricsScopeProvider),
	fx.Provide(HostNameProvider),
	fx.Provide(ServiceNameProvider),
	fx.Provide(ClusterMetadataProvider),
	fx.Provide(ClusterMetadataConfigProvider),
	fx.Provide(TimeSourceProvider),
	fx.Provide(ClusterMetadataManagerProvider),
	fx.Provide(PersistenceServiceResolverProvider),
	fx.Provide(AbstractDatastoreFactoryProvider),
	fx.Provide(ClusterNameProvider),
	fx.Provide(MetricsClientProvider),
	persistenceClient.FactoryModule,
	fx.Provide(SearchAttributeProviderProvider),
	fx.Provide(SearchAttributeManagerProvider),
	fx.Provide(SearchAttributeMapperProvider),
	fx.Provide(MetadataManagerProvider),
	fx.Provide(NamespaceCacheProvider),
	fx.Provide(serialization.NewSerializer),
	fx.Provide(ArchivalMetadataProvider),
	fx.Provide(ArchiverProviderProvider),
	fx.Invoke(RegisterBootstrapContainer),
	fx.Provide(PersistenceBeanProvider),
	fx.Provide(MembershipFactoryProvider),
	fx.Provide(MembershipMonitorProvider),
	fx.Provide(ClientFactoryProvider),
	fx.Provide(ClientBeanProvider),
	fx.Provide(SdkClientProvider),
	fx.Provide(FrontedClientProvider),
	fx.Provide(PersistenceFaultInjectionFactoryProvider),
	fx.Provide(GrpcListenerProvider),
	fx.Provide(InstanceIDProvider),
	fx.Provide(RingpopChannelProvider),
	fx.Provide(RuntimeMetricsReporterProvider),
	fx.Provide(NewFromDI),
)

func SnTaggedLoggerProvider(logger log.Logger, sn ServiceName) SnTaggedLogger {
	return log.With(logger, tag.Service(string(sn)))
}

func ThrottledLoggerProvider(
	logger SnTaggedLogger,
	fn ThrottledLoggerRpsFn,
) ThrottledLogger {
	return log.NewThrottledLogger(
		logger,
		quotas.RateFn(fn),
	)
}

func GrpcListenerProvider(factory common.RPCFactory) net.Listener {
	return factory.GetGRPCListener()
}

func MetricsClientProvider(params *BootstrapParams) metrics.Client {
	return params.MetricsClient
}

func PersistenceConfigProvider(params *BootstrapParams) *config.Persistence {
	return &params.PersistenceConfig
}

func MetricsScopeProvider(params *BootstrapParams) tally.Scope {
	return params.MetricsScope
}

func ServiceNameProvider(params *BootstrapParams) ServiceName {
	return ServiceName(params.Name)
}

func HostNameProvider() (HostName, error) {
	hn, err := os.Hostname()
	return HostName(hn), err
}

func ClusterMetadataConfigProvider(params *BootstrapParams) *config.ClusterMetadata {
	return params.ClusterMetadataConfig
}

func ClusterMetadataProvider(config *config.ClusterMetadata) cluster.Metadata {
	return cluster.NewMetadata(
		config.EnableGlobalNamespace,
		config.FailoverVersionIncrement,
		config.MasterClusterName,
		config.CurrentClusterName,
		config.ClusterInformation,
	)
}

func ClusterNameProvider(config *config.ClusterMetadata) persistenceClient.ClusterName {
	return persistenceClient.ClusterName(config.CurrentClusterName)
}

func PersistenceServiceResolverProvider(params *BootstrapParams) resolver.ServiceResolver {
	return params.PersistenceServiceResolver
}

func AbstractDatastoreFactoryProvider(params *BootstrapParams) persistenceClient.AbstractDataStoreFactory {
	return params.AbstractDatastoreFactory
}

func TimeSourceProvider() clock.TimeSource {
	return clock.NewRealTimeSource()
}

func ClusterMetadataManagerProvider(factory persistenceClient.Factory) (persistence.ClusterMetadataManager, error) {
	return factory.NewClusterMetadataManager()
}

func SearchAttributeProviderProvider(
	timeSource clock.TimeSource,
	cmMgr persistence.ClusterMetadataManager,
) searchattribute.Provider {
	return searchattribute.NewManager(timeSource, cmMgr)
}

func SearchAttributeManagerProvider(
	timeSource clock.TimeSource,
	cmMgr persistence.ClusterMetadataManager,
) searchattribute.Manager {
	return searchattribute.NewManager(timeSource, cmMgr)
}

func SearchAttributeMapperProvider(params *BootstrapParams) searchattribute.Mapper {
	return params.SearchAttributesMapper
}

func MetadataManagerProvider(factory persistenceClient.Factory) (persistence.MetadataManager, error) {
	return factory.NewMetadataManager()
}

func NamespaceCacheProvider(
	logger SnTaggedLogger,
	metricsClient metrics.Client,
	clusterMetadata cluster.Metadata,
	metadataManager persistence.MetadataManager,
) namespace.Registry {
	return namespace.NewRegistry(
		metadataManager,
		clusterMetadata.IsGlobalNamespaceEnabled(),
		metricsClient,
		logger,
	)
}

func ArchivalMetadataProvider(params *BootstrapParams) archiver.ArchivalMetadata {
	return params.ArchivalMetadata
}

func ArchiverProviderProvider(params *BootstrapParams) provider.ArchiverProvider {
	return params.ArchiverProvider
}

func ClientFactoryProvider(params *BootstrapParams) client.FactoryProvider {
	factoryProvider := params.ClientFactoryProvider
	if factoryProvider == nil {
		factoryProvider = client.NewFactoryProvider()
	}
	return factoryProvider
}

func ClientBeanProvider(
	factoryProvider client.FactoryProvider,
	rpcFactory common.RPCFactory,
	membershipMonitor membership.Monitor,
	metricsClient metrics.Client,
	dynamicCollection *dynamicconfig.Collection,
	persistenceConfig *config.Persistence,
	logger SnTaggedLogger,
	clusterMetadata cluster.Metadata,
) (client.Bean, error) {
	return client.NewClientBean(
		factoryProvider.NewFactory(
			rpcFactory,
			membershipMonitor,
			metricsClient,
			dynamicCollection,
			persistenceConfig.NumHistoryShards,
			logger,
		),
		clusterMetadata,
	)
}

func MembershipFactoryProvider(
	params *BootstrapParams,
	persistenceBean persistenceClient.Bean,
	logger SnTaggedLogger,
) (MembershipMonitorFactory, error) {
	return params.MembershipFactoryInitializer(persistenceBean, logger)
}

func PersistenceBeanProvider(factory persistenceClient.Factory) (persistenceClient.Bean, error) {
	return persistenceClient.NewBeanFromFactory(factory)
}

// TODO: Seems that all this factory mostly handles singleton logic. We should be able to handle it via IOC.
func MembershipMonitorProvider(membershipFactory MembershipMonitorFactory) (membership.Monitor, error) {
	return membershipFactory.GetMembershipMonitor()
}

func SdkClientProvider(params *BootstrapParams) sdkclient.Client {
	return params.SdkClient
}

func FrontedClientProvider(clientBean client.Bean) workflowservice.WorkflowServiceClient {
	frontendRawClient := clientBean.GetFrontendClient()
	return frontend.NewRetryableClient(
		frontendRawClient,
		common.CreateFrontendServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)
}

func PersistenceFaultInjectionFactoryProvider(factory persistenceClient.Factory) *persistenceClient.FaultInjectionDataStoreFactory {
	return factory.FaultInjection()
}

func RingpopChannelProvider(rpcFactory common.RPCFactory) *tchannel.Channel {
	return rpcFactory.GetRingpopChannel()
}

func InstanceIDProvider(params *BootstrapParams) InstanceID {
	return InstanceID(params.InstanceID)
}

func RuntimeMetricsReporterProvider(
	metricsScope tally.Scope,
	logger SnTaggedLogger,
	instanceID InstanceID,
) *metrics.RuntimeMetricsReporter {
	return metrics.NewRuntimeMetricsReporter(
		metricsScope,
		time.Minute,
		logger,
		string(instanceID),
	)
}

func RegisterBootstrapContainer(
	logger SnTaggedLogger,
	archiverProvider provider.ArchiverProvider,
	serviceName ServiceName,
	metricsClient metrics.Client,
	clusterMetadata cluster.Metadata,
	persistenceBean persistenceClient.Bean,
) error {
	historyArchiverBootstrapContainer := &archiver.HistoryBootstrapContainer{
		ExecutionManager: persistenceBean.GetExecutionManager(),
		Logger:           logger,
		MetricsClient:    metricsClient,
		ClusterMetadata:  clusterMetadata,
	}
	visibilityArchiverBootstrapContainer := &archiver.VisibilityBootstrapContainer{
		Logger:          logger,
		MetricsClient:   metricsClient,
		ClusterMetadata: clusterMetadata,
	}
	return archiverProvider.RegisterBootstrapContainer(
		string(serviceName),
		historyArchiverBootstrapContainer,
		visibilityArchiverBootstrapContainer,
	)
}

func NewFromDI(
	persistenceConf *config.Persistence,
	svcName ServiceName,
	metricsScope tally.Scope,
	hostName HostName,
	clusterMetadata cluster.Metadata,
	saProvider searchattribute.Provider,
	saManager searchattribute.Manager,
	saMapper searchattribute.Mapper,
	namespaceRegistry namespace.Registry,
	timeSource clock.TimeSource,
	payloadSerializer serialization.Serializer,
	metricsClient metrics.Client,
	archivalMetadata archiver.ArchivalMetadata,
	archiverProvider provider.ArchiverProvider,
	membershipMonitor membership.Monitor,
	sdkClient sdkclient.Client,
	frontendClient workflowservice.WorkflowServiceClient,
	clientBean client.Bean,
	persistenceBean persistenceClient.Bean,
	persistenceFaultInjection *persistenceClient.FaultInjectionDataStoreFactory,
	logger SnTaggedLogger,
	throttledLogger ThrottledLogger,
	grpcListener net.Listener,
	ringpopChannel *tchannel.Channel,
	runtimeMetricsReporter *metrics.RuntimeMetricsReporter,
	rpcFactory common.RPCFactory,
) (Resource, error) {

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

	matchingRawClient, err := clientBean.GetMatchingClient(namespaceRegistry.GetNamespaceName)
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

	return &Impl{
		status: common.DaemonStatusInitialized,

		numShards:       persistenceConf.NumHistoryShards,
		serviceName:     string(svcName),
		hostName:        string(hostName),
		metricsScope:    metricsScope,
		clusterMetadata: clusterMetadata,
		saProvider:      saProvider,
		saManager:       saManager,
		saMapper:        saMapper,

		namespaceRegistry: namespaceRegistry,
		timeSource:        timeSource,
		payloadSerializer: payloadSerializer,
		metricsClient:     metricsClient,
		archivalMetadata:  archivalMetadata,
		archiverProvider:  archiverProvider,

		// membership infos

		membershipMonitor:       membershipMonitor,
		frontendServiceResolver: frontendServiceResolver,
		matchingServiceResolver: matchingServiceResolver,
		historyServiceResolver:  historyServiceResolver,
		workerServiceResolver:   workerServiceResolver,

		sdkClient:         sdkClient,
		frontendClient:    frontendClient,
		matchingRawClient: matchingRawClient,
		matchingClient:    matchingClient,
		historyRawClient:  historyRawClient,
		historyClient:     historyClient,
		clientBean:        clientBean,

		persistenceBean:           persistenceBean,
		persistenceFaultInjection: persistenceFaultInjection,

		logger:          logger,
		throttledLogger: throttledLogger,

		grpcListener: grpcListener,

		ringpopChannel: ringpopChannel,

		runtimeMetricsReporter: runtimeMetricsReporter,
		rpcFactory:             rpcFactory,
	}, nil
}
