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
	"context"
	"net"
	"os"
	"time"

	"github.com/uber/tchannel-go"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/fx"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
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
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
)

type (
	SnTaggedLogger       log.Logger
	ThrottledLogger      log.Logger
	ThrottledLoggerRpsFn quotas.RateFn
	ServiceName          string
	HostName             string
	InstanceID           string

	MatchingRawClient matchingservice.MatchingServiceClient
	MatchingClient    matchingservice.MatchingServiceClient
)

var Module = fx.Options(
	persistenceClient.Module,
	fx.Provide(SnTaggedLoggerProvider),
	fx.Provide(ThrottledLoggerProvider),
	fx.Provide(PersistenceConfigProvider),
	fx.Provide(HostNameProvider),
	fx.Provide(ServiceNameProvider),
	fx.Provide(TimeSourceProvider),
	fx.Provide(cluster.NewMetadataFromConfig),
	fx.Provide(MetricsClientProvider),
	fx.Provide(MetricsUserScopeProvider),
	fx.Provide(SearchAttributeProviderProvider),
	fx.Provide(SearchAttributeManagerProvider),
	fx.Provide(NamespaceRegistryProvider),
	namespace.RegistryLifetimeHooksModule,
	fx.Provide(serialization.NewSerializer),
	fx.Provide(ArchivalMetadataProvider),
	fx.Provide(ArchiverProviderProvider),
	fx.Provide(HistoryBootstrapContainerProvider),
	fx.Provide(VisibilityBootstrapContainerProvider),
	fx.Provide(MembershipFactoryProvider),
	fx.Provide(MembershipMonitorProvider),
	membership.MonitorLifetimeHooksModule,
	fx.Provide(ClientFactoryProvider),
	fx.Provide(ClientBeanProvider),
	sdk.Module,
	fx.Provide(SdkClientFactoryProvider),
	fx.Provide(FrontedClientProvider),
	fx.Provide(PersistenceFaultInjectionFactoryProvider),
	fx.Provide(GrpcListenerProvider),
	fx.Provide(InstanceIDProvider),
	fx.Provide(RingpopChannelProvider),
	fx.Invoke(RingpopChannelLifetimeHooks),
	fx.Provide(RuntimeMetricsReporterProvider),
	metrics.RuntimeMetricsReporterLifetimeHooksModule,
	fx.Provide(HistoryClientProvider),
	fx.Provide(MatchingRawClientProvider),
	fx.Provide(MatchingClientProvider),
	HostInfoProviderModule,
	fx.Invoke(RegisterBootstrapContainer),
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

func ServiceNameProvider(params *BootstrapParams) ServiceName {
	return ServiceName(params.Name)
}

func HostNameProvider() (HostName, error) {
	hn, err := os.Hostname()
	return HostName(hn), err
}

func TimeSourceProvider() clock.TimeSource {
	return clock.NewRealTimeSource()
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

func NamespaceRegistryProvider(
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

func ClientFactoryProvider(
	factoryProvider client.FactoryProvider,
	rpcFactory common.RPCFactory,
	membershipMonitor membership.Monitor,
	metricsClient metrics.Client,
	dynamicCollection *dynamicconfig.Collection,
	persistenceConfig *config.Persistence,
	logger SnTaggedLogger,
) client.Factory {
	return factoryProvider.NewFactory(
		rpcFactory,
		membershipMonitor,
		metricsClient,
		dynamicCollection,
		persistenceConfig.NumHistoryShards,
		logger,
	)
}

func ClientBeanProvider(
	clientFactory client.Factory,
	clusterMetadata cluster.Metadata,
) (client.Bean, error) {
	return client.NewClientBean(
		clientFactory,
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

// TODO: Seems that this factory mostly handles singleton logic. We should be able to handle it via IOC.
func MembershipMonitorProvider(membershipFactory MembershipMonitorFactory) (membership.Monitor, error) {
	return membershipFactory.GetMembershipMonitor()
}

// TODO (alex): move this to `sdk` package after BootstrapParams removal.
func SdkClientFactoryProvider(params *BootstrapParams) sdk.ClientFactory {
	return params.SdkClientFactory
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

func RingpopChannelLifetimeHooks(
	lc fx.Lifecycle,
	ch *tchannel.Channel,
) {
	lc.Append(
		fx.Hook{
			OnStop: func(context.Context) error {
				ch.Close()
				return nil
			},
		},
	)
}

func InstanceIDProvider(params *BootstrapParams) InstanceID {
	return InstanceID(params.InstanceID)
}

func MetricsUserScopeProvider(serverMetricsClient metrics.Client) metrics.UserScope {
	return serverMetricsClient.UserScope()
}

func RuntimeMetricsReporterProvider(
	metricsScope metrics.UserScope,
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

func VisibilityBootstrapContainerProvider(
	logger SnTaggedLogger,
	metricsClient metrics.Client,
	clusterMetadata cluster.Metadata,
) *archiver.VisibilityBootstrapContainer {
	return &archiver.VisibilityBootstrapContainer{
		Logger:          logger,
		MetricsClient:   metricsClient,
		ClusterMetadata: clusterMetadata,
	}
}

func HistoryBootstrapContainerProvider(
	logger SnTaggedLogger,
	metricsClient metrics.Client,
	clusterMetadata cluster.Metadata,
	executionManager persistence.ExecutionManager,
) *archiver.HistoryBootstrapContainer {
	return &archiver.HistoryBootstrapContainer{
		ExecutionManager: executionManager,
		Logger:           logger,
		MetricsClient:    metricsClient,
		ClusterMetadata:  clusterMetadata,
	}
}

func RegisterBootstrapContainer(
	archiverProvider provider.ArchiverProvider,
	serviceName ServiceName,
	visibilityArchiverBootstrapContainer *archiver.VisibilityBootstrapContainer,
	historyArchiverBootstrapContainer *archiver.HistoryBootstrapContainer,
) error {
	return archiverProvider.RegisterBootstrapContainer(
		string(serviceName),
		historyArchiverBootstrapContainer,
		visibilityArchiverBootstrapContainer,
	)
}

func HistoryClientProvider(clientBean client.Bean) historyservice.HistoryServiceClient {
	historyRawClient := clientBean.GetHistoryClient()
	historyClient := history.NewRetryableClient(
		historyRawClient,
		common.CreateHistoryServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)
	return historyClient
}

func MatchingRawClientProvider(clientBean client.Bean, namespaceRegistry namespace.Registry) (
	MatchingRawClient,
	error,
) {
	return clientBean.GetMatchingClient(namespaceRegistry.GetNamespaceName)
}

func MatchingClientProvider(matchingRawClient MatchingRawClient) MatchingClient {
	return matching.NewRetryableClient(
		matchingRawClient,
		common.CreateMatchingServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)
}
