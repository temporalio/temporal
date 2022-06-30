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
	"fmt"
	"net"
	"os"
	"time"

	"go.uber.org/fx"
	"google.golang.org/grpc"

	"go.temporal.io/api/workflowservice/v1"

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
	"go.temporal.io/server/common/ringpop"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/telemetry"
)

type (
	SnTaggedLogger       log.Logger
	ThrottledLogger      log.Logger
	ThrottledLoggerRpsFn quotas.RateFn
	NamespaceLogger      log.Logger
	ServiceName          string
	HostName             string
	InstanceID           string
	ServiceNames         map[string]struct{}

	MatchingRawClient matchingservice.MatchingServiceClient
	MatchingClient    matchingservice.MatchingServiceClient

	RuntimeMetricsReporterParams struct {
		fx.In

		Provider   metrics.MetricsHandler
		Logger     SnTaggedLogger
		InstanceID InstanceID `optional:"true"`
	}
)

// Module
// Use fx.Hook and OnStart/OnStop to manage Daemon resource lifecycle
// See LifetimeHooksModule for detail
var Module = fx.Options(
	persistenceClient.Module,
	fx.Provide(SnTaggedLoggerProvider),
	fx.Provide(HostNameProvider),
	fx.Provide(TimeSourceProvider),
	cluster.MetadataLifetimeHooksModule,
	fx.Provide(SearchAttributeProviderProvider),
	fx.Provide(SearchAttributeManagerProvider),
	fx.Provide(NamespaceRegistryProvider),
	namespace.RegistryLifetimeHooksModule,
	fx.Provide(serialization.NewSerializer),
	fx.Provide(HistoryBootstrapContainerProvider),
	fx.Provide(VisibilityBootstrapContainerProvider),
	fx.Provide(ClientFactoryProvider),
	fx.Provide(ClientBeanProvider),
	fx.Provide(FrontendClientProvider),
	fx.Provide(GrpcListenerProvider),
	fx.Provide(RuntimeMetricsReporterProvider),
	metrics.RuntimeMetricsReporterLifetimeHooksModule,
	fx.Provide(HistoryClientProvider),
	fx.Provide(MatchingRawClientProvider),
	fx.Provide(MatchingClientProvider),
	membership.HostInfoProviderModule,
	fx.Invoke(RegisterBootstrapContainer),
	fx.Provide(PersistenceConfigProvider),
	fx.Provide(MetricsClientProvider),
)

var DefaultOptions = fx.Options(
	fx.Provide(MembershipMonitorProvider),
	fx.Provide(RPCFactoryProvider),
	fx.Provide(ArchivalMetadataProvider),
	fx.Provide(ArchiverProviderProvider),
	fx.Provide(ThrottledLoggerProvider),
	fx.Provide(SdkClientFactoryProvider),
	fx.Provide(SdkWorkerFactoryProvider),
	fx.Provide(DCRedirectionPolicyProvider),
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
	dynamicCollection *dynamicconfig.Collection,
) searchattribute.Provider {
	return searchattribute.NewManager(
		timeSource,
		cmMgr,
		dynamicCollection.GetBoolProperty(dynamicconfig.ForceSearchAttributesCacheRefreshOnRead, false))
}

func SearchAttributeManagerProvider(
	timeSource clock.TimeSource,
	cmMgr persistence.ClusterMetadataManager,
	dynamicCollection *dynamicconfig.Collection,
) searchattribute.Manager {
	return searchattribute.NewManager(
		timeSource,
		cmMgr,
		dynamicCollection.GetBoolProperty(dynamicconfig.ForceSearchAttributesCacheRefreshOnRead, false))
}

func NamespaceRegistryProvider(
	logger SnTaggedLogger,
	metricsClient metrics.Client,
	clusterMetadata cluster.Metadata,
	metadataManager persistence.MetadataManager,
	dynamicCollection *dynamicconfig.Collection,
) namespace.Registry {
	return namespace.NewRegistry(
		metadataManager,
		clusterMetadata.IsGlobalNamespaceEnabled(),
		dynamicCollection.GetDurationProperty(dynamicconfig.NamespaceCacheRefreshInterval, 10*time.Second),
		metricsClient,
		logger,
	)
}

func ClientFactoryProvider(
	factoryProvider client.FactoryProvider,
	rpcFactory common.RPCFactory,
	membershipMonitor membership.Monitor,
	metricsClient metrics.Client,
	dynamicCollection *dynamicconfig.Collection,
	persistenceConfig *config.Persistence,
	logger SnTaggedLogger,
	throttledLogger ThrottledLogger,
) client.Factory {
	return factoryProvider.NewFactory(
		rpcFactory,
		membershipMonitor,
		metricsClient,
		dynamicCollection,
		persistenceConfig.NumHistoryShards,
		logger,
		throttledLogger,
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

func MembershipMonitorProvider(
	lc fx.Lifecycle,
	clusterMetadataManager persistence.ClusterMetadataManager,
	logger SnTaggedLogger,
	cfg *config.Config,
	svcName ServiceName,
	tlsConfigProvider encryption.TLSConfigProvider,
	dc *dynamicconfig.Collection,
) (membership.Monitor, error) {
	servicePortMap := make(map[string]int)
	for sn, sc := range cfg.Services {
		servicePortMap[sn] = sc.RPC.GRPCPort
	}

	rpcConfig := cfg.Services[string(svcName)].RPC

	factory, err := ringpop.NewRingpopFactory(
		&cfg.Global.Membership,
		string(svcName),
		servicePortMap,
		logger,
		clusterMetadataManager,
		&rpcConfig,
		tlsConfigProvider,
		dc,
	)
	if err != nil {
		return nil, err
	}

	monitor, err := factory.GetMembershipMonitor()
	if err != nil {
		return nil, err
	}

	lc.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				monitor.Start()
				return nil
			},
			OnStop: func(context.Context) error {
				monitor.Stop()
				factory.CloseTChannel()
				return nil
			},
		},
	)

	return monitor, nil
}

func FrontendClientProvider(clientBean client.Bean) workflowservice.WorkflowServiceClient {
	frontendRawClient := clientBean.GetFrontendClient()
	return frontend.NewRetryableClient(
		frontendRawClient,
		common.CreateFrontendServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)
}

func RuntimeMetricsReporterProvider(
	params RuntimeMetricsReporterParams,
) *metrics.RuntimeMetricsReporter {
	return metrics.NewRuntimeMetricsReporter(
		params.Provider,
		time.Minute,
		params.Logger,
		string(params.InstanceID),
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

// TODO: rework to depend on...
func MetricsClientProvider(logger log.Logger, serviceName ServiceName, provider metrics.MetricsHandler) metrics.Client {
	serviceIdx := metrics.GetMetricsServiceIdx(string(serviceName), logger)
	return metrics.NewClient(provider, serviceIdx)
}

func PersistenceConfigProvider(persistenceConfig config.Persistence, dc *dynamicconfig.Collection) *config.Persistence {
	persistenceConfig.TransactionSizeLimit = dc.GetIntProperty(dynamicconfig.TransactionSizeLimit, common.DefaultTransactionSizeLimit)
	return &persistenceConfig
}

func ArchivalMetadataProvider(dc *dynamicconfig.Collection, cfg *config.Config) archiver.ArchivalMetadata {
	return archiver.NewArchivalMetadata(
		dc,
		cfg.Archival.History.State,
		cfg.Archival.History.EnableRead,
		cfg.Archival.Visibility.State,
		cfg.Archival.Visibility.EnableRead,
		&cfg.NamespaceDefaults.Archival,
	)
}

func ArchiverProviderProvider(cfg *config.Config) provider.ArchiverProvider {
	return provider.NewArchiverProvider(cfg.Archival.History.Provider, cfg.Archival.Visibility.Provider)
}

func SdkClientFactoryProvider(cfg *config.Config, tlsConfigProvider encryption.TLSConfigProvider, provider metrics.MetricsHandler) (sdk.ClientFactory, error) {
	tlsFrontendConfig, err := tlsConfigProvider.GetFrontendClientConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to load frontend TLS configuration: %w", err)
	}

	return sdk.NewClientFactory(
		cfg.PublicClient.HostPort,
		tlsFrontendConfig,
		sdk.NewMetricsHandler(provider),
	), nil
}

func SdkWorkerFactoryProvider() sdk.WorkerFactory {
	return sdk.NewWorkerFactory()
}

func DCRedirectionPolicyProvider(cfg *config.Config) config.DCRedirectionPolicy {
	return cfg.DCRedirectionPolicy
}

func RPCFactoryProvider(
	cfg *config.Config,
	svcName ServiceName,
	logger log.Logger,
	tlsConfigProvider encryption.TLSConfigProvider,
	dc *dynamicconfig.Collection,
	clusterMetadata *cluster.Config,
	traceInterceptor telemetry.ClientTraceInterceptor,
) common.RPCFactory {
	svcCfg := cfg.Services[string(svcName)]
	return rpc.NewFactory(
		&svcCfg.RPC,
		string(svcName),
		logger,
		tlsConfigProvider,
		dc,
		clusterMetadata,
		[]grpc.UnaryClientInterceptor{
			grpc.UnaryClientInterceptor(traceInterceptor),
		},
	)
}
