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
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"time"

	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"

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
	"go.temporal.io/server/common/deadlock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/pingable"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/utf8validator"
)

type (
	ThrottledLoggerRpsFn quotas.RateFn
	NamespaceLogger      log.Logger
	HostName             string
	InstanceID           string
	ServiceNames         map[primitives.ServiceName]struct{}

	HistoryRawClient historyservice.HistoryServiceClient
	HistoryClient    historyservice.HistoryServiceClient

	MatchingRawClient matchingservice.MatchingServiceClient
	MatchingClient    matchingservice.MatchingServiceClient

	RuntimeMetricsReporterParams struct {
		fx.In

		MetricHandler metrics.Handler
		Logger        log.SnTaggedLogger
		InstanceID    InstanceID `optional:"true"`
	}
)

// Module
// Use fx.Hook and OnStart/OnStop to manage Daemon resource lifecycle
// See LifetimeHooksModule for detail
var Module = fx.Options(
	persistenceClient.Module,
	fx.Provide(HostNameProvider),
	fx.Provide(TimeSourceProvider),
	cluster.MetadataLifetimeHooksModule,
	fx.Provide(SearchAttributeMapperProviderProvider),
	fx.Provide(SearchAttributeProviderProvider),
	fx.Provide(SearchAttributeManagerProvider),
	fx.Provide(NamespaceRegistryProvider),
	namespace.RegistryLifetimeHooksModule,
	fx.Provide(fx.Annotate(
		func(p namespace.Registry) pingable.Pingable { return p },
		fx.ResultTags(`group:"deadlockDetectorRoots"`),
	)),
	fx.Provide(serialization.NewSerializer),
	fx.Provide(HistoryBootstrapContainerProvider),
	fx.Provide(VisibilityBootstrapContainerProvider),
	fx.Provide(ClientFactoryProvider),
	fx.Provide(ClientBeanProvider),
	fx.Provide(FrontendClientProvider),
	fx.Provide(GrpcListenerProvider),
	fx.Provide(RuntimeMetricsReporterProvider),
	metrics.RuntimeMetricsReporterLifetimeHooksModule,
	fx.Provide(HistoryRawClientProvider),
	fx.Provide(HistoryClientProvider),
	fx.Provide(MatchingRawClientProvider),
	fx.Provide(MatchingClientProvider),
	membership.GRPCResolverModule,
	fx.Provide(FrontendHTTPClientCacheProvider),
	fx.Invoke(RegisterBootstrapContainer),
	fx.Provide(PersistenceConfigProvider),
	fx.Provide(health.NewServer),
	deadlock.Module,
	config.Module,
	utf8validator.Module,
	fx.Invoke(func(*utf8validator.Validator) {}), // force this to be constructed even if not referenced elsewhere
)

var DefaultOptions = fx.Options(
	fx.Provide(RPCFactoryProvider),
	fx.Provide(ArchivalMetadataProvider),
	fx.Provide(ArchiverProviderProvider),
	fx.Provide(ThrottledLoggerProvider),
	fx.Provide(SdkClientFactoryProvider),
	fx.Provide(DCRedirectionPolicyProvider),
)

func DefaultSnTaggedLoggerProvider(logger log.Logger, sn primitives.ServiceName) log.SnTaggedLogger {
	return log.With(logger, tag.Service(sn))
}

func ThrottledLoggerProvider(
	logger log.SnTaggedLogger,
	fn ThrottledLoggerRpsFn,
) log.ThrottledLogger {
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

func SearchAttributeMapperProviderProvider(
	saMapper searchattribute.Mapper,
	namespaceRegistry namespace.Registry,
	searchAttributeProvider searchattribute.Provider,
	persistenceConfig *config.Persistence,
) searchattribute.MapperProvider {
	return searchattribute.NewMapperProvider(
		saMapper,
		namespaceRegistry,
		searchAttributeProvider,
		persistenceConfig.IsSQLVisibilityStore(),
	)
}

func SearchAttributeProviderProvider(
	timeSource clock.TimeSource,
	cmMgr persistence.ClusterMetadataManager,
	dynamicCollection *dynamicconfig.Collection,
) searchattribute.Provider {
	return searchattribute.NewManager(
		timeSource,
		cmMgr,
		dynamicconfig.ForceSearchAttributesCacheRefreshOnRead.Get(dynamicCollection))
}

func SearchAttributeManagerProvider(
	timeSource clock.TimeSource,
	cmMgr persistence.ClusterMetadataManager,
	dynamicCollection *dynamicconfig.Collection,
) searchattribute.Manager {
	return searchattribute.NewManager(
		timeSource,
		cmMgr,
		dynamicconfig.ForceSearchAttributesCacheRefreshOnRead.Get(dynamicCollection))
}

func NamespaceRegistryProvider(
	logger log.SnTaggedLogger,
	metricsHandler metrics.Handler,
	clusterMetadata cluster.Metadata,
	metadataManager persistence.MetadataManager,
	dynamicCollection *dynamicconfig.Collection,
) namespace.Registry {
	return namespace.NewRegistry(
		metadataManager,
		clusterMetadata.IsGlobalNamespaceEnabled(),
		dynamicconfig.NamespaceCacheRefreshInterval.Get(dynamicCollection),
		dynamicconfig.ForceSearchAttributesCacheRefreshOnRead.Get(dynamicCollection),
		metricsHandler,
		logger,
	)
}

func ClientFactoryProvider(
	factoryProvider client.FactoryProvider,
	rpcFactory common.RPCFactory,
	membershipMonitor membership.Monitor,
	metricsHandler metrics.Handler,
	dynamicCollection *dynamicconfig.Collection,
	persistenceConfig *config.Persistence,
	logger log.SnTaggedLogger,
	throttledLogger log.ThrottledLogger,
) client.Factory {
	return factoryProvider.NewFactory(
		rpcFactory,
		membershipMonitor,
		metricsHandler,
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

func FrontendClientProvider(clientBean client.Bean) workflowservice.WorkflowServiceClient {
	frontendRawClient := clientBean.GetFrontendClient()
	return frontend.NewRetryableClient(
		frontendRawClient,
		common.CreateFrontendClientRetryPolicy(),
		common.IsServiceClientTransientError,
	)
}

func RuntimeMetricsReporterProvider(
	params RuntimeMetricsReporterParams,
) *metrics.RuntimeMetricsReporter {
	return metrics.NewRuntimeMetricsReporter(
		params.MetricHandler,
		time.Minute,
		params.Logger,
		string(params.InstanceID),
	)
}

func VisibilityBootstrapContainerProvider(
	logger log.SnTaggedLogger,
	metricsHandler metrics.Handler,
	clusterMetadata cluster.Metadata,
) *archiver.VisibilityBootstrapContainer {
	return &archiver.VisibilityBootstrapContainer{
		Logger:          logger,
		MetricsHandler:  metricsHandler,
		ClusterMetadata: clusterMetadata,
	}
}

func HistoryBootstrapContainerProvider(
	logger log.SnTaggedLogger,
	metricsHandler metrics.Handler,
	clusterMetadata cluster.Metadata,
	executionManager persistence.ExecutionManager,
) *archiver.HistoryBootstrapContainer {
	return &archiver.HistoryBootstrapContainer{
		ExecutionManager: executionManager,
		Logger:           logger,
		MetricsHandler:   metricsHandler,
		ClusterMetadata:  clusterMetadata,
	}
}

func RegisterBootstrapContainer(
	archiverProvider provider.ArchiverProvider,
	serviceName primitives.ServiceName,
	visibilityArchiverBootstrapContainer *archiver.VisibilityBootstrapContainer,
	historyArchiverBootstrapContainer *archiver.HistoryBootstrapContainer,
) error {
	return archiverProvider.RegisterBootstrapContainer(
		string(serviceName),
		historyArchiverBootstrapContainer,
		visibilityArchiverBootstrapContainer,
	)
}

func HistoryRawClientProvider(clientBean client.Bean) HistoryRawClient {
	return clientBean.GetHistoryClient()
}

func HistoryClientProvider(historyRawClient HistoryRawClient) HistoryClient {
	return history.NewRetryableClient(
		historyRawClient,
		common.CreateHistoryClientRetryPolicy(),
		common.IsServiceClientTransientError,
	)
}

func MatchingRawClientProvider(
	clientBean client.Bean,
	namespaceRegistry namespace.Registry,
) (MatchingRawClient, error) {
	return clientBean.GetMatchingClient(namespaceRegistry.GetNamespaceName)
}

func MatchingClientProvider(matchingRawClient MatchingRawClient) MatchingClient {
	return matching.NewRetryableClient(
		matchingRawClient,
		common.CreateMatchingClientRetryPolicy(),
		common.IsServiceClientTransientError,
	)
}

func PersistenceConfigProvider(persistenceConfig config.Persistence, dc *dynamicconfig.Collection) *config.Persistence {
	persistenceConfig.TransactionSizeLimit = dynamicconfig.TransactionSizeLimit.Get(dc)
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

func SdkClientFactoryProvider(
	cfg *config.Config,
	tlsConfigProvider encryption.TLSConfigProvider,
	metricsHandler metrics.Handler,
	logger log.SnTaggedLogger,
	resolver membership.GRPCResolver,
	dc *dynamicconfig.Collection,
) (sdk.ClientFactory, error) {
	frontendURL, _, _, frontendTLSConfig, err := getFrontendConnectionDetails(cfg, tlsConfigProvider, resolver)
	if err != nil {
		return nil, err
	}
	return sdk.NewClientFactory(
		frontendURL,
		frontendTLSConfig,
		metricsHandler,
		logger,
		dynamicconfig.WorkerStickyCacheSize.Get(dc),
	), nil
}

func DCRedirectionPolicyProvider(cfg *config.Config) config.DCRedirectionPolicy {
	return cfg.DCRedirectionPolicy
}

func RPCFactoryProvider(
	cfg *config.Config,
	svcName primitives.ServiceName,
	logger log.Logger,
	tlsConfigProvider encryption.TLSConfigProvider,
	resolver membership.GRPCResolver,
	traceInterceptor telemetry.ClientTraceInterceptor,
	monitor membership.Monitor,
) (common.RPCFactory, error) {
	svcCfg := cfg.Services[string(svcName)]
	frontendURL, frontendHTTPURL, frontendHTTPPort, frontendTLSConfig, err := getFrontendConnectionDetails(cfg, tlsConfigProvider, resolver)
	if err != nil {
		return nil, err
	}
	return rpc.NewFactory(
		&svcCfg.RPC,
		svcName,
		logger,
		tlsConfigProvider,
		frontendURL,
		frontendHTTPURL,
		frontendHTTPPort,
		frontendTLSConfig,
		[]grpc.UnaryClientInterceptor{
			grpc.UnaryClientInterceptor(traceInterceptor),
		},
		monitor,
	), nil
}

func FrontendHTTPClientCacheProvider(
	metadata cluster.Metadata,
	tlsConfigProvider encryption.TLSConfigProvider,
) *cluster.FrontendHTTPClientCache {
	return cluster.NewFrontendHTTPClientCache(metadata, tlsConfigProvider)
}

func getFrontendConnectionDetails(
	cfg *config.Config,
	tlsConfigProvider encryption.TLSConfigProvider,
	resolver membership.GRPCResolver,
) (string, string, int, *tls.Config, error) {
	// To simplify the static config, we switch default values based on whether the config
	// defines an "internal-frontend" service. The default for TLS config can be overridden
	// with publicClient.forceTLSConfig, and the default for hostPort can be overridden by
	// explicitly setting hostPort to "membership://internal-frontend" or
	// "membership://frontend".
	_, hasIFE := cfg.Services[string(primitives.InternalFrontendService)]

	forceTLS := cfg.PublicClient.ForceTLSConfig
	if forceTLS == config.ForceTLSConfigAuto {
		if hasIFE {
			forceTLS = config.ForceTLSConfigInternode
		} else {
			forceTLS = config.ForceTLSConfigFrontend
		}
	}

	var frontendTLSConfig *tls.Config
	var err error
	switch forceTLS {
	case config.ForceTLSConfigInternode:
		frontendTLSConfig, err = tlsConfigProvider.GetInternodeClientConfig()
	case config.ForceTLSConfigFrontend:
		frontendTLSConfig, err = tlsConfigProvider.GetFrontendClientConfig()
	default:
		err = fmt.Errorf("invalid forceTLSConfig")
	}
	if err != nil {
		return "", "", 0, nil, fmt.Errorf("unable to load TLS configuration: %w", err)
	}

	frontendURL := cfg.PublicClient.HostPort
	if frontendURL == "" {
		if hasIFE {
			frontendURL = membership.MakeResolverURL(primitives.InternalFrontendService)
		} else {
			frontendURL = membership.MakeResolverURL(primitives.FrontendService)
		}
	}
	frontendHTTPURL := cfg.PublicClient.HTTPHostPort
	if frontendHTTPURL == "" {
		if hasIFE {
			frontendHTTPURL = membership.MakeResolverURL(primitives.InternalFrontendService)
		} else {
			frontendHTTPURL = membership.MakeResolverURL(primitives.FrontendService)
		}

	}

	var frontendHTTPPort int
	if hasIFE {
		frontendHTTPPort = cfg.Services[string(primitives.InternalFrontendService)].RPC.HTTPPort
	} else {
		frontendHTTPPort = cfg.Services[string(primitives.FrontendService)].RPC.HTTPPort
	}

	return frontendURL, frontendHTTPURL, frontendHTTPPort, frontendTLSConfig, nil
}
