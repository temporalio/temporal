// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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

package matching

import (
	"fmt"
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/ringpop"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/matching/configs"
	"google.golang.org/grpc"
)

type (
	MetricsReporter        metrics.Reporter
	UserMetricsReporter    metrics.Reporter
	UserSdkMetricsReporter metrics.Reporter
	SDKReporter            metrics.Reporter
	InstanceId             string
	ServiceName            string
	ServicesConfigMap      map[string]config.Service

	ServiceMetrics struct {
		reporter MetricsReporter
		deprecatedTally tally.Scope
	}
)

func TaggedLoggerProvider(logger log.Logger) (TaggedLogger, error) {
	taggedLogger := log.With(logger, tag.Service(serviceName))
	return taggedLogger, nil
}

func RateLimitInterceptorProvider(serviceConfig *Config) (*interceptor.RateLimitInterceptor, error) {
	rateLimiter := configs.NewPriorityRateLimiter(func() float64 { return float64(serviceConfig.RPS()) })

	rateLimiterInterceptor := interceptor.NewRateLimitInterceptor(
		rateLimiter,
		map[string]int{},
	)
	return rateLimiterInterceptor, nil
}

func MembershipFactoryInitializerProvider(
	svcName ServiceName,
	services ServicesConfigMap,
	membership *config.Membership,
	rpcFactory common.RPCFactory,
) resource.MembershipFactoryInitializerFunc {

	servicePortMap := make(map[string]int)
	for sn, sc := range services { //todomigryz: commented code cfg.Services {
		servicePortMap[sn] = sc.RPC.GRPCPort
	}

	result := func(persistenceBean persistenceClient.Bean, logger log.Logger) (
		resource.MembershipMonitorFactory,
		error,
	) {
		return ringpop.NewRingpopFactory(
			membership,
			rpcFactory.GetRingpopChannel(),
			string(svcName),
			servicePortMap,
			logger,
			persistenceBean.GetClusterMetadataManager(),
		)
	}

	return result
}

// todomigryz: seem to be possible to join with MFIProvider
func MembershipFactoryProvider(
	factoryInitializer resource.MembershipFactoryInitializerFunc,
	taggedLogger TaggedLogger,
	persistenceBean persistenceClient.Bean,
) (resource.MembershipMonitorFactory, error) {
	return factoryInitializer(persistenceBean, taggedLogger)
}

func RPCFactoryProvider(
	svcCfg config.Service,
	logger log.Logger,
	tlsConfigProvider encryption.TLSConfigProvider,
	svcName ServiceName,
) common.RPCFactory {
	return rpc.NewFactory(&svcCfg.RPC, string(svcName), logger, tlsConfigProvider)
}

func GrpcListenerProvider(rpcFactory common.RPCFactory) GRPCListener {
	return rpcFactory.GetGRPCListener()
}

func GrpcServerProvider(
	logger log.Logger,
	metricsInterceptor *interceptor.TelemetryInterceptor,
	rateLimiterInterceptor *interceptor.RateLimitInterceptor,
	rpcFactory common.RPCFactory,
) (*grpc.Server, error) {

	grpcServerOptions, err := rpcFactory.GetInternodeGRPCServerOptions()
	if err != nil {
		logger.Fatal("creating gRPC server options failed", tag.Error(err))
	}

	grpcServerOptions = append(
		grpcServerOptions,
		grpc.ChainUnaryInterceptor(
			rpc.ServiceErrorInterceptor,
			metrics.NewServerMetricsContextInjectorInterceptor(),
			metrics.NewServerMetricsTrailerPropagatorInterceptor(logger),
			metricsInterceptor.Intercept,
			rateLimiterInterceptor.Intercept,
		),
	)
	return grpc.NewServer(grpcServerOptions...), nil
}

// TODO: Seems that all this factory mostly handles singleton logic. We should be able to handle it via IOC.
func MembershipMonitorProvider(membershipFactory resource.MembershipMonitorFactory) (membership.Monitor, error) {
	return membershipFactory.GetMembershipMonitor()
}

// todomigryz: configure autoformatting in goland.
// todomigryz: I believe we config collection can be shared across services. Might be worth injecting it directly.
func ServiceConfigProvider(logger log.Logger, dcClient dynamicconfig.Client) (*Config, error) {
	dcCollection := dynamicconfig.NewCollection(dcClient, logger)
	return NewConfig(dcCollection), nil
}


// todo: This should be able to work without tally.
func RuntimeMetricsReporterProvider(
	logger TaggedLogger,
	tallyScope tally.Scope,
	// instanceId InstanceId, // todo: this is not set in BootstrapParams
) *metrics.RuntimeMetricsReporter {
	return metrics.NewRuntimeMetricsReporter(
		tallyScope,
		time.Minute,
		logger,
		"",
	)
}

func extractTallyScopeForSDK(sdkReporter metrics.Reporter) (tally.Scope, error) {
	if sdkTallyReporter, ok := sdkReporter.(*metrics.TallyReporter); ok {
		return sdkTallyReporter.GetScope(), nil
	} else {
		return nil, fmt.Errorf(
			"SDK reporter is not of Tally type. Unfortunately, SDK only supports Tally for now. "+
				"Please specify prometheusSDK in metrics config with framework type %s", metrics.FrameworkTally,
		)
	}
}

func MetricsReporterProvider(
	logger TaggedLogger,
	userReporter UserMetricsReporter,
	userSdkReporter UserSdkMetricsReporter,
	svcCfg config.Service,
) (ServiceMetrics, error) {
	if userReporter != nil {
		tallyScope, err := extractTallyScopeForSDK(userSdkReporter)
		if err != nil {
			return ServiceMetrics{}, err
		}
		return ServiceMetrics{
			reporter:        userReporter,
			deprecatedTally: tallyScope,
		}, nil
	}

	// todomigryz: remove support of configuring metrics reporter per-service. Sync with Samar.
	// todo: Replace this hack with actually using sdkReporter, Client or Scope.
	serivceReporter, sdkReporter, err := svcCfg.Metrics.InitMetricReporters(logger, nil)
	if err != nil {
		return ServiceMetrics{}, fmt.Errorf(
			"unable to initialize per-service metric client. "+
				"This is deprecated behavior used as fallback, please use global metric config. Error: %w", err,
		)
	}

	// todo: uncomment after removing dependency on tally. See @RuntimeMetricsReporterProvider
	// if serivceReporter != sdkReporter {
	// 	sdkReporter.Stop(logger)
	// }

	tallyScope, err := extractTallyScopeForSDK(sdkReporter)
	if err != nil {
		return ServiceMetrics{}, err
	}

	return ServiceMetrics{
		reporter:        serivceReporter,
		deprecatedTally: tallyScope,
	}, nil
}

func MetricsClientProvider(
	logger TaggedLogger,
	serverReporter MetricsReporter,
	serviceIdx metrics.ServiceIdx,
) (metrics.Client, error) {
	return serverReporter.NewClient(logger, serviceIdx)
}

func RingpopChannelProvider(rpcFactory common.RPCFactory) *tchannel.Channel {
	return rpcFactory.GetRingpopChannel()
}

func ClientBeanProvider(
	persistenceConfig *config.Persistence,
	logger TaggedLogger,
	dcClient dynamicconfig.Client,
	rpcFactory common.RPCFactory,
	membershipMonitor membership.Monitor,
	metricsClient metrics.Client,
	clusterMetadata cluster.Metadata,
) (client.Bean, error) {
	dynamicCollection := dynamicconfig.NewCollection(dcClient, logger)

	return client.NewClientBean(
		client.NewRPCClientFactory(
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

func PersistenceBeanProvider(
	serviceConfig *Config,
	persistenceConfig *config.Persistence,
	metricsClient metrics.Client,
	logger TaggedLogger,
	clusterMetadataConfig *config.ClusterMetadata,
	persistenceServiceResolver resolver.ServiceResolver,
	datastoreFactory     persistenceClient.AbstractDataStoreFactory,
) (persistenceClient.Bean, error) {
	persistenceMaxQPS := serviceConfig.PersistenceMaxQPS
	persistenceGlobalMaxQPS := serviceConfig.PersistenceGlobalMaxQPS
	persistenceBean, err := persistenceClient.NewBeanFromFactory(
		persistenceClient.NewFactory(
			persistenceConfig,
			persistenceServiceResolver,
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
			datastoreFactory,
			clusterMetadataConfig.CurrentClusterName,
			metricsClient,
			logger,
		),
	)
	return persistenceBean, err
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

// todomigryz: needs PersistenceBeanProvider
func MetadataManagerProvider(persistenceBean persistenceClient.Bean) (persistence.MetadataManager, error) {
	return persistenceBean.GetMetadataManager(), nil
}

// todomigryz: seems this can be replaced with constructor
func NamespaceCacheProvider(
	metadataMgr persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	metricsClient metrics.Client,
	logger log.Logger,
) (cache.NamespaceCache, error) {
	namespaceCache := cache.NewNamespaceCache(
		metadataMgr, // persistenceBean.GetMetadataManager(),
		clusterMetadata,
		metricsClient,
		logger,
	)
	return namespaceCache, nil
}

func ThrottledLoggerProvider(logger TaggedLogger, config *Config) (log.ThrottledLogger, error) {
	dynConfigFn := config.ThrottledLogRPS
	throttledLogger := log.NewThrottledLogger(logger, func() float64 { return float64(dynConfigFn()) })
	return throttledLogger, nil
}

func HandlerProvider(
	serviceConfig *Config,
	logger TaggedLogger,
	throttledLogger log.ThrottledLogger,
	metricsClient metrics.Client,
	namespaceCache cache.NamespaceCache,
	clientBean client.Bean,
	membershipMonitor membership.Monitor,
	persistenceBean persistenceClient.Bean,
) (*Handler, error) {

	matchingRawClient, err := clientBean.GetMatchingClient(namespaceCache.GetNamespaceName)
	if err != nil {
		return nil, err
	}

	matchingServiceResolver, err := membershipMonitor.GetResolver(common.MatchingServiceName)
	if err != nil {
		return nil, err
	}

	historyRawClient := clientBean.GetHistoryClient()

	historyClient := history.NewRetryableClient(
		historyRawClient,
		common.CreateHistoryServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)

	engine := NewEngine(
		persistenceBean.GetTaskManager(),
		historyClient,
		matchingRawClient,
		serviceConfig,
		logger,
		metricsClient,
		namespaceCache,
		matchingServiceResolver,
	)

	result := NewHandler(
		serviceConfig,
		logger,
		throttledLogger,
		metricsClient,
		engine,
		namespaceCache,
	)

	return result, nil
}
