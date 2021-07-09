package matching

import (
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/matching/configs"
	"google.golang.org/grpc"
)



func PriorityRateLimiterProvider(serviceConfig *Config)  (quotas.RequestRateLimiter, error){
	result := configs.NewPriorityRateLimiter(func() float64 { return float64(serviceConfig.RPS()) })
	return result, nil
}

func RateLimitInterceptorProvider(rateLimiter quotas.RequestRateLimiter) (*interceptor.RateLimitInterceptor, error) {
	rateLimiterInterceptor := interceptor.NewRateLimitInterceptor(
		rateLimiter,
		map[string]int{},
	)
	return rateLimiterInterceptor, nil
}

// todomigryz: metrics client comes from bootstrap params that I'm not using atm. Although these should be injected now.
func MetricsInterceptorProvider(logger log.Logger, metricsClient metrics.Client, namespaceCache cache.NamespaceCache) (*interceptor.TelemetryInterceptor, error){
	metricsInterceptor := interceptor.NewTelemetryInterceptor(
		namespaceCache,
		metricsClient,
		metrics.MatchingAPIMetricsScopes(),
		logger,
	)
	return metricsInterceptor, nil
}

func GrpcServerProvider(logger log.Logger,
	metricsInterceptor *interceptor.TelemetryInterceptor,
	rateLimiterInterceptor *interceptor.RateLimitInterceptor,
	rpcFactory common.RPCFactory) (*grpc.Server, error) {

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

// todomigryz: configure autoformatting in goland.
// todomigryz: I believe we config collection can be shared across services. Might be worth injecting it directly.
func ServiceConfigProvider(logger log.Logger, dcClient dynamicconfig.Client) (*Config, error) {
	dcCollection := dynamicconfig.NewCollection(dcClient, logger)
	return NewConfig(dcCollection), nil
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

func ThrottledLoggerProvider(logger log.Logger, config *Config) (log.Logger, error) {
	dynConfigFn := config.ThrottledLogRPS
	throttledLogger := log.NewThrottledLogger(logger, func() float64 { return float64(dynConfigFn()) })
	return throttledLogger, nil
}
