package matching

import (
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
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


// todomigryz: this belongs in handler file service/matching/handler.go
// todomigryz: this should not use resource :(
func HandlerProvider(
	resource resource.Resource,
	config *Config,
	metricsClient metrics.Client,
	) (*Handler, error) {
	handler := &Handler{
		Resource:      resource,
		config:        config,
		metricsClient: resource.GetMetricsClient(),
		logger:        resource.GetLogger(),
		engine: NewEngine(
			resource.GetTaskManager(),
			resource.GetHistoryClient(),
			resource.GetMatchingRawClient(), // Use non retry client inside matching
			config,
			resource.GetLogger(),
			metricsClient, // resource.GetMetricsClient(),
			resource.GetNamespaceCache(),
			resource.GetMatchingServiceResolver(),
		),
	}

	// prevent from serving requests before matching engine is started and ready
	handler.startWG.Add(1)

	return handler, nil
}

func ThrottledLoggerProvider(logger log.Logger, config *Config) (log.Logger, error) {
	dynConfigFn := config.ThrottledLogRPS
	throttledLogger := log.NewThrottledLogger(logger, func() float64 { return float64(dynConfigFn()) })
	return throttledLogger, nil
}

// todomigryz: providers should move towards respective classes and potentially replace "NewClass" function.
func MatchingServiceProvider(
	logger log.Logger, // todomigryz: this logger should be overriden by per-service logger.
	throttledLogger log.ThrottledLogger,
	serviceConfig *Config,
	grpcServer *grpc.Server,
	handler *Handler,
	) (*Service, error) {

	//serviceResource, err := resource.New(
	//	params,
	//	common.MatchingServiceName,
	//	serviceConfig.PersistenceMaxQPS,
	//	serviceConfig.PersistenceGlobalMaxQPS,
	//	serviceConfig.ThrottledLogRPS,
	//	func(
	//		persistenceBean persistenceClient.Bean,
	//		searchAttributesProvider searchattribute.Provider,
	//		logger log.Logger,
	//	) (persistence.VisibilityManager, error) {
	//		return persistenceBean.GetVisibilityManager(), nil
	//	},
	//)

	//if err != nil {
	//	return nil, err
	//}

	return &Service{
		Resource: serviceResource, // todomigryz: this should be removed in favor of DI
		status:   common.DaemonStatusInitialized,
		config:   serviceConfig,
		server:   grpcServer,
		handler:  handler, // todomigryz: implement handler provider. NewHandler(serviceResource, serviceConfig),
		logger: logger,
		throttledLogger: throttledLogger,
	}, nil
}
