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

package history

import (
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

var Module = fx.Options(
	fx.Provide(ParamsExpandProvider), // BootstrapParams should be deprecated
	fx.Provide(dynamicconfig.NewCollection),
	fx.Provide(ConfigProvider), // might be worth just using provider for configs.Config directly
	fx.Provide(ResourceProvider),
	fx.Provide(TelemetryInterceptorProvider),
	fx.Provide(RateLimitInterceptorProvider),
	fx.Provide(GrpcServerOptionsProvider),
	fx.Provide(ESProcessorConfigProvider),
	fx.Provide(VisibilityManagerProvider),
	fx.Provide(NewService),
)

func ParamsExpandProvider(params *resource.BootstrapParams) (
	log.Logger,
	dynamicconfig.Client,
	config.Persistence,
	*config.Elasticsearch,
	common.RPCFactory,
) {
	return params.Logger,
		params.DynamicConfigClient,
		params.PersistenceConfig,
		params.ESConfig,
		params.RPCFactory
}

func ConfigProvider(
	dc *dynamicconfig.Collection,
	persistenceConfig config.Persistence,
	esConfig *config.Elasticsearch,
) *configs.Config {
	return configs.NewConfig(dc,
		persistenceConfig.NumHistoryShards,
		persistenceConfig.IsAdvancedVisibilityConfigExist(),
		esConfig.GetVisibilityIndex())
}

func ResourceProvider(params *resource.BootstrapParams, serviceConfig *configs.Config) (resource.Resource, error) {
	return resource.New(
		params,
		common.HistoryServiceName,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.PersistenceGlobalMaxQPS,
		serviceConfig.ThrottledLogRPS,
	)
}

func TelemetryInterceptorProvider(
	logger log.Logger,
	resource resource.Resource,
) *interceptor.TelemetryInterceptor {
	return interceptor.NewTelemetryInterceptor(
		resource.GetNamespaceCache(),
		resource.GetMetricsClient(),
		metrics.HistoryAPIMetricsScopes(),
		logger,
	)

}

func RateLimitInterceptorProvider(
	serviceConfig *configs.Config,
) *interceptor.RateLimitInterceptor {
	return interceptor.NewRateLimitInterceptor(
		configs.NewPriorityRateLimiter(func() float64 { return float64(serviceConfig.RPS()) }),
		map[string]int{},
	)
}

func GrpcServerOptionsProvider(
	logger log.Logger,
	rpcFactory common.RPCFactory,
	telemetryInterceptor *interceptor.TelemetryInterceptor,
	rateLimitInterceptor *interceptor.RateLimitInterceptor,
) []grpc.ServerOption {
	grpcServerOptions, err := rpcFactory.GetInternodeGRPCServerOptions()

	if err != nil {
		logger.Fatal("creating gRPC server options failed", tag.Error(err))
	}

	return append(
		grpcServerOptions,
		grpc.ChainUnaryInterceptor(
			rpc.ServiceErrorInterceptor,
			metrics.NewServerMetricsContextInjectorInterceptor(),
			metrics.NewServerMetricsTrailerPropagatorInterceptor(logger),
			telemetryInterceptor.Intercept,
			rateLimitInterceptor.Intercept,
		),
	)
}

func ESProcessorConfigProvider(
	serviceConfig *configs.Config,
) *elasticsearch.ProcessorConfig {
	return &elasticsearch.ProcessorConfig{
		IndexerConcurrency:       serviceConfig.IndexerConcurrency,
		ESProcessorNumOfWorkers:  serviceConfig.ESProcessorNumOfWorkers,
		ESProcessorBulkActions:   serviceConfig.ESProcessorBulkActions,
		ESProcessorBulkSize:      serviceConfig.ESProcessorBulkSize,
		ESProcessorFlushInterval: serviceConfig.ESProcessorFlushInterval,
		ESProcessorAckTimeout:    serviceConfig.ESProcessorAckTimeout,
	}
}

func VisibilityManagerProvider(
	params *resource.BootstrapParams,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	serviceResource resource.Resource,
	serviceConfig *configs.Config,
) (manager.VisibilityManager, error) {
	return visibility.NewManager(
		params.PersistenceConfig,
		params.PersistenceServiceResolver,
		params.ESConfig.GetVisibilityIndex(),
		params.ESClient,
		esProcessorConfig,
		serviceResource.GetSearchAttributesProvider(),
		params.SearchAttributesMapper,
		serviceConfig.StandardVisibilityPersistenceMaxReadQPS,
		serviceConfig.StandardVisibilityPersistenceMaxWriteQPS,
		serviceConfig.AdvancedVisibilityPersistenceMaxReadQPS,
		serviceConfig.AdvancedVisibilityPersistenceMaxWriteQPS,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false), // history visibility never read
		serviceConfig.AdvancedVisibilityWritingMode,
		params.MetricsClient,
		params.Logger,
	)
}
