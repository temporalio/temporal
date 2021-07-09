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

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/matching/configs"
	"google.golang.org/grpc"
)

type (
	MetricsReporter metrics.Reporter
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

// todomigryz: configure autoformatting in goland.
// todomigryz: I believe we config collection can be shared across services. Might be worth injecting it directly.
func ServiceConfigProvider(logger log.Logger, dcClient dynamicconfig.Client) (*Config, error) {
	dcCollection := dynamicconfig.NewCollection(dcClient, logger)
	return NewConfig(dcCollection), nil
}

func MetricsReporterProvider(
	logger TaggedLogger,
	userReporter metrics.Reporter,
	svcCfg config.Service,
) (MetricsReporter, error) {
	if userReporter != nil {
		return userReporter, nil
	}

	// todomigryz: remove support of configuring metrics reporter per-service. Sync with Samar.
	// todo: Replace this hack with actually using sdkReporter, Client or Scope.
	serverReporter, sdkReporter, err := svcCfg.Metrics.InitMetricReporters(logger, nil)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to initialize per-service metric client. "+
				"This is deprecated behavior used as fallback, please use global metric config. Error: %w", err,
		)
	}
	if serverReporter != sdkReporter {
		sdkReporter.Stop(logger)
	}
	return serverReporter, nil
}

func MetricsClientProvider(
	logger TaggedLogger,
	serverReporter MetricsReporter,
	serviceIdx metrics.ServiceIdx,
) (metrics.Client, error) {
	return serverReporter.NewClient(logger, serviceIdx)
}

func PersistenceBeanProvider(
	serviceConfig *Config,
	params *resource.BootstrapParams,
	metricsClient metrics.Client,
	logger TaggedLogger,
) (persistenceClient.Bean, error) {
	persistenceMaxQPS := serviceConfig.PersistenceMaxQPS
	persistenceGlobalMaxQPS := serviceConfig.PersistenceGlobalMaxQPS
	persistenceBean, err := persistenceClient.NewBeanFromFactory(
		persistenceClient.NewFactory(
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
