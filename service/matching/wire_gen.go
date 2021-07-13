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

// Code generated by Wire. DO NOT EDIT.

//go:generate go run github.com/google/wire/cmd/wire
//+build !wireinject

package matching

import (
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/rpc/interceptor"
)

// Injectors from wire.go:

// todomigryz: svcName can be hardcoded here. We switch on svc name one layer above.
func InitializeMatchingService(serviceName2 ServiceName, logger log.Logger, dcClient dynamicconfig.Client, metricsReporter UserMetricsReporter, sdkMetricsReporter UserSdkMetricsReporter, svcCfg config.Service, clusterMetadata *config.ClusterMetadata, tlsConfigProvider encryption.TLSConfigProvider, services ServicesConfigMap, membershipConfig *config.Membership, persistenceConfig *config.Persistence, persistenceServiceResolver resolver.ServiceResolver, datastoreFactory client.AbstractDataStoreFactory) (*Service, error) {
	taggedLogger, err := TaggedLoggerProvider(logger)
	if err != nil {
		return nil, err
	}
	matchingConfig, err := ServiceConfigProvider(logger, dcClient)
	if err != nil {
		return nil, err
	}
	throttledLogger, err := ThrottledLoggerProvider(taggedLogger, matchingConfig)
	if err != nil {
		return nil, err
	}
	serviceMetrics, err := MetricsReporterProvider(taggedLogger, metricsReporter, sdkMetricsReporter, svcCfg)
	if err != nil {
		return nil, err
	}
	matchingMetricsReporter := serviceMetrics.reporter
	serviceIdx := _wireServiceIdxValue
	metricsClient, err := MetricsClientProvider(taggedLogger, matchingMetricsReporter, serviceIdx)
	if err != nil {
		return nil, err
	}
	bean, err := PersistenceBeanProvider(matchingConfig, persistenceConfig, metricsClient, taggedLogger, clusterMetadata, persistenceServiceResolver, datastoreFactory)
	if err != nil {
		return nil, err
	}
	metadataManager, err := MetadataManagerProvider(bean)
	if err != nil {
		return nil, err
	}
	metadata := ClusterMetadataProvider(clusterMetadata)
	namespaceCache := cache.NewNamespaceCache(metadataManager, metadata, metricsClient, logger)
	matchingAPIMetricsScopes := metrics.NewMatchingAPIMetricsScopes()
	telemetryInterceptor := interceptor.NewTelemetryInterceptor(namespaceCache, metricsClient, matchingAPIMetricsScopes, logger)
	rateLimitInterceptor, err := RateLimitInterceptorProvider(matchingConfig)
	if err != nil {
		return nil, err
	}
	rpcFactory := RPCFactoryProvider(svcCfg, logger, tlsConfigProvider, serviceName2)
	server, err := GrpcServerProvider(logger, telemetryInterceptor, rateLimitInterceptor, rpcFactory)
	if err != nil {
		return nil, err
	}
	grpcListener := GrpcListenerProvider(rpcFactory)
	membershipFactoryInitializerFunc := MembershipFactoryInitializerProvider(serviceName2, services, membershipConfig, rpcFactory)
	membershipMonitorFactory, err := MembershipFactoryProvider(membershipFactoryInitializerFunc, taggedLogger, bean)
	if err != nil {
		return nil, err
	}
	monitor, err := MembershipMonitorProvider(membershipMonitorFactory)
	if err != nil {
		return nil, err
	}
	clientBean, err := ClientBeanProvider(persistenceConfig, taggedLogger, dcClient, rpcFactory, monitor, metricsClient, metadata)
	if err != nil {
		return nil, err
	}
	channel := RingpopChannelProvider(rpcFactory)
	handler, err := HandlerProvider(matchingConfig, taggedLogger, throttledLogger, metricsClient, namespaceCache, clientBean, monitor, bean)
	if err != nil {
		return nil, err
	}
	scope := serviceMetrics.deprecatedTally
	runtimeMetricsReporter := RuntimeMetricsReporterProvider(taggedLogger, scope)
	service, err := NewService(taggedLogger, throttledLogger, matchingConfig, bean, namespaceCache, server, grpcListener, monitor, clientBean, channel, handler, runtimeMetricsReporter, scope)
	if err != nil {
		return nil, err
	}
	return service, nil
}

var (
	_wireServiceIdxValue = metrics.ServiceIdx(metrics.Matching)
)

// todomigryz: svcName can be hardcoded here. We switch on svc name one layer above.
func InitializeTestMatchingService(serviceName2 ServiceName, logger log.Logger, dcClient dynamicconfig.Client, metricsReporter UserMetricsReporter, sdkMetricsReporter UserSdkMetricsReporter, svcCfg config.Service, clusterMetadata *config.ClusterMetadata, tlsConfigProvider encryption.TLSConfigProvider, persistenceConfig *config.Persistence, persistenceServiceResolver resolver.ServiceResolver, datastoreFactory client.AbstractDataStoreFactory, membershipFactory resource.MembershipFactoryInitializerFunc) (*Service, error) {
	taggedLogger, err := TaggedLoggerProvider(logger)
	if err != nil {
		return nil, err
	}
	matchingConfig, err := ServiceConfigProvider(logger, dcClient)
	if err != nil {
		return nil, err
	}
	throttledLogger, err := ThrottledLoggerProvider(taggedLogger, matchingConfig)
	if err != nil {
		return nil, err
	}
	serviceMetrics, err := MetricsReporterProvider(taggedLogger, metricsReporter, sdkMetricsReporter, svcCfg)
	if err != nil {
		return nil, err
	}
	matchingMetricsReporter := serviceMetrics.reporter
	serviceIdx := _wireMetricsServiceIdxValue
	metricsClient, err := MetricsClientProvider(taggedLogger, matchingMetricsReporter, serviceIdx)
	if err != nil {
		return nil, err
	}
	bean, err := PersistenceBeanProvider(matchingConfig, persistenceConfig, metricsClient, taggedLogger, clusterMetadata, persistenceServiceResolver, datastoreFactory)
	if err != nil {
		return nil, err
	}
	metadataManager, err := MetadataManagerProvider(bean)
	if err != nil {
		return nil, err
	}
	metadata := ClusterMetadataProvider(clusterMetadata)
	namespaceCache := cache.NewNamespaceCache(metadataManager, metadata, metricsClient, logger)
	matchingAPIMetricsScopes := metrics.NewMatchingAPIMetricsScopes()
	telemetryInterceptor := interceptor.NewTelemetryInterceptor(namespaceCache, metricsClient, matchingAPIMetricsScopes, logger)
	rateLimitInterceptor, err := RateLimitInterceptorProvider(matchingConfig)
	if err != nil {
		return nil, err
	}
	rpcFactory := RPCFactoryProvider(svcCfg, logger, tlsConfigProvider, serviceName2)
	server, err := GrpcServerProvider(logger, telemetryInterceptor, rateLimitInterceptor, rpcFactory)
	if err != nil {
		return nil, err
	}
	grpcListener := GrpcListenerProvider(rpcFactory)
	membershipMonitorFactory, err := MembershipFactoryProvider(membershipFactory, taggedLogger, bean)
	if err != nil {
		return nil, err
	}
	monitor, err := MembershipMonitorProvider(membershipMonitorFactory)
	if err != nil {
		return nil, err
	}
	clientBean, err := ClientBeanProvider(persistenceConfig, taggedLogger, dcClient, rpcFactory, monitor, metricsClient, metadata)
	if err != nil {
		return nil, err
	}
	channel := RingpopChannelProvider(rpcFactory)
	handler, err := HandlerProvider(matchingConfig, taggedLogger, throttledLogger, metricsClient, namespaceCache, clientBean, monitor, bean)
	if err != nil {
		return nil, err
	}
	scope := serviceMetrics.deprecatedTally
	runtimeMetricsReporter := RuntimeMetricsReporterProvider(taggedLogger, scope)
	service, err := NewService(taggedLogger, throttledLogger, matchingConfig, bean, namespaceCache, server, grpcListener, monitor, clientBean, channel, handler, runtimeMetricsReporter, scope)
	if err != nil {
		return nil, err
	}
	return service, nil
}

var (
	_wireMetricsServiceIdxValue = metrics.ServiceIdx(metrics.Matching)
)
