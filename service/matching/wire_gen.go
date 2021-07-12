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
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/rpc/interceptor"
)

// Injectors from wire.go:

// todomigryz: implement this method. Replace NewService method.
// todomigryz: Need to come up with proper naming convention for initialize vs factory methods.
func InitializeMatchingService(serviceName2 ServiceName, logger log.Logger, params *resource.BootstrapParams, dcClient dynamicconfig.Client, metricsReporter metrics.Reporter, svcCfg config.Service, clusterMetadata *config.ClusterMetadata, tlsConfigProvider encryption.TLSConfigProvider, services ServicesConfigMap, membership *config.Membership) (*Service, error) {
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
	matchingMetricsReporter, err := MetricsReporterProvider(taggedLogger, metricsReporter, svcCfg)
	if err != nil {
		return nil, err
	}
	serviceIdx := _wireServiceIdxValue
	client, err := MetricsClientProvider(taggedLogger, matchingMetricsReporter, serviceIdx)
	if err != nil {
		return nil, err
	}
	bean, err := PersistenceBeanProvider(matchingConfig, params, client, taggedLogger)
	if err != nil {
		return nil, err
	}
	metadataManager, err := MetadataManagerProvider(bean)
	if err != nil {
		return nil, err
	}
	metadata := ClusterMetadataProvider(clusterMetadata)
	namespaceCache := cache.NewNamespaceCache(metadataManager, metadata, client, logger)
	matchingAPIMetricsScopes := metrics.NewMatchingAPIMetricsScopes()
	telemetryInterceptor := interceptor.NewTelemetryInterceptor(namespaceCache, client, matchingAPIMetricsScopes, logger)
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
	membershipFactoryInitializerFunc := MembershipFactoryInitializerProvider(serviceName2, services, membership, rpcFactory)
	membershipMonitorFactory, err := MembershipFactoryProvider(membershipFactoryInitializerFunc, taggedLogger, bean)
	if err != nil {
		return nil, err
	}
	monitor, err := MembershipMonitorProvider(membershipMonitorFactory)
	if err != nil {
		return nil, err
	}
	clientBean, err := ClientBeanProvider(params, taggedLogger, dcClient, rpcFactory, monitor, client, metadata)
	if err != nil {
		return nil, err
	}
	channel := RingpopChannelProvider(rpcFactory)
	handler, err := HandlerProvider(matchingConfig, taggedLogger, throttledLogger, client, namespaceCache, clientBean, monitor, bean)
	if err != nil {
		return nil, err
	}
	runtimeMetricsReporter := RuntimeMetricsReporterProvider(params, taggedLogger)
	service, err := NewService(params, taggedLogger, throttledLogger, matchingConfig, bean, namespaceCache, server, grpcListener, monitor, clientBean, channel, handler, runtimeMetricsReporter)
	if err != nil {
		return nil, err
	}
	return service, nil
}

var (
	_wireServiceIdxValue = metrics.ServiceIdx(metrics.Matching)
)
