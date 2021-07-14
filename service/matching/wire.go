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

// +build wireinject

package matching

import (
	"github.com/google/wire"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/rpc/interceptor"

	persistenceClient "go.temporal.io/server/common/persistence/client"
)

// todomigryz: svcName can be hardcoded here. We switch on svc name one layer above.
func InitializeMatchingService(
	serviceName ServiceName,
	logger log.Logger,
	dcClient dynamicconfig.Client,
	metricsReporter UserMetricsReporter,
	sdkMetricsReporter UserSdkMetricsReporter,
	svcCfg config.Service,
	clusterMetadata *config.ClusterMetadata,
	tlsConfigProvider encryption.TLSConfigProvider,
	services ServicesConfigMap,
	membershipConfig *config.Membership,
	persistenceConfig *config.Persistence,
	persistenceServiceResolver resolver.ServiceResolver,
	datastoreFactory persistenceClient.AbstractDataStoreFactory,
) (*Service, error) {
	wire.Build(
		wire.Value(metrics.ServiceIdx(metrics.Matching)),
		ServiceConfigProvider,
		TaggedLoggerProvider,
		ThrottledLoggerProvider,
		MetricsReporterProvider,
		wire.FieldsOf(new(ServiceMetrics), "reporter"),
		wire.FieldsOf(new(ServiceMetrics), "deprecatedTally"),
		MetricsClientProvider,
		PersistenceBeanProvider,
		ClusterMetadataProvider,
		MetadataManagerProvider,
		cache.NewNamespaceCache,
		metrics.NewMatchingAPIMetricsScopes,
		interceptor.NewTelemetryInterceptor,
		RateLimitInterceptorProvider,
		MembershipFactoryProvider,
		RPCFactoryProvider,
		GrpcServerProvider,
		GrpcListenerProvider,
		MembershipMonitorProvider,
		ClientBeanProvider,
		RingpopChannelProvider,
		HandlerProvider,
		RuntimeMetricsReporterProvider,
		MembershipFactoryInitializerProvider,
		NewService,
	)
	return nil, nil
}

func InitializeTestMatchingService(
	serviceName ServiceName,
	logger log.Logger,
	dcClient dynamicconfig.Client,
	metricsReporter UserMetricsReporter,
	sdkMetricsReporter UserSdkMetricsReporter,
	svcCfg config.Service,
	clusterMetadata *config.ClusterMetadata,
	tlsConfigProvider encryption.TLSConfigProvider,
	membershipFactory resource.MembershipFactoryInitializerFunc,
	persistenceConfig *config.Persistence,
	persistenceServiceResolver resolver.ServiceResolver,
	datastoreFactory persistenceClient.AbstractDataStoreFactory,
	archivalMetadata archiver.ArchivalMetadata,
	archiverProvider provider.ArchiverProvider,
) (*Service, error) {
	wire.Build(
		wire.Value(metrics.ServiceIdx(metrics.Matching)),
		ServiceConfigProvider,
		TaggedLoggerProvider,
		ThrottledLoggerProvider,
		MetricsReporterProvider,
		wire.FieldsOf(new(ServiceMetrics), "reporter"),
		wire.FieldsOf(new(ServiceMetrics), "deprecatedTally"),
		MetricsClientProvider,
		PersistenceBeanProvider,
		ClusterMetadataProvider,
		MetadataManagerProvider,
		cache.NewNamespaceCache,
		metrics.NewMatchingAPIMetricsScopes,
		interceptor.NewTelemetryInterceptor,
		RateLimitInterceptorProvider,
		MembershipFactoryProvider,
		RPCFactoryProvider,
		GrpcServerProvider,
		GrpcListenerProvider,
		MembershipMonitorProvider,
		ClientBeanProvider,
		RingpopChannelProvider,
		HandlerProvider,
		RuntimeMetricsReporterProvider,
		NewService,
	)
	return nil, nil
}
