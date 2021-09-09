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
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.uber.org/dig"
)

func InjectMatchingServiceCommonProviders(dc *dig.Container) error {
	if err := dc.Provide(func() metrics.ServiceIdx {return metrics.ServiceIdx(metrics.Matching)}); err != nil {
		return err
	}
	if err := dc.Provide(ServiceConfigProvider); err != nil {
		return err
	}
	if err := dc.Provide(TaggedLoggerProvider); err != nil {
		return err
	}
	if err := dc.Provide(ThrottledLoggerProvider); err != nil {
		return err
	}
	if err := dc.Provide(MetricsReporterProvider); err != nil {
		return err
	}

	if err := dc.Provide(MetricsClientProvider); err != nil {
		return err
	}
	if err := dc.Provide(PersistenceBeanProvider); err != nil {
		return err
	}
	if err := dc.Provide(ClusterMetadataProvider); err != nil {
		return err
	}
	if err := dc.Provide(MetadataManagerProvider); err != nil {
		return err
	}
	if err := dc.Provide(cache.NewNamespaceCache); err != nil {
		return err
	}
	if err := dc.Provide(metrics.NewMatchingAPIMetricsScopes); err != nil {
		return err
	}
	if err := dc.Provide(interceptor.NewTelemetryInterceptor); err != nil {
		return err
	}
	if err := dc.Provide(RateLimitInterceptorProvider); err != nil {
		return err
	}
	if err := dc.Provide(MembershipFactoryProvider); err != nil {
		return err
	}

	if err := dc.Provide(GrpcServerProvider); err != nil {
		return err
	}
	if err := dc.Provide(GrpcListenerProvider); err != nil {
		return err
	}
	if err := dc.Provide(MembershipMonitorProvider); err != nil {
		return err
	}
	if err := dc.Provide(ClientBeanProvider); err != nil {
		return err
	}
	if err := dc.Provide(RingpopChannelProvider); err != nil {
		return err
	}
	if err := dc.Provide(HandlerProvider); err != nil {
		return err
	}
	if err := dc.Provide(RuntimeMetricsReporterProvider); err != nil {
		return err
	}
	if err := dc.Provide(NewService); err != nil {
		return err
	}
	return nil
}

func InjectMatchingServiceProviders(dc *dig.Container) error {
	if err := InjectMatchingServiceCommonProviders(dc); err != nil {
		return err
	}

	if err := dc.Provide(RPCFactoryProvider); err != nil {
		return err
	}
	if err := dc.Provide(MembershipFactoryInitializerProvider); err != nil {
		return err
	}

	return nil
}

func InjectCommonValueProviders(
	dc *dig.Container,
	logger log.Logger,
		dcClient dynamicconfig.Client,
		metricsReporter UserMetricsReporter,
		sdkMetricsReporter UserSdkMetricsReporter,
		svcCfg config.Service,
		clusterMetadata *config.ClusterMetadata,
		tlsConfigProvider encryption.TLSConfigProvider,
		persistenceConfig *config.Persistence,
		persistenceServiceResolver resolver.ServiceResolver,
		datastoreFactory persistenceClient.AbstractDataStoreFactory,
) error {
	if err := dc.Provide(func() log.Logger {return logger}); err != nil {
		return err
	}
	if err := dc.Provide(func() dynamicconfig.Client {return dcClient}); err != nil {
		return err
	}
	if err := dc.Provide(func() UserMetricsReporter {return metricsReporter}); err != nil {
		return err
	}
	if err := dc.Provide(func() UserSdkMetricsReporter {return sdkMetricsReporter}); err != nil {
		return err
	}
	if err := dc.Provide(func() config.Service {return svcCfg}); err != nil {
		return err
	}
	if err := dc.Provide(func() *config.ClusterMetadata {return clusterMetadata}); err != nil {
		return err
	}
	if err := dc.Provide(func() encryption.TLSConfigProvider {return tlsConfigProvider}); err != nil {
		return err
	}
	if err := dc.Provide(func() *config.Persistence {return persistenceConfig}); err != nil {
		return err
	}
	if err := dc.Provide(func() resolver.ServiceResolver {return persistenceServiceResolver}); err != nil {
		return err
	}
	if err := dc.Provide(func() persistenceClient.AbstractDataStoreFactory {return datastoreFactory}); err != nil {
		return err
	}
	return nil
}

func InitializeMatchingService(
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
	dc := dig.New()
	if err := InjectCommonValueProviders(
		dc,
		logger,
		dcClient,
		metricsReporter,
		sdkMetricsReporter,
		svcCfg,
		clusterMetadata,
		tlsConfigProvider,
		persistenceConfig,
		persistenceServiceResolver,
		datastoreFactory,
	); err != nil {
		return nil, err
	}

	if err := dc.Provide(func() ServicesConfigMap {return services}); err != nil {
		return nil, err
	}
	if err := dc.Provide(func() *config.Membership {return membershipConfig}); err != nil {
		return nil, err
	}

	var err error
	if err = InjectMatchingServiceProviders(dc); err != nil {
		return nil, err
	}

	var result *Service

	if err = dc.Invoke(func(svc *Service) error { result = svc; return nil }); err != nil {
		return nil, err
	}

	return result, nil
}


func InitializeTestMatchingService(
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
	rpcFactory common.RPCFactory,
) (*Service, error) {
	dc := dig.New()

	if err := InjectCommonValueProviders(
		dc,
		logger,
		dcClient,
		metricsReporter,
		sdkMetricsReporter,
		svcCfg,
		clusterMetadata,
		tlsConfigProvider,
		persistenceConfig,
		persistenceServiceResolver,
		datastoreFactory,
		); err != nil {
		return nil, err
	}

	if err := dc.Provide(func() resource.MembershipFactoryInitializerFunc {return membershipFactory}); err != nil {
		return nil, err
	}
	if err := dc.Provide(func() archiver.ArchivalMetadata {return archivalMetadata}); err != nil {
		return nil, err
	}
	if err := dc.Provide(func() provider.ArchiverProvider {return archiverProvider}); err != nil {
		return nil, err
	}
	if err := dc.Provide(func() common.RPCFactory {return rpcFactory}); err != nil {
		return nil, err
	}

	var err error

	if err := InjectMatchingServiceCommonProviders(dc); err != nil {
		return nil, err
	}

	var result *Service

	if err = dc.Invoke(func(svc *Service) error { result = svc; return nil }); err != nil {
		return nil, err
	}

	return result, nil
}
