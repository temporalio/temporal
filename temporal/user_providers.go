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

package temporal

import (
	"errors"
	"fmt"
	"net/http"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	tlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	esclient "go.temporal.io/server/common/persistence/elasticsearch/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/rpc/encryption"
)

type (
	ServiceNamesList []string
	NamespaceLogger  tlog.Logger
	ESHttpClient     *http.Client
	MetricsReporters struct {
		ServerReporter metrics.Reporter
		SdkReporter    metrics.Reporter
	}
	ServerInterruptCh <-chan interface{}

	ProviderCommonParams struct {
		logger tlog.Logger
		cfg    *config.Config
	}

	UserLoggerProviderFunc                     func(c *config.Config) tlog.Logger
	UserNamespaceLoggerProviderFunc            func(c *config.Config, logger tlog.Logger) NamespaceLogger
	UserMetricsReportersProviderFunc            func(c *config.Config, log tlog.Logger) (*MetricsReporters, error)

	UserAuthorizerProviderFuncParams struct {*ProviderCommonParams}
	UserAuthorizerProviderFunc                 func(params UserAuthorizerProviderFuncParams) (authorization.Authorizer, error)

	UserTlsConfigProviderFuncParams struct {
		*ProviderCommonParams
	}
	UserTlsConfigProviderFunc                  func(params UserTlsConfigProviderFuncParams) (encryption.TLSConfigProvider, error)

	UserClaimMapperProviderFuncParams struct {*ProviderCommonParams}
	UserClaimMapperProviderFunc                func(params UserClaimMapperProviderFuncParams) (authorization.ClaimMapper, error)

	UserAudienceGetterProviderFuncParams struct {*ProviderCommonParams}
	UserAudienceGetterProviderFunc             func(params UserAudienceGetterProviderFuncParams) (authorization.JWTAudienceMapper, error)

	UserPersistenceServiceResolverProviderFuncParams struct {*ProviderCommonParams}
	UserPersistenceServiceResolverProviderFunc func(params UserPersistenceServiceResolverProviderFuncParams) (resolver.ServiceResolver, error)

	UserElasticSeachHttpClientProviderFuncParams struct {
		*ProviderCommonParams
		esConfig *config.Elasticsearch
	}
	UserElasticSeachHttpClientProviderFunc func(params UserElasticSeachHttpClientProviderFuncParams) (*http.Client, error)

	UserDynamicConfigClientProviderFuncParams struct {*ProviderCommonParams}
	UserDynamicConfigClientProviderFunc        func(params UserDynamicConfigClientProviderFuncParams) (dynamicconfig.Client, error)

	UserCustomDataStoreFactoryProviderFuncParams struct {*ProviderCommonParams}
	UserCustomDataStoreFactoryProviderFunc     func(params UserCustomDataStoreFactoryProviderFuncParams) (persistenceClient.AbstractDataStoreFactory, error)
)

func ProviderCommonParamsProvider(logger tlog.Logger, cfg *config.Config) (*ProviderCommonParams, error) {
	result := ProviderCommonParams{logger, cfg}
	return &result, nil
}

func DefaultConfigProvider(opts *serverOptions) (*config.Config, error) {
	if opts.config != nil {
		return opts.config, nil
	}

	env := opts.env
	zone := opts.zone
	configDir := opts.configDir
	cfg, err := config.LoadConfig(env, configDir, zone)
	if err != nil {
		return nil, fmt.Errorf("Unable to load configuration: %v.", err)
	}
	return cfg, nil
}

func DefaultLoggerProvider(cfg *config.Config, opts *serverOptions) tlog.Logger {
	if opts.UserLoggerProvider != nil {
		return opts.UserLoggerProvider(cfg)
	}
	return tlog.NewZapLogger(tlog.BuildZapLogger(cfg.Log))
}

func DefaultNamespaceLoggerProvider(cfg *config.Config, opts *serverOptions, logger tlog.Logger) NamespaceLogger {
	if opts.UserNamespaceLoggerProvider != nil {
		return opts.UserNamespaceLoggerProvider(cfg, logger)
	}
	return logger
}

func DefaultServiceNameListProvider(log tlog.Logger, options *serverOptions) (ServiceNamesList, error) {
	if options.serviceNames == nil {
		return nil, errors.New("please, specify services to run via temporal.ForServices")
	}

	return options.serviceNames, nil
}

func DefaultDynamicConfigClientProvider(pcp *ProviderCommonParams, opts *serverOptions) (dynamicconfig.Client, error) {
	if opts.UserDynamicConfigClientProvider != nil {
		return opts.UserDynamicConfigClientProvider(UserDynamicConfigClientProviderFuncParams{pcp})
	}

	dynamicConfigClient, err := dynamicconfig.NewFileBasedClient(&pcp.cfg.DynamicConfigClient, pcp.logger, InterruptCh())
	if err != nil {
		pcp.logger.Info(
			"Unable to create file based dynamic config client, use no-op config client instead.",
			tag.Error(err),
		)
		dynamicConfigClient = dynamicconfig.NewNoopClient()
	}
	return dynamicConfigClient, nil
}

func DefaultDynamicConfigCollectionProvider(client dynamicconfig.Client, logger tlog.Logger) *dynamicconfig.Collection {
	return dynamicconfig.NewCollection(client, logger)
}

func DefaultAuthorizerProvider(pcp *ProviderCommonParams, opts *serverOptions) (authorization.Authorizer, error) {
	if opts.UserAuthorizerProvider != nil {
		return opts.UserAuthorizerProvider(UserAuthorizerProviderFuncParams{pcp})
	}

	authorizer, err := authorization.GetAuthorizerFromConfig(
		&pcp.cfg.Global.Authorization,
	)
	return authorizer, err
}

func DefaultClaimMapper(pcp *ProviderCommonParams, opts *serverOptions) (authorization.ClaimMapper, error) {
	if opts.UserClaimMapperProvider != nil {
		return opts.UserClaimMapperProvider(UserClaimMapperProviderFuncParams{pcp})
	}

	claimMapper, err := authorization.GetClaimMapperFromConfig(&pcp.cfg.Global.Authorization, pcp.logger)
	return claimMapper, err
}

func DefaultDatastoreFactory(pcp *ProviderCommonParams, opts *serverOptions) (persistenceClient.AbstractDataStoreFactory, error) {
	if opts.UserCustomDataStoreFactoryProvider != nil {
		return opts.UserCustomDataStoreFactoryProvider(UserCustomDataStoreFactoryProviderFuncParams{pcp})
	}
	return nil, nil
}

func DefaultMetricsReportersProvider(
	opts *serverOptions,
	cfg *config.Config,
	logger tlog.Logger) (*MetricsReporters, error) {
	if opts.UserMetricsReportersProvider != nil {
		return opts.UserMetricsReportersProvider(cfg, logger)
	}

	result := &MetricsReporters{}
	if cfg.Global.Metrics == nil {
		return result, nil
	}

	var err error
	result.ServerReporter, result.SdkReporter, err = cfg.Global.Metrics.InitMetricReporters(logger, nil)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func DefaultTLSConfigProvider(
	pcp *ProviderCommonParams,
	opts *serverOptions,
	metricReporters *MetricsReporters,
) (encryption.TLSConfigProvider, error) {
	if opts.UserTlsConfigProvider != nil {
		return opts.UserTlsConfigProvider(UserTlsConfigProviderFuncParams{pcp})
	}

	logger := pcp.logger
	scope, err := extractTallyScopeForSDK(metricReporters.SdkReporter)
	if err != nil {
		return nil, err
	}
	tlsConfigProvider, err := encryption.NewTLSConfigProviderFromConfig(
		pcp.cfg.Global.TLS, scope, logger, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("TLS provider initialization error: %w", err)
	}
	return tlsConfigProvider, nil
}

func DefaultAudienceGetterProvider(pcp *ProviderCommonParams, opts *serverOptions) (authorization.JWTAudienceMapper, error) {
	if opts.UserAudienceGetterProvider != nil {
		return opts.UserAudienceGetterProvider(UserAudienceGetterProviderFuncParams{pcp})
	}
	return nil, nil
}

func DefaultPersistenseServiceResolverProvider(pcp *ProviderCommonParams, opts *serverOptions) (resolver.ServiceResolver, error) {
	if opts.UserPersistenceServiceResolverProvider != nil {
		return opts.UserPersistenceServiceResolverProvider(UserPersistenceServiceResolverProviderFuncParams{pcp})
	}
	return resolver.NewNoopResolver(), nil
}

func DefaultElasticSearchHttpClientProvider(pcp *ProviderCommonParams, opts *serverOptions, esConfig *config.Elasticsearch) (ESHttpClient, error) {
	if opts.UserElasticSeachHttpClientProvider != nil {
		return opts.UserElasticSeachHttpClientProvider(UserElasticSeachHttpClientProviderFuncParams{pcp, esConfig})
	}

	if esConfig == nil {
		return nil, nil
	}

	elasticseachHttpClient, err := esclient.NewAwsHttpClient(esConfig.AWSRequestSigning)
	if err != nil {
		return nil, fmt.Errorf("unable to create AWS HTTP client for Elasticsearch: %w", err)
	}
	return elasticseachHttpClient, nil
}

func DefaultInterruptChProvider(opts *serverOptions) ServerInterruptCh {
	if opts.interruptCh != nil {
		return opts.interruptCh
	}

	return InterruptCh()
}
