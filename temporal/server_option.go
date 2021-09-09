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

package temporal

import (
	"net/http"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/rpc/encryption"
)

type (
	ServerOption interface {
		apply(*serverOptions)
	}
)

func WithConfig(cfg *config.Config) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.config = cfg
	})
}

func WithConfigLoader(configDir string, env string, zone string) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.configDir, s.env, s.zone = configDir, env, zone
	})
}

func ForServices(names []string) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.serviceNames = names
	})
}

// InterruptOn interrupts server on the signal from server. If channel is nil Start() will block forever.
func InterruptOn(interruptCh <-chan interface{}) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.blockingStart = true
		s.interruptCh = interruptCh
	})
}

func WithLogger(logger log.Logger) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.logger = logger
	})
}

// Sets optional logger for all frontend operations
func WithNamespaceLogger(namespaceLogger log.Logger) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.namespaceLogger = namespaceLogger
	})
}

// Sets low level authorizer to allow/deny all API calls
func WithAuthorizer(authorizer authorization.Authorizer) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.authorizer = authorizer
	})
}

// Overrides default provider of TLS configuration
func WithTLSConfigFactory(tlsConfigProvider encryption.TLSConfigProvider) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.tlsConfigProvider = tlsConfigProvider
	})
}

// Configures a role mapper for authorization
func WithClaimMapper(claimMapper func(cfg *config.Config) authorization.ClaimMapper) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.claimMapper = claimMapper(s.config)
	})
}

// Configures JWT audience getter for authorization
func WithAudienceGetter(audienceGetter func(cfg *config.Config) authorization.JWTAudienceMapper) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.audienceGetter = audienceGetter(s.config)
	})
}

// Set custom metric reporter
// for (deprecated) Tally it should be tally.BaseStatsReporter
// for Prometheus with framework metrics.FrameworkCustom it should be metrics.Reporter
// not used otherwise
// TODO: replace argument type with metrics.Reporter once tally is deprecated.
func WithCustomMetricsReporter(reporter interface{}) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.metricsReporter = reporter
	})
}

// Set custom persistence service resolver which will convert service name or address value from config to another a....
func WithPersistenceServiceResolver(r resolver.ServiceResolver) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.persistenceServiceResolver = r
	})
}

// Set custom persistence service resolver which will convert service name or address value from config to another a....
func WithElasticsearchHttpClient(c *http.Client) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.elasticseachHttpClient = c
	})
}

// Set custom dynmaic config client
func WithDynamicConfigClient(c dynamicconfig.Client) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.dynamicConfigClient = c
	})
}

// WithCustomDataStoreFactory sets a custom AbstractDataStoreFactory
// NOTE: this option is experimental.
func WithCustomDataStoreFactory(customFactory client.AbstractDataStoreFactory) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.customDataStoreFactory = customFactory
	})
}


// TODO: deprecate methods above. Should use providers instead as they are more flexible.

func WithLoggerProvider(userFunc UserLoggerProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.UserLoggerProvider = userFunc
	})
}

func WithNamespaceLoggerProvider(userFunc UserNamespaceLoggerProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.UserNamespaceLoggerProvider = userFunc
	})
}

func WithAuthorizerProvider(userFunc UserAuthorizerProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.UserAuthorizerProvider = userFunc
	})
}

func WithTlsConfigProvider(userFunc UserTlsConfigProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
			s.UserTlsConfigProvider = userFunc
		})
}

func WithClaimMapperProvider(userFunc UserClaimMapperProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.UserClaimMapperProvider = userFunc
	})
}

func WithAudienceGetterProvider(userFunc UserAudienceGetterProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.UserAudienceGetterProvider = userFunc
	})
}

func WithMetricsReportersProvider(userFunc UserMetricsReportersProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.UserMetricsReportersProvider = userFunc
	})
}

func WithPersistenceServiceResolverProvider(userFunc UserPersistenceServiceResolverProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.UserPersistenceServiceResolverProvider = userFunc
	})
}

func WithElasticseachHttpClientProvider(userFunc UserElasticSeachHttpClientProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.UserElasticSeachHttpClientProvider = userFunc
	})
}

func WithDynamicConfigClientProvider(userFunc UserDynamicConfigClientProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.UserDynamicConfigClientProvider = userFunc
	})
}

func WithCustomDataStoreFactoryProvider(userFunc UserCustomDataStoreFactoryProviderFunc) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.UserCustomDataStoreFactoryProvider = userFunc
	})
}

