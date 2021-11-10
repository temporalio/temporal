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

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/grpc"
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
		s.serviceNames = make(map[string]struct{})
		for _, name := range names {
			s.serviceNames[name] = struct{}{}
		}
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
		s.elasticsearchHttpClient = c
	})
}

// Set custom dynmaic config client
func WithDynamicConfigClient(c dynamicconfig.Client) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.dynamicConfigClient = c
	})
}

// WithCustomDataStoreFactory sets a custom AbstractDataStoreFactory
// NOTE: this option is experimental and may be changed or removed in future release.
func WithCustomDataStoreFactory(customFactory persistenceclient.AbstractDataStoreFactory) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.customDataStoreFactory = customFactory
	})
}

// WithClientFactoryProvider sets a custom ClientFactoryProvider
// NOTE: this option is experimental and may be changed or removed in future release.
func WithClientFactoryProvider(clientFactoryProvider client.FactoryProvider) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.clientFactoryProvider = clientFactoryProvider
	})
}

// Set custom search attributes mapper which converts search attributes aliases to field names and vice versa.
func WithSearchAttributesMapper(m searchattribute.Mapper) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.searchAttributesMapper = m
	})
}

// WithChainedFrontendGrpcInterceptors sets a chain of ordered custom grpc interceptors that will be invoked for all
// Frontend gRPC API calls. The list of custom interceptors will be appended to the end of the internal
// ServerInterceptors. The custom interceptors will be invoked in the order as they appear in the supplied list, after
// the internal ServerInterceptors.
func WithChainedFrontendGrpcInterceptors(
	interceptors ...grpc.UnaryServerInterceptor,
) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.customInterceptors = interceptors
	})
}
