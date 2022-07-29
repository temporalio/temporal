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

	"google.golang.org/grpc"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/searchattribute"
)

type (
	ServerOption interface {
		apply(*serverOptions)
	}
)

// WithConfig sets a custom configuration
func WithConfig(cfg *config.Config) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.config = cfg
	})
}

// WithConfigLoader sets a custom configuration load
func WithConfigLoader(configDir string, env string, zone string) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.configDir, s.env, s.zone = configDir, env, zone
	})
}

// ForServices indicates which supplied services (e.g. frontend, history, matching, worker) within the server to start
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

// WithLogger sets a custom logger
func WithLogger(logger log.Logger) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.logger = logger
	})
}

// WithNamespaceLogger sets an optional logger for all frontend operations
func WithNamespaceLogger(namespaceLogger log.Logger) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.namespaceLogger = namespaceLogger
	})
}

// WithAuthorizer sets a low level authorizer to allow/deny all API calls
func WithAuthorizer(authorizer authorization.Authorizer) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.authorizer = authorizer
	})
}

// WithTLSConfigFactory overrides default provider of TLS configuration
func WithTLSConfigFactory(tlsConfigProvider encryption.TLSConfigProvider) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.tlsConfigProvider = tlsConfigProvider
	})
}

// WithClaimMapper configures a role mapper for authorization
func WithClaimMapper(claimMapper func(cfg *config.Config) authorization.ClaimMapper) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.claimMapper = claimMapper(s.config)
	})
}

// WithAudienceGetter configures JWT audience getter for authorization
func WithAudienceGetter(audienceGetter func(cfg *config.Config) authorization.JWTAudienceMapper) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.audienceGetter = audienceGetter(s.config)
	})
}

// WithPersistenceServiceResolver sets a custom persistence service resolver which will convert service name or address value from config to another address
func WithPersistenceServiceResolver(r resolver.ServiceResolver) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.persistenceServiceResolver = r
	})
}

// WithElasticsearchHttpClient sets a custom HTTP client which is used to make requests to Elasticsearch
func WithElasticsearchHttpClient(c *http.Client) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.elasticsearchHttpClient = c
	})
}

// WithDynamicConfigClient sets custom dynamic config client for reading dynamic configuration.
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

// WithSearchAttributesMapper sets a custom search attributes mapper which converts search attributes aliases to field names and vice versa.
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

// WithCustomerMetricsProvider sets a custom implementation of the metrics.MetricsHandler interface
// metrics.MetricsHandler is the base interface for publishing metric events
func WithCustomMetricsHandler(provider metrics.MetricsHandler) ServerOption {
	return newApplyFuncContainer(func(s *serverOptions) {
		s.metricProvider = provider
	})
}
