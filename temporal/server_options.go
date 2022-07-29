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
	"fmt"
	"net/http"

	"golang.org/x/exp/slices"
	"google.golang.org/grpc"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/searchattribute"
)

type (
	serverOptions struct {
		serviceNames map[string]struct{}

		config    *config.Config
		configDir string
		env       string
		zone      string

		interruptCh   <-chan interface{}
		blockingStart bool

		logger                     log.Logger
		namespaceLogger            log.Logger
		authorizer                 authorization.Authorizer
		tlsConfigProvider          encryption.TLSConfigProvider
		claimMapper                authorization.ClaimMapper
		audienceGetter             authorization.JWTAudienceMapper
		persistenceServiceResolver resolver.ServiceResolver
		elasticsearchHttpClient    *http.Client
		dynamicConfigClient        dynamicconfig.Client
		customDataStoreFactory     persistenceClient.AbstractDataStoreFactory
		clientFactoryProvider      client.FactoryProvider
		searchAttributesMapper     searchattribute.Mapper
		customInterceptors         []grpc.UnaryServerInterceptor
		metricProvider             metrics.MetricsHandler
	}
)

func newServerOptions(opts []ServerOption) *serverOptions {
	so := &serverOptions{
		// Set defaults here.
		persistenceServiceResolver: resolver.NewNoopResolver(),
	}
	for _, opt := range opts {
		opt.apply(so)
	}

	return so
}

func (so *serverOptions) loadAndValidate() error {
	for serviceName := range so.serviceNames {
		if !slices.Contains(Services, serviceName) {
			return fmt.Errorf("invalid service %q in service list %v", serviceName, so.serviceNames)
		}
	}

	if so.config == nil {
		err := so.loadConfig()
		if err != nil {
			return fmt.Errorf("unable to load config: %w", err)
		}
	}

	err := so.validateConfig()
	if err != nil {
		return fmt.Errorf("config validation error: %w", err)
	}

	return nil
}

func (so *serverOptions) loadConfig() error {
	so.config = &config.Config{}
	err := config.Load(so.env, so.configDir, so.zone, so.config)
	if err != nil {
		return fmt.Errorf("config file corrupted: %w", err)
	}

	return nil
}

func (so *serverOptions) validateConfig() error {
	if err := so.config.Validate(); err != nil {
		return err
	}

	for name := range so.serviceNames {
		if _, ok := so.config.Services[name]; !ok {
			return fmt.Errorf("%q service is missing in config", name)
		}
	}
	return nil
}
