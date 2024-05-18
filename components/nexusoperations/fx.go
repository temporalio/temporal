// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexusoperations

import (
	"context"
	"net/http"

	"github.com/nexus-rpc/sdk-go/nexus"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.uber.org/fx"

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/queues"
)

var Module = fx.Module(
	"component.nexusoperations",
	fx.Provide(ConfigProvider),
	fx.Provide(ClientProviderFactory),
	fx.Provide(DefaultNexusTransportProvider),
	fx.Provide(CallbackTokenGeneratorProvider),
	fx.Provide(EndpointRegistryProvider),
	fx.Invoke(EndpointRegistryLifetimeHooks),
	fx.Provide(EndpointCheckerProvider),
	fx.Invoke(RegisterStateMachines),
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterEventDefinitions),
	fx.Invoke(RegisterExecutor),
)

func EndpointRegistryProvider(
	matchingClient resource.MatchingClient,
	endpointManager persistence.NexusEndpointManager,
	logger log.Logger,
	dc *dynamicconfig.Collection,
) *commonnexus.EndpointRegistry {
	registryConfig := commonnexus.NewEndpointRegistryConfig(dc)
	return commonnexus.NewEndpointRegistry(
		registryConfig,
		matchingClient,
		endpointManager,
		logger,
	)
}

func EndpointRegistryLifetimeHooks(lc fx.Lifecycle, registry *commonnexus.EndpointRegistry) {
	lc.Append(fx.StartStopHook(registry.StartLifecycle, registry.StopLifecycle))
}

func EndpointCheckerProvider(reg *commonnexus.EndpointRegistry) EndpointChecker {
	return func(ctx context.Context, namespaceName, endpointName string) error {
		_, err := reg.GetByName(ctx, endpointName)
		return err
	}
}

// NexusTransportProvider type alias allows a provider to customize the default implementation specifically for Nexus.
type NexusTransportProvider func(namespaceID, serviceName string) http.RoundTripper

func DefaultNexusTransportProvider() NexusTransportProvider {
	return func(namespaceID, serviceName string) http.RoundTripper {
		// In the future, we'll want to inject headers and certs here.
		// For now this is must be done externally via a custom transport provider.
		return http.DefaultTransport
	}
}

type clientProviderCacheKey struct {
	queues.NamespaceIDAndDestination
	// URL is part of the cache key in case the service configuration is modified to use a new URL after caching the
	// client for the service.
	url string
}

func ClientProviderFactory(
	namespaceRegistry namespace.Registry,
	endpointRegistry *commonnexus.EndpointRegistry,
	httpTransportProvider NexusTransportProvider,
) ClientProvider {
	// TODO(bergundy): This should use an LRU or other form of cache that supports eviction.
	m := collection.NewFallibleOnceMap(func(key clientProviderCacheKey) (*http.Client, error) {
		transport := httpTransportProvider(key.NamespaceID, key.Destination)
		return &http.Client{
			Transport: ResponseSizeLimiter{transport},
		}, nil
	})
	return func(ctx context.Context, key queues.NamespaceIDAndDestination, service string) (*nexus.Client, error) {
		endpoint, err := endpointRegistry.GetByName(ctx, key.Destination)
		if err != nil {
			return nil, err
		}
		var url string
		var httpClient *http.Client
		switch variant := endpoint.Spec.Target.Variant.(type) {
		case *nexuspb.EndpointTarget_External_:
			url = variant.External.GetUrl()
			httpClient, err = m.Get(clientProviderCacheKey{key, url})
			if err != nil {
				return nil, err
			}
		case *nexuspb.EndpointTarget_Worker_:
			// TODO(bergundy): support worker targets
			return nil, serviceerror.NewInternal("worker targets not yet implemented")
		default:
			return nil, serviceerror.NewInternal("got unexpected endpoint target")
		}
		return nexus.NewClient(nexus.ClientOptions{
			BaseURL:    url,
			Service:    service,
			HTTPCaller: httpClient.Do,
			Serializer: commonnexus.PayloadSerializer,
		})
	}
}

func CallbackTokenGeneratorProvider() *commonnexus.CallbackTokenGenerator {
	return commonnexus.NewCallbackTokenGenerator()
}
