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
	"go.temporal.io/api/serviceerror"
	"go.uber.org/fx"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
)

var Module = fx.Module(
	"component.nexusoperations",
	fx.Provide(ConfigProvider),
	fx.Provide(ClientProviderFactory),
	fx.Provide(DefaultNexusTransportProvider),
	fx.Provide(CallbackTokenGeneratorProvider),
	fx.Provide(EndpointRegistryProvider),
	fx.Invoke(EndpointRegistryLifetimeHooks),
	fx.Invoke(RegisterStateMachines),
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterEventDefinitions),
	fx.Invoke(RegisterExecutor),
)

const NexusCallbackSourceHeader = "Nexus-Callback-Source"

func EndpointRegistryProvider(
	matchingClient resource.MatchingClient,
	endpointManager persistence.NexusEndpointManager,
	logger log.Logger,
	dc *dynamicconfig.Collection,
) commonnexus.EndpointRegistry {
	registryConfig := commonnexus.NewEndpointRegistryConfig(dc)
	return commonnexus.NewEndpointRegistry(
		registryConfig,
		matchingClient,
		endpointManager,
		logger,
	)
}

func EndpointRegistryLifetimeHooks(lc fx.Lifecycle, registry commonnexus.EndpointRegistry) {
	lc.Append(fx.StartStopHook(registry.StartLifecycle, registry.StopLifecycle))
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
	namespaceID, endpointID string
	// URL is part of the cache key in case the service configuration is modified to use a new URL after caching the
	// client for the service.
	url string
}

func ClientProviderFactory(
	namespaceRegistry namespace.Registry,
	endpointRegistry commonnexus.EndpointRegistry,
	httpTransportProvider NexusTransportProvider,
	clusterMetadata cluster.Metadata,
	httpClientCache *cluster.FrontendHTTPClientCache,
) ClientProvider {
	// TODO(bergundy): This should use an LRU or other form of cache that supports eviction.
	m := collection.NewFallibleOnceMap(func(key clientProviderCacheKey) (*http.Client, error) {
		transport := httpTransportProvider(key.namespaceID, key.endpointID)
		return &http.Client{
			Transport: ResponseSizeLimiter{transport},
		}, nil
	})
	return func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexus.Client, error) {
		var url string
		var httpClient *http.Client
		switch variant := entry.Endpoint.Spec.Target.Variant.(type) {
		case *persistencespb.NexusEndpointTarget_External_:
			url = variant.External.GetUrl()
			var err error
			httpClient, err = m.Get(clientProviderCacheKey{namespaceID, entry.Id, url})
			if err != nil {
				return nil, err
			}
		case *persistencespb.NexusEndpointTarget_Worker_:
			cl, err := httpClientCache.Get(clusterMetadata.GetCurrentClusterName())
			if err != nil {
				return nil, err
			}
			url = cl.BaseURL() + "/" + commonnexus.RouteDispatchNexusTaskByEndpoint.Path(entry.Id)
			httpClient = &cl.Client
		default:
			return nil, serviceerror.NewInternal("got unexpected endpoint target")
		}
		httpCaller := httpClient.Do
		if clusterInfo, ok := clusterMetadata.GetAllClusterInfo()[clusterMetadata.GetCurrentClusterName()]; ok {
			httpCaller = func(r *http.Request) (*http.Response, error) {
				r.Header.Set(NexusCallbackSourceHeader, clusterInfo.ClusterID)
				return httpClient.Do(r)
			}
		}
		return nexus.NewClient(nexus.ClientOptions{
			BaseURL:    url,
			Service:    service,
			HTTPCaller: httpCaller,
			Serializer: commonnexus.PayloadSerializer,
		})
	}
}

func CallbackTokenGeneratorProvider() *commonnexus.CallbackTokenGenerator {
	return commonnexus.NewCallbackTokenGenerator()
}
