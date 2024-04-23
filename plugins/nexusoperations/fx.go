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
	"net/http"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.uber.org/fx"

	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/queues"
)

var Module = fx.Module(
	"plugin.nexusoperations",
	fx.Provide(ConfigProvider),
	fx.Provide(ClientProviderFactory),
	fx.Provide(DefaultNexusTransportProvider),
	fx.Provide(CallbackTokenGeneratorProvider),
	fx.Invoke(RegisterStateMachines),
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterEventDefinitions),
	fx.Invoke(RegisterExecutor),
)

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

func ClientProviderFactory(namespaceRegistry namespace.Registry, httpTransportProvider NexusTransportProvider) ClientProvider {
	// TODO(bergundy): This should use an LRU or other form of cache that supports eviction.
	m := collection.NewFallibleOnceMap(func(key clientProviderCacheKey) (*nexus.Client, error) {
		transport := httpTransportProvider(key.NamespaceID, key.Destination)
		httpClient := &http.Client{
			Transport: ResponseSizeLimiter{transport},
		}
		return nexus.NewClient(nexus.ClientOptions{
			ServiceBaseURL: key.url,
			HTTPCaller:     httpClient.Do,
			Serializer:     commonnexus.PayloadSerializer,
		})
	})
	return func(key queues.NamespaceIDAndDestination, spec *nexuspb.OutgoingServiceSpec) (*nexus.Client, error) {
		return m.Get(clientProviderCacheKey{key, spec.Url})
	}
}

func CallbackTokenGeneratorProvider() *commonnexus.CallbackTokenGenerator {
	return commonnexus.NewCallbackTokenGenerator()
}
