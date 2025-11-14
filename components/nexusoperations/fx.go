package nexusoperations

import (
	"context"
	"fmt"
	"net/http"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
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
	dc *dynamicconfig.Collection,
	logger log.Logger,
	metricsHandler metrics.Handler,
) commonnexus.EndpointRegistry {
	registryConfig := commonnexus.NewEndpointRegistryConfig(dc)
	return commonnexus.NewEndpointRegistry(
		registryConfig,
		matchingClient,
		endpointManager,
		logger,
		metricsHandler,
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
	rpcFactory common.RPCFactory,
) (ClientProvider, error) {
	cl, err := rpcFactory.CreateLocalFrontendHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("cannot create local frontend HTTP client: %w", err)
	}
	var clusterID string

	if clusterInfo, ok := clusterMetadata.GetAllClusterInfo()[clusterMetadata.GetCurrentClusterName()]; ok {
		clusterID = clusterInfo.ClusterID
	}
	// TODO(bergundy): This should use an LRU or other form of cache that supports eviction.
	m := collection.NewFallibleOnceMap(func(key clientProviderCacheKey) (*http.Client, error) {
		transport := httpTransportProvider(key.namespaceID, key.endpointID)
		return &http.Client{
			Transport: ResponseSizeLimiter{transport},
		}, nil
	})

	return func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexusrpc.HTTPClient, error) {
		var url string
		var httpClient *http.Client
		httpCaller := httpClient.Do
		switch variant := entry.Endpoint.Spec.Target.Variant.(type) {
		case *persistencespb.NexusEndpointTarget_External_:
			url = variant.External.GetUrl()
			var err error
			httpClient, err = m.Get(clientProviderCacheKey{namespaceID, entry.Id, url})
			if err != nil {
				return nil, err
			}
			if clusterID != "" {
				httpCaller = func(r *http.Request) (*http.Response, error) {
					resp, callErr := httpClient.Do(r)
					commonnexus.SetFailureSourceOnContext(ctx, resp)
					return resp, callErr
				}
			}
		case *persistencespb.NexusEndpointTarget_Worker_:
			url = cl.BaseURL() + "/" + commonnexus.RouteDispatchNexusTaskByEndpoint.Path(entry.Id)
			httpClient = &cl.Client
			if clusterID != "" {
				httpCaller = func(r *http.Request) (*http.Response, error) {
					r.Header.Set(NexusCallbackSourceHeader, clusterID)
					resp, callErr := httpClient.Do(r)
					commonnexus.SetFailureSourceOnContext(ctx, resp)
					return resp, callErr
				}
			}
		default:
			return nil, serviceerror.NewInternal("got unexpected endpoint target")
		}
		// still need to override the caller
		return nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
			BaseURL:    url,
			Service:    service,
			HTTPCaller: httpCaller,
			Serializer: commonnexus.PayloadSerializer,
		})
	}, nil
}

func CallbackTokenGeneratorProvider() *commonnexus.CallbackTokenGenerator {
	return commonnexus.NewCallbackTokenGenerator()
}
