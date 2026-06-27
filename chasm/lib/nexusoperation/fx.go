package nexusoperation

import (
	"context"
	"fmt"
	"net/http"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
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
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/telemetry"
	"go.uber.org/fx"
)

const nexusCallbackSourceHeader = "Nexus-Callback-Source"

var Module = fx.Module(
	"chasm.lib.nexusoperation",
	fx.Provide(configProvider),
	fx.Provide(commonnexus.NewCallbackTokenGenerator),
	fx.Provide(endpointRegistryProvider),
	fx.Invoke(endpointRegistryLifetimeHooks),
	fx.Provide(defaultNexusTransportProvider),
	fx.Provide(clientProviderFactory),
	fx.Provide(newHandler),
	fx.Provide(newCancellationBackoffTaskHandler),
	fx.Provide(newCancellationInvocationTaskHandler),
	fx.Provide(newOperationBackoffTaskHandler),
	fx.Provide(newOperationInvocationTaskHandler),
	fx.Provide(newOperationScheduleToCloseTimeoutTaskHandler),
	fx.Provide(newOperationScheduleToStartTimeoutTaskHandler),
	fx.Provide(newOperationStartToCloseTimeoutTaskHandler),
	fx.Provide(newLibrary),
	fx.Invoke(register),
)

var FrontendModule = fx.Module(
	"chasm.lib.nexusoperation.frontend",
	fx.Provide(configProvider),
	fx.Provide(nexusoperationpb.NewNexusOperationServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
	fx.Provide(newComponentOnlyLibrary),
	fx.Invoke(func(l *componentOnlyLibrary, registry *chasm.Registry) error {
		// Frontend needs to register the component in order to serialize ComponentRefs, but doesn't
		// need task handlers.
		return registry.Register(l)
	}),
)

func register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}

func endpointRegistryProvider(
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

func endpointRegistryLifetimeHooks(lc fx.Lifecycle, registry commonnexus.EndpointRegistry) {
	lc.Append(fx.StartStopHook(registry.StartLifecycle, registry.StopLifecycle))
}

// NexusTransportProvider allows customization of the HTTP transport used for Nexus requests.
type NexusTransportProvider func(namespaceID, serviceName string) http.RoundTripper

func defaultNexusTransportProvider() NexusTransportProvider {
	return func(namespaceID, serviceName string) http.RoundTripper {
		return http.DefaultTransport
	}
}

// responseSizeLimiter wraps an http.RoundTripper to limit response body size.
type responseSizeLimiter struct {
	rt http.RoundTripper
}

func (r responseSizeLimiter) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := r.rt.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	response.Body = http.MaxBytesReader(nil, response.Body, rpc.MaxNexusAPIRequestBodyBytes)
	return response, nil
}

type clientProviderCacheKey struct {
	namespaceID, endpointID string
	url                     string
}

func clientProviderFactory(
	httpTransportProvider NexusTransportProvider,
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
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
	m := collection.NewFallibleOnceMap(func(key clientProviderCacheKey) (*http.Client, error) {
		transport := httpTransportProvider(key.namespaceID, key.endpointID)
		return &http.Client{
			Transport: responseSizeLimiter{transport},
		}, nil
	})

	return func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexusrpc.HTTPClient, error) {
		var url string
		var httpClient *http.Client
		var targetNamespaceName string
		originNamespaceName := namespaceID
		if namespaceName, err := namespaceRegistry.GetNamespaceName(namespace.ID(namespaceID)); err == nil {
			originNamespaceName = namespaceName.String()
		}
		setCallbackSource := false
		switch variant := entry.Endpoint.Spec.Target.Variant.(type) {
		case *persistencespb.NexusEndpointTarget_External_:
			url = variant.External.GetUrl()
			var err error
			httpClient, err = m.Get(clientProviderCacheKey{namespaceID, entry.Id, url})
			if err != nil {
				return nil, err
			}
		case *persistencespb.NexusEndpointTarget_Worker_:
			url = cl.BaseURL() + "/" + commonnexus.RouteDispatchNexusTaskByEndpoint.Path(entry.Id)
			httpClient = &cl.Client
			if clusterID != "" {
				setCallbackSource = true
			}
			if namespaceName, err := namespaceRegistry.GetNamespaceName(namespace.ID(variant.Worker.GetNamespaceId())); err == nil {
				targetNamespaceName = namespaceName.String()
			}
		default:
			return nil, serviceerror.NewInternal("got unexpected endpoint target")
		}
		httpCaller := func(r *http.Request) (*http.Response, error) {
			telemetry.MarkNexusHTTPRequest(r, originNamespaceName, targetNamespaceName)
			if setCallbackSource {
				r.Header.Set(nexusCallbackSourceHeader, clusterID)
			}
			resp, callErr := httpClient.Do(r)
			if clusterID != "" {
				commonnexus.SetFailureSourceOnContext(ctx, resp)
			}
			return resp, callErr
		}
		return nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
			BaseURL:    url,
			Service:    service,
			HTTPCaller: httpCaller,
			Serializer: commonnexus.PayloadSerializer,
		})
	}, nil
}
