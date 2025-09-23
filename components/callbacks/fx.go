package callbacks

import (
	"errors"
	"fmt"
	"net/http"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/queues"
	"go.uber.org/fx"
)

// Header key used to identify callbacks that originate from and target the same cluster.
const callbackSourceHeader = "source"

var Module = fx.Module(
	"component.callbacks",
	fx.Provide(ConfigProvider),
	fx.Provide(HTTPCallerProviderProvider),
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterStateMachine),
	fx.Invoke(RegisterExecutor),
)

var systemCallbackPath = "/" + nexus.RouteCompletionCallbackNoIdentifier.Path("")

func routeInternally(r *http.Request,
	clusterMetadata cluster.Metadata,
	httpClientCache *cluster.FrontendHTTPClientCache,
	localClient *common.FrontendHTTPClient,
	logger log.Logger,
) (*http.Response, error) {
	callbackSource := r.Header.Get(callbackSourceHeader)
	for clusterName, clusterInfo := range clusterMetadata.GetAllClusterInfo() {
		if callbackSource == clusterInfo.ClusterID {
			var frontendClient *common.FrontendHTTPClient
			if clusterMetadata.GetCurrentClusterName() == clusterName {
				frontendClient = localClient
			} else {
				fe, err := httpClientCache.Get(clusterName)
				if err != nil {
					// the behavior here differs from the default
					// the request does not have a scheme and host for us to default to.
					// therefore, if we can't resolve the proper frontend the request is not rouatable
					logger.Error(
						"HTTPCallerProvider unable to get FrontendHTTPClient for callback target cluster. Request not routable",
						tag.SourceCluster(clusterMetadata.GetCurrentClusterName()),
						tag.TargetCluster(clusterName),
						tag.Error(err),
					)
					return nil, fmt.Errorf("could not find frontend client for request: %w", err)
				}
				frontendClient = fe
			}
			r.URL.Scheme = frontendClient.Scheme
			r.URL.Host = frontendClient.Address
			r.URL.Path = systemCallbackPath
			r.Host = frontendClient.Address

			return frontendClient.Do(r)
		}
	}
	return nil, errors.New("could not find frontend client for request")
}

func HTTPCallerProviderProvider(
	clusterMetadata cluster.Metadata,
	rpcFactory common.RPCFactory,
	httpClientCache *cluster.FrontendHTTPClientCache,
	logger log.Logger,
) (HTTPCallerProvider, error) {
	localClient, err := rpcFactory.CreateLocalFrontendHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("cannot create local frontend HTTP client: %w", err)
	}

	m := collection.NewOnceMap(func(queues.NamespaceIDAndDestination) HTTPCaller {
		// Create this once and reuse for all outgoing requests.
		// Note that this may not ever be used but it cheap enough to create and is better to avoid the complexities of
		// lazily creating it.
		client := &http.Client{}

		return func(r *http.Request) (*http.Response, error) {
			if r.Header == nil || r.Header.Get(callbackSourceHeader) == "" {
				return client.Do(r)
			}
			if r.URL.String() == nexus.SystemCallbackURL {
				return routeInternally(r,
					clusterMetadata,
					httpClientCache,
					localClient,
					logger,
				)
			}

			callbackSource := r.Header.Get(callbackSourceHeader)
			for clusterName, clusterInfo := range clusterMetadata.GetAllClusterInfo() {
				if callbackSource == clusterInfo.ClusterID {
					var frontendClient *common.FrontendHTTPClient
					if clusterMetadata.GetCurrentClusterName() == clusterName {
						frontendClient = localClient
					} else {
						frontendClient, err = httpClientCache.Get(clusterName)
						if err != nil {
							logger.Warn(
								"HTTPCallerProvider unable to get FrontendHTTPClient for callback target cluster. Using default HTTP client.",
								tag.SourceCluster(clusterMetadata.GetCurrentClusterName()),
								tag.TargetCluster(clusterName),
								tag.Error(err),
							)
							return client.Do(r)
						}
					}
					r.URL.Scheme = frontendClient.Scheme
					r.URL.Host = frontendClient.Address
					r.Host = frontendClient.Address
					return frontendClient.Do(r)
				}
			}

			// We don't know this calling cluster, use the default client.
			return client.Do(r)
		}
	})
	return m.Get, nil
}
