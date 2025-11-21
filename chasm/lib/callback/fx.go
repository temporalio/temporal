package callback

import (
	"fmt"
	"net/http"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
)

func register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}

// httpCallerProviderProvider provides an HTTPCallerProvider for CHASM callbacks.
func httpCallerProviderProvider(
	clusterMetadata cluster.Metadata,
	rpcFactory common.RPCFactory,
	httpClientCache *cluster.FrontendHTTPClientCache,
	logger log.Logger,
) (HTTPCallerProvider, error) {
	localClient, err := rpcFactory.CreateLocalFrontendHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("cannot create local frontend HTTP client: %w", err)
	}
	defaultClient := &http.Client{}

	m := collection.NewOnceMap(func(NamespaceIDAndDestination) HTTPCaller {
		return func(r *http.Request) (*http.Response, error) {
			return routeRequest(r,
				clusterMetadata,
				httpClientCache,
				defaultClient,
				localClient,
				logger,
			)
		}
	})
	return m.Get, nil
}

var Module = fx.Module(
	"chasm.lib.callback",
	fx.Provide(configProvider),
	fx.Provide(httpCallerProviderProvider),
	fx.Provide(NewInvocationTaskExecutor),
	fx.Provide(NewBackoffTaskExecutor),
	fx.Provide(newLibrary),
	fx.Invoke(register),
)
