package callback

import (
	"fmt"
	"net/http"

	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	"go.uber.org/fx"
)

// httpCallerProviderProvider provides an HTTPCallerProvider for CHASM callbacks.
func httpCallerProviderProvider(
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	rpcFactory common.RPCFactory,
	httpClientCache *cluster.FrontendHTTPClientCache,
	logger log.Logger,
) (HTTPCallerProvider, error) {
	localClient, err := rpcFactory.CreateLocalFrontendHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("cannot create local frontend HTTP client: %w", err)
	}
	defaultClient := &http.Client{}
	callbackTokenGenerator := commonnexus.NewCallbackTokenGenerator()

	m := collection.NewOnceMap(func(queuescommon.NamespaceIDAndDestination) HTTPCaller {
		return func(r *http.Request) (*http.Response, error) {
			return routeRequest(r,
				clusterMetadata,
				namespaceRegistry,
				httpClientCache,
				callbackTokenGenerator,
				defaultClient,
				localClient,
				logger,
			)
		}
	})
	return m.Get, nil
}

// Slimmed-down module just containing the CHASM components, but not their implementation.
var FrontendModule = fx.Module(
	"callback-frontend",
	fx.Provide(callbackspb.NewCallbackExecutionServiceLayeredClient),
	fx.Provide(ConfigProvider),
	fx.Provide(NewFrontendHandler),

	fx.Provide(newComponentOnlyLibrary),
	fx.Invoke(func(registry *chasm.Registry, coLibrary *componentOnlyLibrary) error {
		return registry.Register(coLibrary)
	}),
)

var HistoryModule = fx.Module(
	"chasm.lib.callback",
	fx.Provide(ConfigProvider),
	fx.Provide(httpCallerProviderProvider),
	fx.Provide(newInvocationTaskHandler),
	fx.Provide(newBackoffTaskHandler),
	fx.Provide(newCallbackExecutionHandler),
	fx.Provide(NewScheduleToCloseTimeoutTaskHandler),

	// Register the Callback CHASM component on startup.
	fx.Provide(newLibrary),
	fx.Invoke(func(registry *chasm.Registry, library *library) error {
		return registry.Register(library)
	}),
)
