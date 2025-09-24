package callbacks

import (
	"fmt"
	"net/http"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
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
		return func(r *http.Request) (*http.Response, error) {
			return routeRequest(r,
				clusterMetadata,
				httpClientCache,
				localClient,
				logger,
			)

		}
	})
	return m.Get, nil
}
