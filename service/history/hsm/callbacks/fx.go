package callbacks

import (
	"fmt"
	"net/http"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/rpc/httpfaults"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	"go.uber.org/fx"
)

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
	namespaceRegistry namespace.Registry,
	rpcFactory common.RPCFactory,
	httpClientCache *cluster.FrontendHTTPClientCache,
	logger log.Logger,
	transportWrapper httpfaults.TransportWrapper,
	config *Config,
) (HTTPCallerProvider, error) {
	localClient, err := rpcFactory.CreateLocalFrontendHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("cannot create local frontend HTTP client: %w", err)
	}
	defaultTransport, err := common.NewHTTPTransport(nil)
	if err != nil {
		return nil, err
	}
	defaultClient := &http.Client{Transport: transportWrapper.Wrap(defaultTransport)}
	callbackTokenGenerator := commonnexus.NewCallbackTokenGenerator()

	m := collection.NewOnceMap(func(key queuescommon.NamespaceIDAndDestination) HTTPCaller {
		caller := func(r *http.Request) (*http.Response, error) {
			return routeRequest(httpfaults.WithScope(r, httpfaults.Scope{NamespaceID: namespace.ID(key.NamespaceID)}),
				clusterMetadata,
				namespaceRegistry,
				httpClientCache,
				callbackTokenGenerator,
				defaultClient,
				localClient,
				logger,
				config.InspectSourceHeader(),
			)
		}
		return caller
	})
	return m.Get, nil
}
