//+build wireinject

package temporal

import (
	"github.com/google/wire"
	"github.com/urfave/cli/v2"
	tlog "go.temporal.io/server/common/log"
)


// todomigryz: create server provider that receives a struct with dependencies
// todomigryz: wire init struct with dependencies using wire tools

func InitializeDefaultUserProviderSet(c *cli.Context) (wire.ProviderSet) {
	return wire.NewSet(
		DefaultConfigProvider,
		DefaultLogger,
		DefaultDynamicConfigClientProvider,
		DefaultAuthorizerProvider,
		DefaultClaimMapper,
		DefaultServiceNameListProvider,
		DefaultDatastoreFactory,
		DefaultMetricsReportersProvider,
		DefaultTLSConfigProvider,
		DefaultDynamicConfigCollectionProvider,
		DefaultAudienceGetterProvider,
		DefaultPersistenseServiceResolverProvider,
		DefaultElasticSearchHttpClientProvider,
		)
}

var UserSet = wire.NewSet(
	DefaultConfigProvider,
	DefaultLogger,
	wire.Bind(new(NamespaceLogger), new(tlog.Logger)),
	DefaultDynamicConfigClientProvider,
	DefaultAuthorizerProvider,
	DefaultClaimMapper,
	DefaultServiceNameListProvider,
	DefaultDatastoreFactory,
	DefaultMetricsReportersProvider,
	DefaultTLSConfigProvider,
	DefaultDynamicConfigCollectionProvider,
	DefaultAudienceGetterProvider,
	DefaultPersistenseServiceResolverProvider,
	DefaultElasticSearchHttpClientProvider,
	DefaultInterruptChProvider,
)

var serverSet = wire.NewSet(
	ServicesProvider,
	ServerProvider,
	AdvancedVisibilityStoreProvider,
	ESClientProvider,
	ESConfigProvider,
	AdvancedVisibilityWritingModeProvider,
)

func InitializeServer(c *cli.Context) (*Server, error) {
	wire.Build(
		UserSet,
		serverSet,
		)
	return nil, nil
}
