package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/fx"
)

func TestCommonServiceOptionsStaticMembership(t *testing.T) {
	t.Parallel()

	params := membershipTestParams()
	params.StaticServiceHosts = membershipTestHosts()
	var monitor membership.Monitor
	var hostInfo membership.HostInfoProvider
	app := fx.New(
		params.GetCommonServiceOptions(primitives.FrontendService),
		fx.Populate(&monitor, &hostInfo),
		fx.NopLogger,
	)
	require.NoError(t, app.Err())
	require.Equal(t, "127.0.0.1:7233", hostInfo.HostInfo().GetAddress())
	for service, address := range map[primitives.ServiceName]string{
		primitives.FrontendService: "127.0.0.1:7233",
		primitives.HistoryService:  "127.0.0.1:7234",
		primitives.MatchingService: "127.0.0.1:7235",
		primitives.WorkerService:   "127.0.0.1:7239",
	} {
		resolver, err := monitor.GetResolver(service)
		require.NoError(t, err)
		host, err := resolver.Lookup("workflow")
		require.NoError(t, err)
		require.Equal(t, address, host.GetAddress())
	}
}

func membershipTestParams() ServiceProviderParamsCommon {
	return ServiceProviderParamsCommon{
		Cfg:                        &config.Config{},
		ClusterMetadata:            &cluster.Config{},
		Logger:                     log.NewNoopLogger(),
		MetricsHandler:             metrics.NoopMetricsHandler,
		EventLoggerProvider:        lognoop.NewLoggerProvider(),
		PersistenceFactoryProvider: PersistenceFactoryProvider(),
	}
}

func membershipTestHosts() map[primitives.ServiceName]static.Hosts {
	return map[primitives.ServiceName]static.Hosts{
		primitives.FrontendService: static.SingleLocalHost("127.0.0.1:7233"),
		primitives.HistoryService:  static.SingleLocalHost("127.0.0.1:7234"),
		primitives.MatchingService: static.SingleLocalHost("127.0.0.1:7235"),
		primitives.WorkerService:   static.SingleLocalHost("127.0.0.1:7239"),
	}
}
