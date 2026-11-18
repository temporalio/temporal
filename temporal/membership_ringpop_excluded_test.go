//go:build temporal_minimal && !temporal_with_ringpop

package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/fx"
)

func TestServerOptionsRequireStaticHosts(t *testing.T) {
	t.Parallel()

	for name, hosts := range map[string]map[primitives.ServiceName]static.Hosts{
		"nil":   nil,
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := ServerOptionsProvider([]ServerOption{
				WithConfig(membershipTestConfig()),
				WithStaticHosts(hosts),
			})
			require.ErrorContains(t, err, "WithStaticHosts")
			require.ErrorContains(t, err, "temporal_with_ringpop")
		})
	}
}

func TestServerOptionsRejectIncompleteStaticHostsBeforePersistence(t *testing.T) {
	t.Parallel()

	for _, service := range DefaultServices {
		t.Run(service, func(t *testing.T) {
			t.Parallel()

			hosts := membershipTestHosts()
			delete(hosts, primitives.ServiceName(service))
			_, err := ServerOptionsProvider([]ServerOption{
				WithConfig(membershipTestConfig()),
				WithStaticHosts(hosts),
			})
			require.ErrorIs(t, err, missingServiceInStaticHosts)
			require.ErrorContains(t, err, service)
			require.ErrorContains(t, err, "WithStaticHosts")
			require.ErrorContains(t, err, "temporal_with_ringpop")
		})
	}
}

func TestServerOptionsRejectInvalidStaticSelfBeforePersistence(t *testing.T) {
	t.Parallel()

	for _, service := range []primitives.ServiceName{primitives.FrontendService, primitives.InternalFrontendService} {
		for name, hosts := range map[string]static.Hosts{
			"missing hosts": {},
			"missing self":  {All: []string{"127.0.0.1:7233"}},
			"self outside all": {
				All:  []string{"127.0.0.1:7233"},
				Self: "127.0.0.1:9999",
			},
		} {
			t.Run(string(service)+"/"+name, func(t *testing.T) {
				t.Parallel()

				hostsByService := membershipTestHosts()
				hostsByService[service] = hosts
				_, err := ServerOptionsProvider([]ServerOption{
					WithConfig(membershipTestConfig()),
					WithStaticHosts(hostsByService),
					ForServices([]string{string(service)}),
				})
				require.ErrorContains(t, err, string(service))
				require.ErrorContains(t, err, "WithStaticHosts")
				require.ErrorContains(t, err, "temporal_with_ringpop")
			})
		}
	}
}

func TestServerOptionsAcceptStaticHosts(t *testing.T) {
	t.Parallel()

	for _, service := range []primitives.ServiceName{primitives.FrontendService, primitives.InternalFrontendService} {
		t.Run(string(service), func(t *testing.T) {
			t.Parallel()

			hosts := membershipTestHosts()
			for service, host := range hosts {
				host.Self = ""
				hosts[service] = host
			}
			hosts[service] = static.SingleLocalHost("127.0.0.1:7233")
			options := newServerOptions([]ServerOption{
				WithConfig(membershipTestConfig()),
				WithStaticHosts(hosts),
				ForServices([]string{string(service)}),
			})
			require.NoError(t, options.loadAndValidate())
		})
	}
}

func TestCommonServiceOptionsRejectDynamicMembership(t *testing.T) {
	t.Parallel()

	params := membershipTestParams()
	app := fx.New(params.GetCommonServiceOptions(primitives.FrontendService), fx.NopLogger)
	require.ErrorContains(t, app.Err(), "WithStaticHosts")
	require.ErrorContains(t, app.Err(), "temporal_with_ringpop")
}

func membershipTestConfig() *config.Config {
	return &config.Config{
		Persistence: config.Persistence{
			DefaultStore:    "test",
			VisibilityStore: "test",
			DataStores: map[string]config.DataStore{
				"test": {SQL: &config.SQL{PluginName: "membership-validation-test"}},
			},
		},
		Services: map[string]config.Service{
			"frontend":          {},
			"internal-frontend": {},
		},
	}
}
