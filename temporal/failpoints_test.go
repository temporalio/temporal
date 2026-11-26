package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/failpointadapter"
	"go.temporal.io/server/common/testing/failpointrules"
	"go.temporal.io/server/common/testing/grpcfailpoints"
	"go.temporal.io/server/common/testing/httpfailpoints"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/testx/failpoint"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

// fakeDataStoreFactory is the data store factory of a custom data store.
type fakeDataStoreFactory struct {
	persistence.DataStoreFactory
}

type fakeAbstractDataStoreFactory struct {
	created *fakeDataStoreFactory
}

func (f fakeAbstractDataStoreFactory) NewFactory(
	config.CustomDatastoreConfig, resolver.ServiceResolver, string, log.Logger, metrics.Handler, serialization.Serializer,
) persistence.DataStoreFactory {
	return f.created
}

func newDataStoreFactory(source *failpoint.Registry) (persistence.DataStoreFactory, *fakeDataStoreFactory) {
	created := &fakeDataStoreFactory{}
	cfg := &config.Persistence{
		DefaultStore: "custom",
		DataStores:   map[string]config.DataStore{"custom": {CustomDataStoreConfig: &config.CustomDatastoreConfig{Name: "custom"}}},
	}
	factory := persistenceclient.DataStoreFactoryProvider(
		"cluster", resolver.NewNoopResolver(), cfg, fakeAbstractDataStoreFactory{created: created},
		log.NewNoopLogger(), metrics.NoopMetricsHandler, telemetry.NoopTracerProvider, nil, source,
	)
	return factory, created
}

// TestResolveFailpoints checks that only the failpoints.enabled config setting enables failpoints,
// that the server warns when it does, and that it says nothing otherwise.
func TestResolveFailpoints(t *testing.T) {
	t.Parallel()

	const enabledMessage = "FAILPOINTS ARE ENABLED (failpoints.enabled in the static config): tests and tools can make " +
		"this server fail, stall or return errors. This is not meant for production use."
	tests := []struct {
		name        string
		enabled     bool
		wantMessage string
	}{
		{name: "disabled"},
		{name: "enabled", enabled: true, wantMessage: enabledMessage},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logs := logger.StartCapture()

			registry := resolveFailpoints(tc.enabled, logger)

			require.Equal(t, tc.enabled, registry != nil)
			var messages []string
			for _, record := range logs.Snapshot() {
				require.Equal(t, testlogger.Warn, record.Level)
				messages = append(messages, record.Message)
			}
			if tc.wantMessage == "" {
				require.Empty(t, messages)
			} else {
				require.Equal(t, []string{tc.wantMessage}, messages)
			}
		})
	}
}

// TestFailpointsGenerators checks that the gRPC fault interceptor, the HTTP fault wrapper and the
// persistence failpoint injector are only installed if failpoints are enabled.
func TestFailpointsGenerators(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		enabled bool
		want    bool
	}{
		"disabled": {},
		"enabled":  {enabled: true, want: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			registry := resolveFailpoints(tc.enabled, log.NewNoopLogger())

			require.Equal(t, tc.want, grpcfailpoints.NewGenerator(registry, failpointadapter.Namespaces{}, false) != nil)
			require.Equal(t, tc.want, httpfailpoints.NewGenerator(registry, failpointadapter.Namespaces{}) != nil)
			factory, created := newDataStoreFactory(registry)
			require.Equal(t, tc.want, factory != created, "the persistence factory is wrapped for failpoints")

			if !tc.want {
				// No rules, no BUGGIFY and no randomized settings: the modules ignore even invalid settings.
				client := dynamicconfig.NewMemoryClient()
				client.OverrideSetting(failpointrules.Setting, []any{map[string]any{"key": "on", "action": "Of"}})
				wrapped, buggifyStatic, err := serverbuggify.RandomizeSettings(client, log.NewNoopLogger(), registry != nil,
					config.FailpointsBuggify{Seed: 5, Trace: true})
				require.NoError(t, err)
				require.Same(t, client, wrapped)
				require.Equal(t, serverbuggify.StaticConfig{}, buggifyStatic)
				app := fxtest.New(t,
					fx.Supply(
						registry,
						fx.Annotate(client, fx.As(new(dynamicconfig.Client))),
						fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger))),
					),
					failpointrules.Module,
					serverbuggify.Module,
					failpointrules.ReleaseModule,
				)
				app.RequireStart()
				app.RequireStop()
			}
		})
	}
}

