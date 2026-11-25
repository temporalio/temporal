package temporal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/failpointrules"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/grpcfaultstest"
	"go.temporal.io/server/common/testing/httpfaultstest"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func lookupEnv(env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := env[name]
		return value, ok
	}
}

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

func newDataStoreFactory(source *failpoints.Source) (persistence.DataStoreFactory, *fakeDataStoreFactory) {
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

// TestResolveFailpoints_RequiresConfigAndEnv checks that neither the config setting nor the
// environment variable alone enables failpoints.
func TestResolveFailpoints_RequiresConfigAndEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		configEnabled bool
		env           map[string]string
		want          bool
	}{
		{name: "neither"},
		{name: "only config", configEnabled: true},
		{name: "only env", env: map[string]string{failpoints.EnvVar: "1"}},
		{name: "config and env", configEnabled: true, env: map[string]string{failpoints.EnvVar: "1"}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logs := logger.StartCapture()
			metricsHandler := metricstest.NewCaptureHandler()
			metricsCapture := metricsHandler.StartCapture()

			source := resolveFailpoints(tc.configEnabled, lookupEnv(tc.env), logger, metricsHandler)

			require.Equal(t, tc.want, source.Failpoints() != nil)
			warned := false
			for _, record := range logs.Snapshot() {
				if record.Level == testlogger.Warn && record.Message != "" {
					warned = true
					require.Contains(t, record.Tags, tag.NewStringTag("failpoints-enabled-by", "config failpoints.enabled and environment "+failpoints.EnvVar+"=1"))
				}
			}
			require.Equal(t, tc.want, warned)

			recordings := metricsCapture.SnapshotMetric(metrics.FailpointsEnabled.Name())
			require.Len(t, recordings, 1)
			wantValue := 0
			if tc.want {
				wantValue = 1
			}
			require.InDelta(t, wantValue, recordings[0].Value, 0)
			require.Equal(t, string(primitives.ServerService), recordings[0].Tags["service_name"])
		})
	}
}

// TestFailpointsOff checks that with the switch off nothing of the failpoints is installed: no
// registry, no fault generators, no persistence wrapper, and no rules or
// BUGGIFY.
func TestFailpointsOff(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]struct {
		configEnabled bool
		env           map[string]string
	}{
		"neither":     {},
		"only config": {configEnabled: true},
		"only env":    {env: map[string]string{failpoints.EnvVar: "1"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := resolveFailpoints(cfg.configEnabled, lookupEnv(cfg.env), log.NewNoopLogger(), metrics.NoopMetricsHandler)

			// No registry.
			require.Nil(t, source.Failpoints())

			// No gRPC fault interceptor or HTTP fault wrapper.
			for _, service := range []primitives.ServiceName{
				primitives.FrontendService, primitives.HistoryService, primitives.MatchingService, primitives.WorkerService,
			} {
				require.Nil(t, grpcfaultstest.NewGenerator(source, nil, service), service)
			}
			require.Nil(t, httpfaultstest.NewGenerator(source))

			// The persistence factory is not wrapped for failpoints.
			factory, created := newDataStoreFactory(source)
			require.Same(t, created, factory)

			// No rules: the module ignores even invalid settings.
			client := dynamicconfig.NewMemoryClient()
			client.OverrideSetting(failpointrules.Setting, []any{map[string]any{"key": "on", "action": "Of"}})
			collection := dynamicconfig.NewCollection(client, log.NewNoopLogger())
			collection.Start()
			t.Cleanup(collection.Stop)
			app := fxtest.New(t,
				fx.Supply(source, collection, fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger)))),
				failpointrules.Module,
				failpointrules.ReleaseModule,
			)
			app.RequireStart()
			app.RequireStop()
			require.Nil(t, source.Failpoints())
		})
	}
}

// TestFailpointsOn is the counterpart of TestFailpointsOff: with the switch on, the pieces that it
// installs are installed.
func TestFailpointsOn(t *testing.T) {
	t.Parallel()

	source := resolveFailpoints(true, lookupEnv(map[string]string{failpoints.EnvVar: "1"}), log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NotNil(t, source.Failpoints())
	require.NotNil(t, grpcfaultstest.NewGenerator(source, nil, primitives.HistoryService))
	require.NotNil(t, httpfaultstest.NewGenerator(source))
	factory, created := newDataStoreFactory(source)
	require.NotSame(t, created, factory)
}

// TestFailpointsOff_InvalidDynamicConfigFile loads a dynamic config file with invalid failpoint
// settings, with the YAML file loader. With the switch off it has no effect and causes no error.
func TestFailpointsOff_InvalidDynamicConfigFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "dynamicconfig.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
testing.failpointRules:
  - value:
      - key: on
        action: Of
        trigger: {1: 2}
`), 0o600))
	done := make(chan any)
	t.Cleanup(func() { close(done) })
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	client, err := dynamicconfig.NewFileBasedClient(&dynamicconfig.FileBasedClientConfig{Filepath: path, PollInterval: time.Hour}, logger, done)
	require.NoError(t, err)

	source := resolveFailpoints(false, lookupEnv(nil), logger, metrics.NoopMetricsHandler)
	collection := dynamicconfig.NewCollection(client, logger)
	collection.Start()
	t.Cleanup(collection.Stop)
	// The converter runs, without failing, whenever the setting is read.
	require.NotPanics(t, func() {
		_ = failpointrules.Setting.Get(collection)()
	})
	app := fxtest.New(t,
		fx.Supply(source, collection, fx.Annotate(logger, fx.As(new(log.Logger)))),
		failpointrules.Module,
		failpointrules.ReleaseModule,
	)
	app.RequireStart()
	app.RequireStop()
	require.Nil(t, source.Failpoints())
}

func TestWithFailpointSourceObserver_Composes(t *testing.T) {
	t.Parallel()

	so := newServerOptions([]ServerOption{
		WithFailpointSourceObserver(func(*failpoints.Source) {}),
		WithFailpointSourceObserver(func(*failpoints.Source) {}),
	})
	require.Len(t, so.failpointSourceObservers, 2)
}
