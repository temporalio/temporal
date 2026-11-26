package temporal

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite" // registers the sqlite plugin
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/server/tests/testutils"
	"go.temporal.io/testx/failpoint"
	"gopkg.in/yaml.v3"
)

// developmentConfig loads the development-sqlite config with a dynamic config file that sets one
// unrelated value.
func developmentConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg, err := config.Load(
		config.WithEnv("development-sqlite"),
		config.WithConfigDir(filepath.Join(testutils.GetRepoRootDirectory(), "config")),
	)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "dynamicconfig.yaml")
	require.NoError(t, os.WriteFile(path, []byte("system.visibilityPersistenceMaxReadQPS: [{value: 123}]\n"), 0o600))
	cfg.DynamicConfigClient = &dynamicconfig.FileBasedClientConfig{Filepath: path, PollInterval: time.Hour}
	return cfg
}

// The server reads BUGGIFY from the failpoints.buggify block of its static config and from nothing
// else: a BUGGIFY seed or trace in the environment of the process does nothing.
func TestServerOptionsProvider_ReadsBuggifyFromTheStaticConfigOnly(t *testing.T) {
	t.Setenv("FAILPOINT_BUGGIFY_SEED", "5")
	t.Setenv("FAILPOINT_BUGGIFY_TRACE", "1")

	provide := func(t *testing.T, cfg *config.Config) serverOptionsProvider {
		t.Helper()
		provided, err := ServerOptionsProvider([]ServerOption{
			WithConfig(cfg),
			WithLogger(log.NewNoopLogger()),
			WithCustomMetricsHandler(metrics.NoopMetricsHandler),
			WithFailpointSource(failpoint.NewRegistry()),
		})
		require.NoError(t, err)
		close(provided.StopChan)
		return provided
	}

	t.Run("an empty block", func(t *testing.T) {
		provided := provide(t, developmentConfig(t))
		require.Equal(t, serverbuggify.StaticConfig{}, provided.BuggifyStatic)
	})

	t.Run("a block with a seed", func(t *testing.T) {
		cfg := developmentConfig(t)
		one := 1.0
		cfg.Failpoints.Buggify = config.FailpointsBuggify{
			Seed:  8,
			Keys:  []config.FailpointsBuggifyKey{{Match: "*", ActivationProbability: &one, FireProbability: &one}},
			Trace: true,
		}
		provided := provide(t, cfg)
		require.Equal(t, uint64(8), provided.BuggifyStatic.Config.Seed)
		require.Len(t, provided.BuggifyStatic.Config.Keys, 1)
		require.True(t, provided.BuggifyStatic.Trace)
	})
}

// pollerCount returns the number of goroutines that poll a dynamic config file.
func pollerCount(t *testing.T) int {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, pprof.Lookup("goroutine").WriteTo(&buf, 1))
	return strings.Count(buf.String(), "dynamicconfig.(*FileBasedClient).init.func1")
}

// A server that fails to start because of its BUGGIFY configuration doesn't leave the poller of its
// dynamic config file running, which would leak one poller for each attempt of a process that retries.
func TestServerOptionsProvider_StopsThePollerWhenBuggifyIsRejected(t *testing.T) {
	cfg := developmentConfig(t)
	cfg.Failpoints.Buggify = config.FailpointsBuggify{Seed: 1}

	before := pollerCount(t)
	_, err := ServerOptionsProvider([]ServerOption{
		WithConfig(cfg),
		WithLogger(log.NewNoopLogger()),
		WithCustomMetricsHandler(metrics.NoopMetricsHandler),
		WithFailpointSource(failpoint.NewRegistry()),
	})
	require.ErrorContains(t, err, "a seed needs keys")
	await.RequireTruef(t, func() bool { return pollerCount(t) <= before }, 10*time.Second, 10*time.Millisecond,
		"the poller of the dynamic config file still runs")
}

// The failpoints.buggify block of the YAML config is decoded only with failpoints on: a server
// without failpoints starts with a malformed block, and one with them rejects it.
func TestServerOptionsProvider_ReadsTheBuggifyBlockOnlyWithFailpoints(t *testing.T) {
	for _, block := range []string{"buggify: false", "buggify:\n    seed: 1.5"} {
		cfg := developmentConfig(t)
		require.NoError(t, yaml.Unmarshal([]byte("failpoints:\n  "+block+"\n"), cfg))

		provided, err := ServerOptionsProvider([]ServerOption{
			WithConfig(cfg),
			WithLogger(log.NewNoopLogger()),
			WithCustomMetricsHandler(metrics.NoopMetricsHandler),
		})
		require.NoError(t, err, block)
		close(provided.StopChan)
		require.Nil(t, provided.FailpointSource)
		require.Equal(t, serverbuggify.StaticConfig{}, provided.BuggifyStatic)

		cfg.Failpoints.Enabled = true
		_, err = ServerOptionsProvider([]ServerOption{
			WithConfig(cfg),
			WithLogger(log.NewNoopLogger()),
			WithCustomMetricsHandler(metrics.NoopMetricsHandler),
		})
		require.ErrorContains(t, err, "invalid failpoints.buggify", block)
	}
}
