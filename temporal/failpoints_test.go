package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

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

// TestFailpointsDisabledIgnoresBuggify checks that when failpoints are disabled, serverbuggify
// neither randomizes dynamic config settings nor installs BUGGIFY, even with a seed configured.
func TestFailpointsDisabledIgnoresBuggify(t *testing.T) {
	t.Parallel()
	registry := resolveFailpoints(false, log.NewNoopLogger())
	client := dynamicconfig.NewMemoryClient()
	wrapped, buggifyStatic, err := serverbuggify.RandomizeSettings(
		client,
		log.NewNoopLogger(),
		registry != nil,
		config.FailpointsBuggify{Seed: 5, Trace: true},
	)
	require.NoError(t, err)
	require.Same(t, client, wrapped)
	require.Equal(t, serverbuggify.StaticConfig{}, buggifyStatic)

	app := fxtest.New(t,
		fx.Supply(
			registry,
			fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger))),
		),
		serverbuggify.Module,
	)
	app.RequireStart()
	app.RequireStop()
}
