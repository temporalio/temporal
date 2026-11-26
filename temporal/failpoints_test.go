package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

// TestFailpointRegistryStartupSwitch checks that only the failpoints.enabled config setting enables failpoints,
// that the server warns when it does, and that it says nothing otherwise.
func TestFailpointRegistryStartupSwitch(t *testing.T) {
	t.Parallel()

	const enabledMessage = "FAILPOINTS ARE ENABLED (failpoints.enabled in the static config): tests and tools can change the behavior of " +
		"this server. This is not meant for production use."
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

			registry := newFailpointRegistryIfEnabled(tc.enabled, logger)

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
// doesn't install BUGGIFY, and doesn't even read the setting, so an invalid value is ignored.
func TestFailpointsDisabledIgnoresBuggify(t *testing.T) {
	t.Parallel()
	registry := newFailpointRegistryIfEnabled(false, log.NewNoopLogger())
	require.Nil(t, registry)
	client := dynamicconfig.NewMemoryClient()
	client.OverrideSetting(serverbuggify.Setting, map[string]any{"seed": 5, "unknown": true})

	app := fxtest.New(t,
		fx.Supply(
			registry,
			fx.Annotate(client, fx.As(new(dynamicconfig.Client))),
			fx.Annotate(log.NewNoopLogger(), fx.As(new(log.Logger))),
		),
		serverbuggify.Module,
	)
	app.RequireStart()
	app.RequireStop()
}
