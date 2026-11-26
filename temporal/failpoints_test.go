package temporal

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/testlogger"
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
