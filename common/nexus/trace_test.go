package nexus

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/testlogger"
)

func TestLoggedHTTPClientTraceUsesProtocolComponent(t *testing.T) {
	t.Parallel()

	provider := &LoggedHTTPClientTraceProvider{
		Config: func() HTTPClientTraceConfig {
			return HTTPClientTraceConfig{
				Enabled:           true,
				ForwardingEnabled: true,
				Hooks:             []string{"GetConn"},
			}
		},
	}

	for name, newTrace := range map[string]func(*testlogger.TestLogger) func(string){
		"operation": func(logger *testlogger.TestLogger) func(string) {
			return provider.NewTrace(1, logger).GetConn
		},
		"forwarding": func(logger *testlogger.TestLogger) func(string) {
			return provider.NewForwardingTrace(logger).GetConn
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			capture := logger.StartCapture()
			newTrace(logger)("localhost:7233")

			records := capture.Snapshot()
			require.Len(t, records, 1)
			require.Contains(t, records[0].Tags, tag.NewStringTag("component", "nexus-protocol"))
		})
	}
}
