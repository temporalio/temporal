//go:build !temporal_minimal || temporal_with_statsd

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestMetricsHandlerFromConfigStatsdEnabled(t *testing.T) {
	t.Parallel()
	for _, framework := range []string{FrameworkTally, FrameworkOpentelemetry} {
		t.Run(framework, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{Statsd: &StatsdConfig{Framework: framework, HostPort: "127.0.0.1:8125"}}
			handler, err := MetricsHandlerFromConfig(log.NewNoopLogger(), cfg)
			require.NoError(t, err)
			require.NotNil(t, handler)
			t.Cleanup(func() { handler.Stop(log.NewNoopLogger()) })
		})
	}
}
