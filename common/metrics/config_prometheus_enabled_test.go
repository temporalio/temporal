//go:build !temporal_minimal || temporal_with_prometheus

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestMetricsHandlerFromConfigPrometheusEnabled(t *testing.T) {
	t.Parallel()
	for _, framework := range []string{FrameworkTally, FrameworkOpentelemetry} {
		t.Run(framework, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{Prometheus: &PrometheusConfig{Framework: framework, ListenAddress: "127.0.0.1:0"}}
			handler, err := MetricsHandlerFromConfig(log.NewNoopLogger(), cfg)
			require.NoError(t, err)
			require.NotNil(t, handler)
			t.Cleanup(func() { handler.Stop(log.NewNoopLogger()) })
		})
	}
}
