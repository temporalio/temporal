//go:build !temporal_minimal || temporal_with_prometheus

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestNewOpenTelemetryProviderWithPrometheus(t *testing.T) {
	t.Parallel()
	logger := log.NewNoopLogger()

	// Test configuration
	clientConfig := &ClientConfig{
		PerUnitHistogramBoundaries: map[string][]float64{
			Dimensionless: {1, 5, 10, 25, 50, 100},
			Bytes:         {1024, 4096, 16384, 65536},
			Milliseconds:  {1, 5, 10, 25, 50, 100, 250, 500, 1000},
			Seconds:       {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
	}

	prometheusConfig := &PrometheusConfig{
		ListenAddress: "127.0.0.1:0", // Use port 0 to get a random available port
		HandlerPath:   "/metrics",
	}

	// Create OpenTelemetry provider with Prometheus
	provider, err := NewOpenTelemetryProviderWithPrometheus(logger, prometheusConfig, clientConfig, false)
	require.NoError(t, err)
	require.NotNil(t, provider)

	// Test that we can get a meter
	meter := provider.GetMeter()
	require.NotNil(t, meter)

	// Create a simple counter to test
	counter, err := meter.Int64Counter("test_counter")
	require.NoError(t, err)

	// Add some measurements
	counter.Add(t.Context(), 1)
	counter.Add(t.Context(), 5)

	// Test shutdown
	provider.Stop(logger)
}
