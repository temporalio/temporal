package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestStatsdExporter(t *testing.T) {
	// Test configuration
	config := &StatsdConfig{
		HostPort: "localhost:8125",
		Prefix:   "test",
		Reporter: StatsdReporterConfig{
			TagSeparator: ",",
		},
	}

	logger := log.NewNoopLogger()

	// Create StatsD exporter
	exporter, err := NewStatsdExporter(config, logger)
	require.NoError(t, err)
	require.NotNil(t, exporter)

	// Test that the exporter implements the correct interface
	assert.NotNil(t, exporter.Temporality)
	assert.NotNil(t, exporter.Aggregation)
	assert.NotNil(t, exporter.Export)
	assert.NotNil(t, exporter.ForceFlush)
	assert.NotNil(t, exporter.Shutdown)

	// Test shutdown
	err = exporter.Shutdown(context.Background())
	assert.NoError(t, err)

	// Test that operations after shutdown return errors
	err = exporter.Export(context.Background(), nil)
	assert.Error(t, err)
}

func TestNewOpenTelemetryProviderWithStatsD(t *testing.T) {
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

	statsdConfig := &StatsdConfig{
		HostPort: "localhost:8125",
		Prefix:   "temporal_test",
		Reporter: StatsdReporterConfig{
			TagSeparator: ",",
		},
	}

	// Create OpenTelemetry provider with StatsD
	provider, err := NewOpenTelemetryProviderWithStatsd(logger, statsdConfig, clientConfig)
	require.NoError(t, err)
	require.NotNil(t, provider)

	// Test that we can get a meter
	meter := provider.GetMeter()
	assert.NotNil(t, meter)

	// Create a simple counter to test
	counter, err := meter.Int64Counter("test_counter")
	require.NoError(t, err)

	// Add some measurements
	counter.Add(context.Background(), 1)
	counter.Add(context.Background(), 5)

	// Test shutdown
	require.Eventually(t, func() bool {
		provider.Stop(logger)
		return true
	}, time.Second, 100*time.Millisecond)
}

func TestNewOpenTelemetryProviderWithPrometheus(t *testing.T) {
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
	assert.NotNil(t, meter)

	// Create a simple counter to test
	counter, err := meter.Int64Counter("test_counter")
	require.NoError(t, err)

	// Add some measurements
	counter.Add(context.Background(), 1)
	counter.Add(context.Background(), 5)

	// Test shutdown
	require.Eventually(t, func() bool {
		provider.Stop(logger)
		return true
	}, time.Second, 100*time.Millisecond)
}
