//go:build temporal_minimal && !temporal_with_metric_reporters

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

func TestMetricsHandlerFromConfigReportersExcluded(t *testing.T) {
	t.Parallel()
	for _, framework := range []string{FrameworkTally, FrameworkOpentelemetry} {
		for name, cfg := range map[string]*Config{
			"statsd":     {Statsd: &StatsdConfig{Framework: framework, HostPort: "127.0.0.1:8125"}},
			"prometheus": {Prometheus: &PrometheusConfig{Framework: framework, ListenAddress: "127.0.0.1:0"}},
		} {
			t.Run(framework+"/"+name, func(t *testing.T) {
				t.Parallel()
				handler, err := MetricsHandlerFromConfig(log.NewNoopLogger(), cfg)
				if handler != nil {
					t.Cleanup(func() { handler.Stop(log.NewNoopLogger()) })
				}
				require.ErrorContains(t, err, "temporal_with_metric_reporters")
				require.Nil(t, handler)
			})
		}
	}
}

func TestMetricsHandlerWithoutReporters(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]*Config{
		"nil":            nil,
		"empty":          {},
		"client options": {ClientConfig: ClientConfig{Prefix: "temporal", Tags: map[string]string{"cluster": "test"}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			handler, err := MetricsHandlerFromConfig(log.NewNoopLogger(), cfg)
			require.NoError(t, err)
			require.Equal(t, NoopMetricsHandler, handler)
			require.Equal(t, tally.NoopScope, NewScope(log.NewNoopLogger(), cfg))
		})
	}
}

func TestReporterConstructorsExcluded(t *testing.T) {
	t.Parallel()
	logger := log.NewNoopLogger()
	statsdProvider, err := NewOpenTelemetryProviderWithStatsd(logger, &StatsdConfig{}, &ClientConfig{})
	require.ErrorContains(t, err, "temporal_with_metric_reporters")
	require.Nil(t, statsdProvider)
	prometheusProvider, err := NewOpenTelemetryProviderWithPrometheus(logger, &PrometheusConfig{}, &ClientConfig{}, false)
	require.ErrorContains(t, err, "temporal_with_metric_reporters")
	require.Nil(t, prometheusProvider)
	exporter, err := NewStatsdExporter(&StatsdConfig{}, logger)
	require.ErrorContains(t, err, "temporal_with_metric_reporters")
	require.Nil(t, exporter)
}

func TestNewScopeReportersExcluded(t *testing.T) {
	t.Parallel()
	logger := log.NewMockLogger(gomock.NewController(t))
	logger.EXPECT().Fatal("unable to create metrics scope", gomock.Any())
	require.Nil(t, NewScope(logger, &Config{Statsd: &StatsdConfig{}}))
}
