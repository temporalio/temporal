//go:build temporal_minimal && !temporal_with_prometheus

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

func TestMetricsHandlerFromConfigPrometheusExcluded(t *testing.T) {
	t.Parallel()
	for _, framework := range []string{FrameworkTally, FrameworkOpentelemetry} {
		t.Run(framework, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{Prometheus: &PrometheusConfig{Framework: framework, ListenAddress: "127.0.0.1:0"}}
			handler, err := MetricsHandlerFromConfig(log.NewNoopLogger(), cfg)
			if handler != nil {
				t.Cleanup(func() { handler.Stop(log.NewNoopLogger()) })
			}
			require.ErrorContains(t, err, "temporal_with_prometheus")
			require.Nil(t, handler)
		})
	}
}

func TestNewScopePrometheusExcluded(t *testing.T) {
	t.Parallel()
	logger := log.NewMockLogger(gomock.NewController(t))
	logger.EXPECT().Fatal("unable to create metrics scope", gomock.Any())
	require.Nil(t, NewScope(logger, &Config{Prometheus: &PrometheusConfig{}}))
}

func TestPrometheusConstructorsExcluded(t *testing.T) {
	t.Parallel()
	logger := log.NewNoopLogger()
	provider, err := NewOpenTelemetryProviderWithPrometheus(logger, &PrometheusConfig{}, &ClientConfig{}, false)
	require.ErrorContains(t, err, "temporal_with_prometheus")
	require.Nil(t, provider)
}
