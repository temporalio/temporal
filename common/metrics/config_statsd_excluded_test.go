//go:build temporal_minimal && !temporal_with_statsd

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

func TestMetricsHandlerFromConfigStatsdExcluded(t *testing.T) {
	t.Parallel()
	for _, framework := range []string{FrameworkTally, FrameworkOpentelemetry} {
		t.Run(framework, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{Statsd: &StatsdConfig{Framework: framework, HostPort: "127.0.0.1:8125"}}
			handler, err := MetricsHandlerFromConfig(log.NewNoopLogger(), cfg)
			if handler != nil {
				t.Cleanup(func() { handler.Stop(log.NewNoopLogger()) })
			}
			require.ErrorContains(t, err, "temporal_with_statsd")
			require.Nil(t, handler)
		})
	}
}

func TestNewScopeStatsdExcluded(t *testing.T) {
	t.Parallel()
	logger := log.NewMockLogger(gomock.NewController(t))
	logger.EXPECT().Fatal("unable to create metrics scope", gomock.Any())
	require.Nil(t, NewScope(logger, &Config{Statsd: &StatsdConfig{}}))
}

func TestStatsdConstructorsExcluded(t *testing.T) {
	t.Parallel()
	logger := log.NewNoopLogger()
	provider, err := NewOpenTelemetryProviderWithStatsd(logger, &StatsdConfig{}, &ClientConfig{})
	require.ErrorContains(t, err, "temporal_with_statsd")
	require.Nil(t, provider)
	exporter, err := NewStatsdExporter(&StatsdConfig{}, logger)
	require.ErrorContains(t, err, "temporal_with_statsd")
	require.Nil(t, exporter)
}
