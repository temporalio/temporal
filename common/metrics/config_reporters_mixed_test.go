//go:build temporal_minimal && ((temporal_with_prometheus && !temporal_with_statsd) || (temporal_with_statsd && !temporal_with_prometheus))

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestMetricsHandlerFromConfigRejectsMixedReporters(t *testing.T) {
	t.Parallel()
	for _, statsdFramework := range []string{FrameworkTally, FrameworkOpentelemetry} {
		for _, prometheusFramework := range []string{FrameworkTally, FrameworkOpentelemetry} {
			t.Run(statsdFramework+"/"+prometheusFramework, func(t *testing.T) {
				t.Parallel()
				cfg := &Config{
					Statsd:     &StatsdConfig{Framework: statsdFramework, HostPort: "127.0.0.1:8125"},
					Prometheus: &PrometheusConfig{Framework: prometheusFramework, ListenAddress: "127.0.0.1:0"},
				}
				handler, err := MetricsHandlerFromConfig(log.NewNoopLogger(), cfg)
				if handler != nil {
					t.Cleanup(func() { handler.Stop(log.NewNoopLogger()) })
				}
				require.ErrorContains(t, err, "build tag")
				require.Nil(t, handler)
			})
		}
	}
}
