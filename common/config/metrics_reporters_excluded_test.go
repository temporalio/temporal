//go:build temporal_minimal && !temporal_with_metric_reporters

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
)

func TestConfigValidateMetricsReportersExcluded(t *testing.T) {
	t.Parallel()
	for name, reporter := range map[string]*metrics.Config{
		"statsd":     {Statsd: &metrics.StatsdConfig{}},
		"prometheus": {Prometheus: &metrics.PrometheusConfig{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				Global: Global{Metrics: reporter},
				Persistence: Persistence{
					DefaultStore: "sql", VisibilityStore: "sql",
					DataStores: map[string]DataStore{"sql": {SQL: &SQL{}}},
				},
			}
			require.ErrorContains(t, cfg.Validate(), "temporal_with_metric_reporters")
		})
	}
}
