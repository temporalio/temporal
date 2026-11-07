//go:build temporal_minimal && !temporal_with_statsd

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
)

func TestConfigValidateStatsdExcluded(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Global: Global{Metrics: &metrics.Config{Statsd: &metrics.StatsdConfig{}}},
		Persistence: Persistence{
			DefaultStore: "sql", VisibilityStore: "sql",
			DataStores: map[string]DataStore{"sql": {SQL: &SQL{}}},
		},
	}
	require.ErrorContains(t, cfg.Validate(), "temporal_with_statsd")
}
