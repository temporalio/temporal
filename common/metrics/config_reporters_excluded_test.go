//go:build temporal_minimal && !temporal_with_prometheus && !temporal_with_statsd

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
)

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
