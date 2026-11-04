//go:build temporal_minimal && !temporal_with_prometheus

package metrics

import (
	"errors"

	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var errPrometheusExcluded = errors.New("prometheus: Prometheus metrics reporter is not compiled into this server; remove this config or rebuild with the temporal_with_prometheus build tag")

func validatePrometheusConfig(c *PrometheusConfig) error {
	if c != nil {
		return errPrometheusExcluded
	}
	return nil
}

func newPrometheusScope(logger log.Logger, _ *Config) tally.Scope {
	logger.Fatal("unable to create metrics scope", tag.Error(errPrometheusExcluded))
	return nil
}

func NewOpenTelemetryProviderWithPrometheus(log.Logger, *PrometheusConfig, *ClientConfig, bool) (*openTelemetryProviderImpl, error) {
	return nil, errPrometheusExcluded
}
