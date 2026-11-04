//go:build temporal_minimal && !temporal_with_prometheus && !temporal_with_statsd

package metrics

import (
	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

func MetricsHandlerFromConfig(_ log.Logger, c *Config) (Handler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return NoopMetricsHandler, nil
}

func NewScope(logger log.Logger, c *Config) tally.Scope {
	if err := c.Validate(); err != nil {
		logger.Fatal("unable to create metrics scope", tag.Error(err))
		return nil
	}
	return tally.NoopScope
}

type openTelemetryProviderImpl struct {
	OpenTelemetryProvider
}
