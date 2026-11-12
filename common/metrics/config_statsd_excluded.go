//go:build temporal_minimal && !temporal_with_statsd

package metrics

import (
	"errors"

	"github.com/uber-go/tally/v4"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var errStatsdExcluded = errors.New("statsd: StatsD metrics reporter is not compiled into this server; remove this config or rebuild with the temporal_with_statsd build tag")

func validateStatsdConfig(c *StatsdConfig) error {
	if c != nil {
		return errStatsdExcluded
	}
	return nil
}

func newStatsdScope(logger log.Logger, _ *Config) tally.Scope {
	logger.Fatal("unable to create metrics scope", tag.Error(errStatsdExcluded))
	return nil
}

func NewOpenTelemetryProviderWithStatsd(log.Logger, *StatsdConfig, *ClientConfig) (*openTelemetryProviderImpl, error) {
	return nil, errStatsdExcluded
}

type statsdExporter struct{ sdkmetrics.Exporter }

func NewStatsdExporter(*StatsdConfig, log.Logger) (*statsdExporter, error) {
	return nil, errStatsdExcluded
}
