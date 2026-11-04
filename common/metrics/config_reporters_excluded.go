//go:build temporal_minimal && !temporal_with_metric_reporters

package metrics

import (
	"errors"

	"github.com/uber-go/tally/v4"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var errMetricReportersExcluded = errors.New("metrics: built-in metric reporters are not compiled into this server; remove this config or rebuild with the temporal_with_metric_reporters build tag")

func (c *Config) Validate() error {
	if c != nil && (c.Statsd != nil || c.Prometheus != nil) {
		return errMetricReportersExcluded
	}
	return nil
}

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

func NewOpenTelemetryProviderWithStatsd(log.Logger, *StatsdConfig, *ClientConfig) (*openTelemetryProviderImpl, error) {
	return nil, errMetricReportersExcluded
}

func NewOpenTelemetryProviderWithPrometheus(log.Logger, *PrometheusConfig, *ClientConfig, bool) (*openTelemetryProviderImpl, error) {
	return nil, errMetricReportersExcluded
}

type statsdExporter struct {
	sdkmetrics.Exporter
}

func NewStatsdExporter(*StatsdConfig, log.Logger) (*statsdExporter, error) {
	return nil, errMetricReportersExcluded
}
