//go:build !temporal_minimal || temporal_with_prometheus || temporal_with_statsd

package metrics

import (
	"maps"
	"time"

	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var (
	defaultPerUnitHistogramBoundaries = map[string][]float64{
		Dimensionless: {
			1,
			2,
			5,
			10,
			20,
			50,
			100,
			200,
			500,
			1_000,
			2_000,
			5_000,
			10_000,
			20_000,
			50_000,
			100_000,
		},
		Milliseconds: {
			1,
			2,
			5,
			10,
			20,
			50,
			100,
			200,
			500,
			1_000, // 1s
			2_000,
			5_000,
			10_000, // 10s
			20_000,
			50_000,
			100_000, // 100s = 1m40s
			200_000,
			500_000,
			1_000_000, // 1000s = 16m40s
		},
		Bytes: {
			1024,
			2048,
			4096,
			8192,
			16384,
			32768,
			65536,
			131072,
			262144,
			524288,
			1048576,
			2097152,
			4194304,
			8388608,
			16777216,
		},
	}
)

// NewScope builds a new tally scope for this metrics configuration
//
// If the underlying configuration is valid for multiple reporter types,
// only one of them will be used for reporting.
//
// Current priority order is:
// statsd > prometheus
func NewScope(logger log.Logger, c *Config) tally.Scope {
	if err := c.Validate(); err != nil {
		logger.Fatal("unable to create metrics scope", tag.Error(err))
		return nil
	}
	if c.Statsd != nil {
		return newStatsdScope(logger, c)
	}
	if c.Prometheus != nil {
		return newPrometheusScope(logger, c)
	}
	return tally.NoopScope
}

func setDefaultPerUnitHistogramBoundaries(clientConfig *ClientConfig) {
	buckets := maps.Clone(defaultPerUnitHistogramBoundaries)

	// In config, when overwrite default buckets, we use [dimensionless / miliseconds / bytes] as keys.
	// But in code, we use [1 / ms / By] as key (to align with otel unit definition). So we do conversion here.
	if bucket, ok := clientConfig.PerUnitHistogramBoundaries[UnitNameDimensionless]; ok {
		buckets[Dimensionless] = bucket
	}
	if bucket, ok := clientConfig.PerUnitHistogramBoundaries[UnitNameMilliseconds]; ok {
		buckets[Milliseconds] = bucket
	}
	if bucket, ok := clientConfig.PerUnitHistogramBoundaries[UnitNameBytes]; ok {
		buckets[Bytes] = bucket
	}

	bucketInSeconds := make([]float64, len(buckets[Milliseconds]))
	for idx, boundary := range buckets[Milliseconds] {
		bucketInSeconds[idx] = boundary / float64(time.Second/time.Millisecond)
	}
	buckets[Seconds] = bucketInSeconds

	clientConfig.PerUnitHistogramBoundaries = buckets
}

// MetricsHandlerFromConfig is used at startup to construct a MetricsHandler
func MetricsHandlerFromConfig(logger log.Logger, c *Config) (Handler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c == nil {
		return NoopMetricsHandler, nil
	}

	setDefaultPerUnitHistogramBoundaries(&c.ClientConfig)

	fatalOnListenerError := true
	if c.Statsd != nil && c.Statsd.Framework == FrameworkOpentelemetry {
		// create opentelemetry provider with just statsd
		otelProvider, err := NewOpenTelemetryProviderWithStatsd(logger, c.Statsd, &c.ClientConfig)
		if err != nil {
			logger.Fatal(err.Error())
		}
		return NewOtelMetricsHandler(logger, otelProvider, c.ClientConfig, false)
	}

	if c.Prometheus != nil && c.Prometheus.Framework == FrameworkOpentelemetry {
		// create opentelemetry provider with just prometheus
		otelProvider, err := NewOpenTelemetryProviderWithPrometheus(logger, c.Prometheus, &c.ClientConfig, fatalOnListenerError)
		if err != nil {
			logger.Fatal(err.Error())
		}
		return NewOtelMetricsHandler(logger, otelProvider, c.ClientConfig, c.RecordTimerInSeconds)
	}

	// fallback to tally if no framework is specified
	return NewTallyMetricsHandler(
		c.ClientConfig,
		NewScope(logger, c),
	), nil
}
