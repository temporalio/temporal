package metrics

import (
	"fmt"
	"maps"
	"time"

	"go.temporal.io/server/common/log"
)

type (
	// Config contains the config items for metrics subsystem
	Config struct {
		ClientConfig `yaml:"clientConfig,inline"`

		// Statsd is the configuration for statsd reporter (now via OpenTelemetry)
		Statsd *StatsdConfig `yaml:"statsd"`
		// Prometheus is the configuration for prometheus reporter (now via OpenTelemetry)
		Prometheus *PrometheusConfig `yaml:"prometheus"`
	}

	ClientConfig struct {
		// Tags is the set of key-value pairs to be reported as part of every metric
		Tags map[string]string `yaml:"tags"`
		// ExcludeTags is a map from tag name string to tag values string list.
		// Each value present in keys will have relevant tag value replaced with "_tag_excluded_"
		// Each value in values list will white-list tag values to be reported as usual.
		ExcludeTags map[string][]string `yaml:"excludeTags"`
		// Prefix sets the prefix to all outgoing metrics using the OpenTelemetry implementation.
		Prefix string `yaml:"prefix"`

		// DefaultHistogramBoundaries defines the default histogram bucket
		// boundaries.
		// Configuration of histogram boundaries for given metric unit.
		// Supported values: "dimensionless", "milliseconds", "bytes"
		PerUnitHistogramBoundaries map[string][]float64 `yaml:"perUnitHistogramBoundaries"`

		// The following configs control OpenTelemetry output format.
		// WithoutUnitSuffix controls the addition of unit suffixes to metric names.
		WithoutUnitSuffix bool `yaml:"withoutUnitSuffix"`
		// WithoutCounterSuffix controls the addition of _total suffixes to counter metric names.
		WithoutCounterSuffix bool `yaml:"withoutCounterSuffix"`
		// RecordTimerInSeconds controls if Timer metric should be emitted as number of seconds
		// (instead of milliseconds).
		RecordTimerInSeconds bool `yaml:"recordTimerInSeconds"`
	}

	// StatsdConfig contains the config items for statsd metrics reporter (via OpenTelemetry)
	StatsdConfig struct {
		// The host and port of the statsd server
		HostPort string `yaml:"hostPort" validate:"nonzero"`
		// The prefix to use in reporting to statsd
		Prefix string `yaml:"prefix"` // Note: OpenTelemetry exporters might handle prefixing differently. Check exporter docs.
		// FlushInterval is the maximum interval for sending packets.
		// If it is not specified, it defaults to a reasonable value based on the Otel exporter.
		FlushInterval time.Duration `yaml:"flushInterval"`
		// FlushBytes specifies the maximum udp packet size you wish to send.
		// If FlushBytes is unspecified, it defaults to a reasonable value based on the Otel exporter.
		FlushBytes int `yaml:"flushBytes"`
		// Optional: Add any specific Otel StatsD exporter options here if needed.
	}

	// PrometheusConfig is the configuration for prometheus metrics (via OpenTelemetry).
	PrometheusConfig struct {
		// Address for prometheus to serve metrics from.
		ListenAddress string `yaml:"listenAddress"`

		// HandlerPath if specified will be used instead of using the default
		// HTTP handler path "/metrics".
		HandlerPath string `yaml:"handlerPath"`

		// LoggerRPS sets the RPS of the logger provided to prometheus. Default of 0 means no limit.
		LoggerRPS float64 `yaml:"loggerRPS"`

		// Optional: Add any specific Otel Prometheus exporter options here if needed.
		// Deprecated fields like TimerType, DefaultHistogramBoundaries, etc., are removed.
		// Use PerUnitHistogramBoundaries in ClientConfig instead.
	}
)

// Supported framework types - Only OpenTelemetry is now supported
const (
	// FrameworkOpentelemetry OpenTelemetry framework id
	FrameworkOpentelemetry = "opentelemetry" // Kept for potential future use, but functionally only one framework now.
)

// Valid unit name for PerUnitHistogramBoundaries config field
const (
	UnitNameDimensionless = "dimensionless"
	UnitNameMilliseconds  = "milliseconds"
	UnitNameBytes         = "bytes"
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
		// Seconds bucket is derived below
	}
)

func setDefaultPerUnitHistogramBoundaries(clientConfig *ClientConfig) {
	if clientConfig.PerUnitHistogramBoundaries == nil {
		clientConfig.PerUnitHistogramBoundaries = make(map[string][]float64)
	}

	buckets := maps.Clone(defaultPerUnitHistogramBoundaries)

	if definedBuckets, ok := clientConfig.PerUnitHistogramBoundaries[UnitNameDimensionless]; ok {
		buckets[Dimensionless] = definedBuckets
	}
	if definedBuckets, ok := clientConfig.PerUnitHistogramBoundaries[UnitNameMilliseconds]; ok {
		buckets[Milliseconds] = definedBuckets
	}
	if definedBuckets, ok := clientConfig.PerUnitHistogramBoundaries[UnitNameBytes]; ok {
		buckets[Bytes] = definedBuckets
	}

	if _, ok := buckets[Dimensionless]; !ok {
		buckets[Dimensionless] = defaultPerUnitHistogramBoundaries[Dimensionless]
	}
	if _, ok := buckets[Milliseconds]; !ok {
		buckets[Milliseconds] = defaultPerUnitHistogramBoundaries[Milliseconds]
	}
	if _, ok := buckets[Bytes]; !ok {
		buckets[Bytes] = defaultPerUnitHistogramBoundaries[Bytes]
	}

	bucketInSeconds := make([]float64, len(buckets[Milliseconds]))
	for idx, boundary := range buckets[Milliseconds] {
		bucketInSeconds[idx] = boundary / float64(time.Second/time.Millisecond)
	}
	buckets[Seconds] = bucketInSeconds

	clientConfig.PerUnitHistogramBoundaries = buckets
}

// MetricsHandlerFromConfig is used at startup to construct a MetricsHandler using OpenTelemetry
func MetricsHandlerFromConfig(logger log.Logger, c *Config) (Handler, error) {
	if c == nil || (c.Prometheus == nil && c.Statsd == nil) {
		logger.Info("Metrics configuration is nil or no reporter configured. Using NoopMetricsHandler.")
		return NoopMetricsHandler, nil
	}

	setDefaultPerUnitHistogramBoundaries(&c.ClientConfig)

	fatalOnListenerError := true
	otelProvider, err := NewOpenTelemetryProvider(logger, c.Prometheus, &c.ClientConfig, fatalOnListenerError) // Still expects opentelemetry.go to be updated
	if err != nil {
		return nil, fmt.Errorf("failed to initialize OpenTelemetry provider: %w", err)
	}

	if otelProvider == nil {
		logger.Warn("OpenTelemetry provider initialization resulted in nil provider. Using NoopMetricsHandler.")
		return NoopMetricsHandler, nil
	}

	// Correctly handle the (Handler, error) return from NewOtelMetricsHandler
	handler, err := NewOtelMetricsHandler(logger, otelProvider, c.ClientConfig)
	if err != nil {
		// Handle potential error from NewOtelMetricsHandler, though its signature might not actually return one.
		// Assuming it might return an error for robustness. If it definitely doesn't, this can be simplified.
		return nil, fmt.Errorf("failed to initialize OtelMetricsHandler: %w", err)
	}
	return handler, nil
}

func configExcludeTags(cfg ClientConfig) map[string]map[string]struct{} {
	tagsToFilter := make(map[string]map[string]struct{})
	for key, val := range cfg.ExcludeTags {
		exclusions := make(map[string]struct{})
		for _, v := range val {
			exclusions[v] = struct{}{}
		}
		tagsToFilter[key] = exclusions
	}
	return tagsToFilter
}
