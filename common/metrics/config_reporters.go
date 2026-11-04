//go:build !temporal_minimal || temporal_with_metric_reporters

package metrics

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/v5/statsd"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/prometheus"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	statsdreporter "go.temporal.io/server/common/metrics/tally/statsd"
	"go.temporal.io/server/common/softassert"
)

func (*Config) Validate() error {
	return nil
}

// tally sanitizer options that satisfy both Prometheus and M3 restrictions.
// This will rename metrics at the tally emission level, so metrics name we
// use maybe different from what gets emitted. In the current implementation
// it will replace - and . with _
// We should still ensure that the base metrics are prometheus compatible,
// but this is necessary as the same prom client initialization is used by
// our system workflows.
var (
	safeCharacters = []rune{'_'}

	defaultTallySanitizeOptions = tally.SanitizeOptions{
		NameCharacters: tally.ValidCharacters{
			Ranges:     tally.AlphanumericRange,
			Characters: safeCharacters,
		},
		KeyCharacters: tally.ValidCharacters{
			Ranges:     tally.AlphanumericRange,
			Characters: safeCharacters,
		},
		ValueCharacters: tally.ValidCharacters{
			Ranges:     tally.AlphanumericRange,
			Characters: safeCharacters,
		},
		ReplacementCharacter: tally.DefaultReplacementCharacter,
	}

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
	if c.Statsd != nil {
		return newStatsdScope(logger, c)
	}
	if c.Prometheus != nil {
		sanitizeOptions, err := convertSanitizeOptionsToTally(c.Prometheus)
		if err != nil {
			logger.Fatal("invalid sanitize options input on prometheus config", tag.Error(err))
			return nil
		}

		if c.Prometheus.LoggerRPS > 0 {
			logger = log.NewThrottledLogger(logger, func() float64 { return c.Prometheus.LoggerRPS })
		}

		return newPrometheusScope(
			logger,
			convertPrometheusConfigToTally(&c.ClientConfig, c.Prometheus),
			sanitizeOptions,
			&c.ClientConfig,
		)
	}
	return tally.NoopScope
}

func convertSanitizeOptionsToTally(config *PrometheusConfig) (tally.SanitizeOptions, error) {
	if config.SanitizeOptions == nil {
		return defaultTallySanitizeOptions, nil
	}

	return config.SanitizeOptions.toTally()
}

func convertPrometheusConfigToTally(
	clientConfig *ClientConfig,
	config *PrometheusConfig,
) *prometheus.Configuration {
	defaultObjectives := make([]prometheus.SummaryObjective, len(config.DefaultSummaryObjectives))
	for i, item := range config.DefaultSummaryObjectives {
		defaultObjectives[i].AllowedError = item.AllowedError
		defaultObjectives[i].Percentile = item.Percentile
	}

	return &prometheus.Configuration{
		HandlerPath:              config.HandlerPath,
		ListenNetwork:            config.ListenNetwork,
		ListenAddress:            config.ListenAddress,
		TimerType:                "histogram",
		DefaultHistogramBuckets:  buildTallyTimerHistogramBuckets(clientConfig, config),
		DefaultSummaryObjectives: defaultObjectives,
		OnError:                  config.OnError,
	}
}

func buildTallyTimerHistogramBuckets(
	clientConfig *ClientConfig,
	config *PrometheusConfig,
) []prometheus.HistogramObjective {
	if len(config.DefaultHistogramBuckets) > 0 {
		result := make([]prometheus.HistogramObjective, len(config.DefaultHistogramBuckets))
		for i, item := range config.DefaultHistogramBuckets {
			result[i].Upper = item.Upper
		}
		return result
	}

	if len(config.DefaultHistogramBoundaries) > 0 {
		result := make([]prometheus.HistogramObjective, 0, len(config.DefaultHistogramBoundaries))
		for _, value := range config.DefaultHistogramBoundaries {
			result = append(result, prometheus.HistogramObjective{
				Upper: value,
			})
		}
		return result
	}

	boundaries := clientConfig.PerUnitHistogramBoundaries[Milliseconds]
	result := make([]prometheus.HistogramObjective, 0, len(boundaries))
	for _, boundary := range boundaries {
		result = append(result, prometheus.HistogramObjective{
			Upper: boundary / float64(time.Second/time.Millisecond), // convert milliseconds to seconds
		})
	}
	return result
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

// newStatsdScope returns a new statsd scope with
// a default reporting interval of a second
func newStatsdScope(logger log.Logger, c *Config) tally.Scope {
	config := c.Statsd
	if len(config.HostPort) == 0 {
		return tally.NoopScope
	}
	statter, err := statsd.NewClientWithConfig(&statsd.ClientConfig{
		Address:       config.HostPort,
		Prefix:        config.Prefix,
		FlushInterval: config.FlushInterval,
		FlushBytes:    config.FlushBytes,
	})
	if err != nil {
		logger.Fatal("error creating statsd client", tag.Error(err))
	}
	// NOTE: according to (https://github.com/uber-go/tally) Tally's statsd implementation doesn't support tagging.
	// Therefore, we implement Tally interface to have a statsd reporter that can support tagging
	opts := statsdreporter.Options{
		TagSeparator: c.Statsd.Reporter.TagSeparator,
	}
	reporter := statsdreporter.NewReporter(statter, opts)
	scopeOpts := tally.ScopeOptions{
		Tags:     c.Tags,
		Reporter: reporter,
		Prefix:   c.Prefix,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	return scope
}

// newPrometheusScope returns a new prometheus scope with
// a default reporting interval of a second
func newPrometheusScope(
	logger log.Logger,
	config *prometheus.Configuration,
	sanitizeOptions tally.SanitizeOptions,
	clientConfig *ClientConfig,
) tally.Scope {
	var definitionConflicts tally.Counter
	reporter, err := config.NewReporter(
		prometheus.ConfigurationOptions{
			Registry: prom.NewRegistry(),
			OnError: func(err error) {
				// Tally drops recordings if the same metric name is used with different label names or help text.
				// Report a soft assertion so tests using this reporter fail, while production keeps running.
				if strings.Contains(err.Error(), "has different label names or a different help string") {
					definitionConflicts.Inc(1)
					softassert.Fail(logger, "error in prometheus reporter", tag.Error(err))
					return
				}
				logger.Warn("error in prometheus reporter", tag.Error(err))
			},
		},
	)
	if err != nil {
		logger.Fatal("error creating prometheus reporter", tag.Error(err))
	}
	scopeOpts := tally.ScopeOptions{
		Tags:            clientConfig.Tags,
		CachedReporter:  reporter,
		Separator:       prometheus.DefaultSeparator,
		SanitizeOptions: &sanitizeOptions,
		Prefix:          clientConfig.Prefix,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	// Cache the counter because OnError runs while a scope's metric lock is held.
	definitionConflicts = scope.Counter(PrometheusMetricDefinitionConflicts.Name())
	return scope
}

// MetricsHandlerFromConfig is used at startup to construct a MetricsHandler
func MetricsHandlerFromConfig(logger log.Logger, c *Config) (Handler, error) {
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

func (s SanitizeRange) toTally() (tally.SanitizeRange, error) {
	startRangeRunes := []rune(s.StartRange)
	if len(startRangeRunes) != 1 {
		return tally.SanitizeRange{}, fmt.Errorf("start range '%+v' must be a single rune", startRangeRunes)
	}

	endRangeRunes := []rune(s.EndRange)
	if len(endRangeRunes) != 1 {
		return tally.SanitizeRange{}, fmt.Errorf("end range '%+v' must be a single rune", endRangeRunes)
	}

	return tally.SanitizeRange([2]rune{startRangeRunes[0], endRangeRunes[0]}), nil
}

func (v ValidCharacters) toTally() (tally.ValidCharacters, error) {
	var ranges []tally.SanitizeRange

	for _, r := range v.Ranges {
		tallyRange, err := r.toTally()
		if err != nil {
			return tally.ValidCharacters{}, err
		}

		ranges = append(ranges, tallyRange)
	}

	return tally.ValidCharacters{
		Ranges:     ranges,
		Characters: []rune(v.SafeCharacters),
	}, nil
}

func (s SanitizeOptions) toTally() (tally.SanitizeOptions, error) {
	tallyNameChars, err := s.NameCharacters.toTally()
	if err != nil {
		return tally.SanitizeOptions{}, fmt.Errorf("invalid nameChars: %v", err)
	}

	tallyKeyChars, err := s.KeyCharacters.toTally()
	if err != nil {
		return tally.SanitizeOptions{}, fmt.Errorf("invalid keyChars: %v", err)
	}

	tallyValueChars, err := s.ValueCharacters.toTally()
	if err != nil {
		return tally.SanitizeOptions{}, fmt.Errorf("invalid valueChars: %v", err)
	}

	replacementChars := []rune(s.ReplacementCharacter)
	if len(replacementChars) != 1 {
		return tally.SanitizeOptions{}, errors.New("can only specify a single replacement character")
	}

	return tally.SanitizeOptions{
		NameCharacters:       tallyNameChars,
		KeyCharacters:        tallyKeyChars,
		ValueCharacters:      tallyValueChars,
		ReplacementCharacter: replacementChars[0],
	}, nil
}
