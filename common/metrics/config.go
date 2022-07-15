// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

import (
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/m3"
	"github.com/uber-go/tally/v4/prometheus"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	statsdreporter "go.temporal.io/server/common/metrics/tally/statsd"
)

type (
	// Config contains the config items for metrics subsystem
	Config struct {
		ClientConfig `yaml:"clientConfig,inline"`

		// M3 is the configuration for m3 metrics reporter
		M3 *m3.Configuration `yaml:"m3"`
		// Statsd is the configuration for statsd reporter
		Statsd *StatsdConfig `yaml:"statsd"`
		// Prometheus is the configuration for prometheus reporter
		Prometheus *PrometheusConfig `yaml:"prometheus"`
	}

	ClientConfig struct {
		// Tags is the set of key-value pairs to be reported as part of every metric
		Tags map[string]string `yaml:"tags"`
		// IgnoreTags is a map from tag name string to tag values string list.
		// Each value present in keys will have relevant tag value replaced with "_tag_excluded_"
		// Each value in values list will white-list tag values to be reported as usual.
		ExcludeTags map[string][]string `yaml:"excludeTags"`
		// Prefix sets the prefix to all outgoing metrics
		Prefix string `yaml:"prefix"`

		// DefaultHistogramBoundaries defines the default histogram bucket
		// boundaries.
		// Configuration of histogram boundaries for given metric unit.
		//
		// Supported values:
		// - "dimensionless"
		// - "milliseconds"
		// - "bytes"
		// - see defs.go:L62
		//
		// Tally implementation uses default buckets for timer/duration metrics.
		PerUnitHistogramBoundaries map[string][]float64 `yaml:"perUnitHistogramBoundaries"`
	}

	// StatsdConfig contains the config items for statsd metrics reporter
	StatsdConfig struct {
		// The host and port of the statsd server
		HostPort string `yaml:"hostPort" validate:"nonzero"`
		// The prefix to use in reporting to statsd
		Prefix string `yaml:"prefix" validate:"nonzero"`
		// FlushInterval is the maximum interval for sending packets.
		// If it is not specified, it defaults to 1 second.
		FlushInterval time.Duration `yaml:"flushInterval"`
		// FlushBytes specifies the maximum udp packet size you wish to send.
		// If FlushBytes is unspecified, it defaults  to 1432 bytes, which is
		// considered safe for local traffic.
		FlushBytes int `yaml:"flushBytes"`
		// Reporter allows additional configuration of the stats reporter, e.g. with custom tagging options.
		Reporter StatsdReporterConfig `yaml:"reporter"`
	}

	StatsdReporterConfig struct {
		// TagSeparator allows tags to be appended with a separator. If not specified tag keys and values
		// are embedded to the stat name directly.
		TagSeparator string `yaml:"tagSeparator"`
	}

	// PrometheusConfig is a new format for config for prometheus metrics.
	PrometheusConfig struct {
		// Metric framework: Tally/OpenTelemetry
		Framework string `yaml:"framework"`
		// Address for prometheus to serve metrics from.
		ListenAddress string `yaml:"listenAddress"`
		// DefaultHistogramBoundaries defines the default histogram bucket
		// boundaries.
		DefaultHistogramBoundaries []float64 `yaml:"defaultHistogramBoundaries"`

		// HandlerPath if specified will be used instead of using the default
		// HTTP handler path "/metrics".
		HandlerPath string `yaml:"handlerPath"`

		// Configs below are kept for backwards compatibility with previously exposed tally prometheus.Configuration.

		// Deprecated. ListenNetwork if specified will be used instead of using tcp network.
		// Supported networks: tcp, tcp4, tcp6 and unix.
		ListenNetwork string `yaml:"listenNetwork"`

		// Deprecated. TimerType is the default Prometheus type to use for Tally timers.
		TimerType string `yaml:"timerType"`

		// Deprecated. DefaultHistogramBuckets if specified will set the default histogram
		// buckets to be used by the reporter.
		DefaultHistogramBuckets []HistogramObjective `yaml:"defaultHistogramBuckets"`

		// Deprecated. DefaultSummaryObjectives if specified will set the default summary
		// objectives to be used by the reporter.
		DefaultSummaryObjectives []SummaryObjective `yaml:"defaultSummaryObjectives"`

		// Deprecated. OnError specifies what to do when an error either with listening
		// on the specified listen address or registering a metric with the
		// Prometheus. By default the registerer will panic.
		OnError string `yaml:"onError"`
	}
)

// Deprecated. HistogramObjective is a Prometheus histogram bucket.
// Added for backwards compatibility.
type HistogramObjective struct {
	Upper float64 `yaml:"upper"`
}

// Deprecated. SummaryObjective is a Prometheus summary objective.
// Added for backwards compatibility.
type SummaryObjective struct {
	Percentile   float64 `yaml:"percentile"`
	AllowedError float64 `yaml:"allowedError"`
}

const (
	ms = float64(time.Millisecond) / float64(time.Second)

	// Supported framework types

	// FrameworkTally tally framework id
	FrameworkTally = "tally"
	// FrameworkOpentelemetry OpenTelemetry framework id
	FrameworkOpentelemetry = "opentelemetry"
)

// tally sanitizer options that satisfy both Prometheus and M3 restrictions.
// This will rename metrics at the tally emission level, so metrics name we
// use maybe different from what gets emitted. In the current implementation
// it will replace - and . with _
// We should still ensure that the base metrics are prometheus compatible,
// but this is necessary as the same prom client initialization is used by
// our system workflows.
var (
	safeCharacters = []rune{'_'}

	sanitizeOptions = tally.SanitizeOptions{
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
			1000,
			2000,
			5000,
			10000,
			20000,
			50000,
			100000,
		},
		Milliseconds: {
			1 * ms,
			2 * ms,
			5 * ms,
			10 * ms,
			20 * ms,
			50 * ms,
			100 * ms,
			200 * ms,
			500 * ms,
			1000 * ms,
			2000 * ms,
			5000 * ms,
			10000 * ms,
			20000 * ms,
			50000 * ms,
			100000 * ms,
			200000 * ms,
			500000 * ms,
			1000000 * ms,
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
		return newPrometheusScope(logger, convertPrometheusConfigToTally(c.Prometheus), &c.ClientConfig)
	}
	return tally.NoopScope
}

func buildHistogramBuckets(
	config *PrometheusConfig,
) []prometheus.HistogramObjective {
	var result []prometheus.HistogramObjective
	if len(config.DefaultHistogramBuckets) > 0 {
		result = make([]prometheus.HistogramObjective, len(config.DefaultHistogramBuckets))
		for i, item := range config.DefaultHistogramBuckets {
			result[i].Upper = item.Upper
		}
	} else if len(config.DefaultHistogramBoundaries) > 0 {
		result = histogramBoundariesToHistogramObjectives(config.DefaultHistogramBoundaries)
	}
	return result
}

func convertPrometheusConfigToTally(
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
		DefaultHistogramBuckets:  buildHistogramBuckets(config),
		DefaultSummaryObjectives: defaultObjectives,
		OnError:                  config.OnError,
	}
}

func setDefaultPerUnitHistogramBoundaries(clientConfig *ClientConfig) {
	if clientConfig.PerUnitHistogramBoundaries == nil {
		clientConfig.PerUnitHistogramBoundaries = make(map[string][]float64)
	}
	for unit, bucket := range defaultPerUnitHistogramBoundaries {
		if _, ok := clientConfig.PerUnitHistogramBoundaries[unit]; !ok {
			clientConfig.PerUnitHistogramBoundaries[unit] = bucket
		}
	}
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
	clientConfig *ClientConfig,
) tally.Scope {
	reporter, err := config.NewReporter(
		prometheus.ConfigurationOptions{
			Registry: prom.NewRegistry(),
			OnError: func(err error) {
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
	return scope
}

func histogramBoundariesToHistogramObjectives(boundaries []float64) []prometheus.HistogramObjective {
	var result []prometheus.HistogramObjective
	for _, value := range boundaries {
		result = append(
			result,
			prometheus.HistogramObjective{
				Upper: value,
			},
		)
	}
	return result
}

// MetricsHandlerFromConfig is used at startup to construct a MetricsHandler
func MetricsHandlerFromConfig(logger log.Logger, c *Config) MetricsHandler {
	if c == nil {
		return NoopMetricsHandler
	}

	setDefaultPerUnitHistogramBoundaries(&c.ClientConfig)

	if c.Prometheus != nil && c.Prometheus.Framework == FrameworkOpentelemetry {
		otelProvider, err := NewOpenTelemetryProvider(logger, c.Prometheus, &c.ClientConfig)
		if err != nil {
			logger.Fatal(err.Error())
		}

		return NewOtelMetricsHandler(logger, otelProvider, c.ClientConfig)
	}

	return NewTallyMetricsHandler(
		c.ClientConfig,
		NewScope(logger, c),
	)
}

func configExcludeTags(cfg ClientConfig) map[string]map[string]struct{} {
	tagsToFilter := make(map[string]map[string]struct{})
	for key, val := range cfg.ExcludeTags {
		exclusions := make(map[string]struct{})
		for _, val := range val {
			exclusions[val] = struct{}{}
		}
		tagsToFilter[key] = exclusions
	}
	return tagsToFilter
}
