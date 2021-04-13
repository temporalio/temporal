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
	"errors"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	prom "github.com/m3db/prometheus_client_golang/prometheus"
	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
	"github.com/uber-go/tally/prometheus"
	tallystatsdreporter "github.com/uber-go/tally/statsd"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	statsdreporter "go.temporal.io/server/common/metrics/tally/statsd"
)

type (
	// Config contains the config items for metrics subsystem
	Config struct {
		// M3 is the configuration for m3 metrics reporter
		M3 *m3.Configuration `yaml:"m3"`
		// Statsd is the configuration for statsd reporter
		Statsd *StatsdConfig `yaml:"statsd"`
		// Prometheus is the configuration for prometheus reporter
		Prometheus *prometheus.Configuration `yaml:"prometheus"`
		// Using new name for backwards compatibility.
		// Config for Prometheus metrics reporter for server reported metrics.
		PrometheusServer *PrometheusConfig `yaml:"prometheusServer"`
		// {optional} Config for Prometheus metrics reporter for SDK reported metrics.
		PrometheusSDK *PrometheusConfig `yaml:"prometheusSDK"`
		// Tags is the set of key-value pairs to be reported as part of every metric
		Tags map[string]string `yaml:"tags"`
		// Prefix sets the prefix to all outgoing metrics
		Prefix string `yaml:"prefix"`
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
	}

	// PrometheusConfig is a new format for config for prometheus metrics.
	PrometheusConfig struct {
		// Metric framework: Tally/OpenTelemetry
		Framework string `yaml:framework`
		// Address for prometheus to serve metrics from.
		ListenAddress string `yaml:"listenAddress"`
		// DefaultHistogramBoundaries defines the default histogram bucket
		// boundaries.
		DefaultHistogramBoundaries []float64 `yaml:"defaultHistogramBoundaries"`
		// HandlerPath if specified will be used instead of using the default
		// HTTP handler path "/metrics".
		HandlerPath string `yaml:"handlerPath"`
	}
)

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

	defaultQuantiles = []float64{50, 75, 90, 95, 99}

	defaultHistogramBoundaries = []float64{
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
	}
)

// InitMetricReporters is a root function for initalizing metrics clients.
//
// Usage pattern
// serverReporter, sdkReporter, err := c.InitMetricReporters
// metricsClient := serverReporter.newClient(logger, serviceIdx)
//
// returns SeverReporter, SDKReporter, error
func (c *Config) InitMetricReporters(logger log.Logger, extensionPoint interface{}) (Reporter, Reporter, error) {
	if c.PrometheusServer == nil {
		var scope tally.Scope
		if extensionPoint != nil {
			scope = c.NewCustomReporterScope(logger, extensionPoint.(tally.BaseStatsReporter))
		} else {
			scope = c.NewScope(logger)
		}
		reporter := newTallyReporter(scope)
		return reporter, reporter, nil
	}

	return c.initReportersFromPrometheusConfig(logger, extensionPoint)
}

func (c *Config) initReportersFromPrometheusConfig(logger log.Logger, extensionPoint interface{}) (Reporter, Reporter, error) {
	if extensionPoint != nil {
		logger.Fatal("Metrics extension point is not implemented.")
	}
	serverReporter, err := c.initReporterFromPrometheusConfig(logger, c.PrometheusServer, extensionPoint)
	if err != nil {
		return nil, nil, err
	}
	sdkReporter := serverReporter
	if c.PrometheusSDK != nil {
		sdkReporter, err = c.initReporterFromPrometheusConfig(logger, c.PrometheusSDK, extensionPoint)
		if err != nil {
			return nil, nil, err
		}
	}
	return serverReporter, sdkReporter, nil
}

func (c *Config) initReporterFromPrometheusConfig(logger log.Logger, config *PrometheusConfig, extensionPoint interface{}) (Reporter, error) {
	switch config.Framework {
	case FrameworkTally:
		tallyScope := c.newTallyScopeByPrometheusConfig(logger, config)
		return newTallyReporter(tallyScope), nil
	case FrameworkOpentelemetry:
		return newOpentelemeteryReporter(logger, c.Tags, c.Prefix, config)
	default:
		logger.Error(
			"Unsupported framework type provided: " + config.Framework +
				". Using NoopMetrics.",
		)
		return nil, errors.New("Unsupported framework type: " + config.Framework)
	}
}

func (c *Config) newTallyScopeByPrometheusConfig(logger log.Logger, config *PrometheusConfig) tally.Scope {
	tallyConfig := c.convertPrometheusConfig(config)
	return c.newPrometheusScope(logger, tallyConfig)
}

// NewScope builds a new tally scope for this metrics configuration
//
// If the underlying configuration is valid for multiple reporter types,
// only one of them will be used for reporting.
//
// Current priority order is:
// m3 > statsd > prometheus
func (c *Config) NewScope(logger log.Logger) tally.Scope {
	if c.M3 != nil {
		return c.newM3Scope(logger)
	}
	if c.Statsd != nil {
		return c.newStatsdScope(logger)
	}
	if c.Prometheus != nil {
		return c.newPrometheusScope(logger, c.Prometheus)
	}
	return tally.NoopScope
}

func (c *Config) convertPrometheusConfig(config *PrometheusConfig) *prometheus.Configuration {
	return &prometheus.Configuration{
		HandlerPath:             config.HandlerPath,
		ListenAddress:           config.ListenAddress,
		TimerType:               "histogram",
		DefaultHistogramBuckets: histogramBoundariesToHistogramObjectives(config.DefaultHistogramBoundaries),
	}
}

func (c *Config) NewCustomReporterScope(logger log.Logger, customReporter tally.BaseStatsReporter) tally.Scope {
	options := tally.ScopeOptions{
		DefaultBuckets: histogramBoundariesToValueBuckets(defaultHistogramBoundaries),
	}
	if c != nil {
		options.Tags = c.Tags
		options.Prefix = c.Prefix
	}

	switch reporter := customReporter.(type) {
	case tally.StatsReporter:
		options.Reporter = reporter
	case tally.CachedStatsReporter:
		options.CachedReporter = reporter
	default:
		logger.Error("Unsupported metrics reporter type.", tag.ValueType(customReporter))
		return tally.NoopScope
	}
	scope, _ := tally.NewRootScope(options, time.Second)
	return scope
}

// newM3Scope returns a new m3 scope with
// a default reporting interval of a second
func (c *Config) newM3Scope(logger log.Logger) tally.Scope {
	reporter, err := c.M3.NewReporter()
	if err != nil {
		logger.Fatal("error creating m3 reporter", tag.Error(err))
	}
	scopeOpts := tally.ScopeOptions{
		Tags:           c.Tags,
		CachedReporter: reporter,
		Prefix:         c.Prefix,
		DefaultBuckets: histogramBoundariesToValueBuckets(defaultHistogramBoundaries),
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	return scope
}

// newM3Scope returns a new statsd scope with
// a default reporting interval of a second
func (c *Config) newStatsdScope(logger log.Logger) tally.Scope {
	config := c.Statsd
	if len(config.HostPort) == 0 {
		return tally.NoopScope
	}
	statter, err := statsd.NewBufferedClient(config.HostPort, config.Prefix, config.FlushInterval, config.FlushBytes)
	if err != nil {
		logger.Fatal("error creating statsd client", tag.Error(err))
	}
	//NOTE: according to ( https://github.com/uber-go/tally )Tally's statsd implementation doesn't support tagging.
	// Therefore, we implement Tally interface to have a statsd reporter that can support tagging
	reporter := statsdreporter.NewReporter(statter, tallystatsdreporter.Options{})
	scopeOpts := tally.ScopeOptions{
		Tags:           c.Tags,
		Reporter:       reporter,
		Prefix:         c.Prefix,
		DefaultBuckets: histogramBoundariesToValueBuckets(defaultHistogramBoundaries),
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	return scope
}

// newPrometheusScope returns a new prometheus scope with
// a default reporting interval of a second
func (c *Config) newPrometheusScope(logger log.Logger, config *prometheus.Configuration) tally.Scope {
	if len(config.DefaultHistogramBuckets) == 0 {
		config.DefaultHistogramBuckets = histogramBoundariesToHistogramObjectives(defaultHistogramBoundaries)
	}
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
		Tags:            c.Tags,
		CachedReporter:  reporter,
		Separator:       prometheus.DefaultSeparator,
		SanitizeOptions: &sanitizeOptions,
		Prefix:          c.Prefix,
		DefaultBuckets:  histogramBoundariesToValueBuckets(defaultHistogramBoundaries),
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

func histogramBoundariesToValueBuckets(buckets []float64) tally.ValueBuckets {
	return tally.ValueBuckets(buckets)
}
