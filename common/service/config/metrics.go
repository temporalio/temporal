// Copyright (c) 2017 Uber Technologies, Inc.
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

package config

import (
	"net/http"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	prom "github.com/m3db/prometheus_client_golang/prometheus"
	"github.com/uber-go/tally"
	"github.com/uber-go/tally/prometheus"
	tallystatsdreporter "github.com/uber-go/tally/statsd"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	statsdreporter "github.com/uber/cadence/common/metrics/tally/statsd"
)

// NewScope builds a new tally scope
// for this metrics configuration
//
// If the underlying configuration is
// valid for multiple reporter types,
// only one of them will be used for
// reporting. Currently, m3 is preferred
// over statsd
func (c *Metrics) NewScope(logger log.Logger) tally.Scope {
	if c.M3 != nil {
		return c.newM3Scope(logger)
	}
	if c.Statsd != nil {
		return c.newStatsdScope(logger)
	}
	if c.Prometheus != nil {
		return c.newPrometheusScope(logger)
	}
	return tally.NoopScope
}

// newM3Scope returns a new m3 scope with
// a default reporting interval of a second
func (c *Metrics) newM3Scope(logger log.Logger) tally.Scope {
	reporter, err := c.M3.NewReporter()
	if err != nil {
		logger.Fatal("error creating m3 reporter", tag.Error(err))
	}
	scopeOpts := tally.ScopeOptions{
		Tags:           c.Tags,
		CachedReporter: reporter,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	return scope
}

// newM3Scope returns a new statsd scope with
// a default reporting interval of a second
func (c *Metrics) newStatsdScope(logger log.Logger) tally.Scope {
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
		Tags:     c.Tags,
		Reporter: reporter,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	return scope
}

// newPrometheusScope returns a new prometheus scope with
// a default reporting interval of a second
func (c *Metrics) newPrometheusScope(logger log.Logger) tally.Scope {
	reporter, err := NewPrometheusReporter(
		c.Prometheus,
		prometheus.ConfigurationOptions{
			OnError: func(err error) {
				logger.Warn("error in prometheus reporter", tag.Error(err))
			},
		},
	)
	if err != nil {
		logger.Fatal("error creating prometheus reporter", tag.Error(err))
	}
	scopeOpts := tally.ScopeOptions{
		Tags:           c.Tags,
		CachedReporter: reporter,
		Separator:      prometheus.DefaultSeparator,
	}
	scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	return scope
}

// NewPrometheusReporter - creates a prometheus reporter
// N.B - copy of the NewReporter method in tally - https://github.com/uber-go/tally/blob/master/prometheus/config.go#L77
// as the above method does not allow setting a separate registry per root
// which is necessary when we are running multiple roles within a same process
func NewPrometheusReporter(
	config *prometheus.Configuration,
	configOpts prometheus.ConfigurationOptions,
) (prometheus.Reporter, error) {
	var opts prometheus.Options
	opts.OnRegisterError = configOpts.OnError

	switch config.TimerType {
	case "summary":
		opts.DefaultTimerType = prometheus.SummaryTimerType
	case "histogram":
		opts.DefaultTimerType = prometheus.HistogramTimerType
	}

	if len(config.DefaultHistogramBuckets) > 0 {
		var values []float64
		for _, value := range config.DefaultHistogramBuckets {
			values = append(values, value.Upper)
		}
		opts.DefaultHistogramBuckets = values
	}

	if len(config.DefaultSummaryObjectives) > 0 {
		values := make(map[float64]float64)
		for _, value := range config.DefaultSummaryObjectives {
			values[value.Percentile] = value.AllowedError
		}
		opts.DefaultSummaryObjectives = values
	}

	opts.Registerer = prom.NewRegistry()

	reporter := prometheus.NewReporter(opts)

	path := "/metrics"
	if handlerPath := strings.TrimSpace(config.HandlerPath); handlerPath != "" {
		path = handlerPath
	}

	if addr := strings.TrimSpace(config.ListenAddress); addr == "" {
		http.Handle(path, reporter.HTTPHandler())
	} else {
		mux := http.NewServeMux()
		mux.Handle(path, reporter.HTTPHandler())
		go func() {
			if err := http.ListenAndServe(addr, mux); err != nil {
				configOpts.OnError(err)
			}
		}()
	}

	return reporter, nil
}
