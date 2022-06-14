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
	"context"
	"time"

	"golang.org/x/exp/event"

	"go.temporal.io/server/common/log"
)

type (
	eventsMetricProvider struct {
		tags []Tag
		ctx  context.Context
	}

	// MetricHandler represents the extension point for collection instruments
	// typedef of event.Handler from https://pkg.go.dev/golang.org/x/exp/event#Handler
	MetricHandler interface {
		event.Handler
		Stop(logger log.Logger)
	}
)

var (
	_                      MetricProvider = (*eventsMetricProvider)(nil)
	defaultMetricNamespace                = "go.temporal.io/server"
)

// MetricHandlerFromConfig is used at startup to construct
func MetricHandlerFromConfig(logger log.Logger, c *Config) MetricHandler {
	if c == nil {
		return NoopMetricHandler
	}

	setDefaultPerUnitHistogramBoundaries(&c.ClientConfig)
	if c.Prometheus != nil && len(c.Prometheus.Framework) > 0 {
		switch c.Prometheus.Framework {
		case FrameworkTally:
			return NewTallyMetricHandler(
				logger,
				newPrometheusScope(
					logger,
					convertPrometheusConfigToTally(c.Prometheus),
					&c.ClientConfig,
				),
				c.ClientConfig,
				c.ClientConfig.PerUnitHistogramBoundaries,
			)
		case FrameworkOpentelemetry:
			otelProvider, err := NewOpenTelemetryProvider(logger, c.Prometheus, &c.ClientConfig)
			if err != nil {
				logger.Fatal(err.Error())
			}

			return NewOtelMetricHandler(logger, otelProvider, c.ClientConfig)
		}
	}

	return NewTallyMetricHandler(
		logger,
		NewScope(logger, c),
		c.ClientConfig,
		c.ClientConfig.PerUnitHistogramBoundaries,
	)
}

// NewEventsMetricProvider provides an eventsMetricProvider given event.Exporter struct
func NewEventsMetricProvider(h MetricHandler) *eventsMetricProvider {
	eo := &event.ExporterOptions{
		DisableLogging:   true,
		DisableTracing:   true,
		EnableNamespaces: false,
	}

	return &eventsMetricProvider{
		ctx: event.WithExporter(context.Background(), event.NewExporter(h, eo)),
	}
}

// WithTags creates a new MetricProvder with provided []Tag
// Tags are merged with registered Tags from the source MetricProvider
func (emp *eventsMetricProvider) WithTags(tags ...Tag) MetricProvider {
	var t []Tag
	if len(emp.tags) != 0 {
		t = append(t, emp.tags...)
	}

	return &eventsMetricProvider{
		ctx:  emp.ctx,
		tags: append(t, tags...),
	}
}

// Counter obtains a counter for the given name.
func (emp *eventsMetricProvider) Counter(n string, m *MetricOptions) CounterMetric {
	e := event.NewCounter(n, m)
	return CounterMetricFunc(func(i int64, t ...Tag) {
		e.Record(emp.ctx, i, tagsToLabels(emp.tags, t)...)
	})
}

// Gauge obtains a gauge for the given name.
func (emp *eventsMetricProvider) Gauge(n string, m *MetricOptions) GaugeMetric {
	e := event.NewFloatGauge(n, m)
	return GaugeMetricFunc(func(f float64, t ...Tag) {
		e.Record(emp.ctx, f, tagsToLabels(emp.tags, t)...)
	})
}

// Timer obtains a timer for the given name.
func (emp *eventsMetricProvider) Timer(n string, m *MetricOptions) TimerMetric {
	e := event.NewDuration(n, m)
	return TimerMetricFunc(func(d time.Duration, t ...Tag) {
		e.Record(emp.ctx, d, tagsToLabels(emp.tags, t)...)
	})
}

// Histogram obtains a histogram for the given name.
func (emp *eventsMetricProvider) Histogram(n string, m *MetricOptions) HistogramMetric {
	e := event.NewIntDistribution(n, m)
	return HistogramMetricFunc(func(i int64, t ...Tag) {
		e.Record(emp.ctx, i, tagsToLabels(emp.tags, t)...)
	})
}

// tagsToLabels helper to merge registred tags and additional tags converting to event.Label struct
func tagsToLabels(t1 []Tag, t2 []Tag) []event.Label {
	l := make([]event.Label, 0, len(t1)+len(t2))

	for i := range t1 {
		l = append(l, event.String(t1[i].Key(), t1[i].Value()))
	}

	for i := range t2 {
		l = append(l, event.String(t2[i].Key(), t2[i].Value()))
	}

	return l
}
