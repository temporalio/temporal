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
	eventMetricProvider struct {
		context context.Context
		tags    []Tag
	}

	// MetricHandler represents the extension point for collection instruments
	// typedef of event.Handler from https://pkg.go.dev/golang.org/x/exp/event#Handler
	MetricHandler = event.Handler
)

var _ MetricProvider = (*eventMetricProvider)(nil)

// MetricHandlerFromConfig is used at startup to construct
func MetricHandlerFromConfig(logger log.Logger, c *Config) MetricHandler {
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
				c.ClientConfig.PerUnitHistogramBoundaries,
			)
		case FrameworkOpentelemetry:
			otelProvider, err := NewOpenTelemetryProvider(logger, c.Prometheus, &c.ClientConfig)
			if err != nil {
				logger.Fatal(err.Error())
			}

			return NewOtelMetricHandler(logger, otelProvider.GetMeter())
		}
	}

	return NewTallyMetricHandler(
		logger,
		NewScope(logger, c),
		c.ClientConfig.PerUnitHistogramBoundaries,
	)
}

// NewEventMetricProvider provides an eventMetricProvider given event.Exporter struct
func NewEventMetricProvider(h MetricHandler) *eventMetricProvider {
	eo := &event.ExporterOptions{
		DisableLogging: true,
		DisableTracing: true,
	}

	return &eventMetricProvider{
		context: event.WithExporter(context.Background(), event.NewExporter(h, eo)),
		tags:    []Tag{},
	}
}

// WithTags creates a new MetricProvder with provided []Tag
// Tags are merged with registered Tags from the source MetricProvider
func (emp *eventMetricProvider) WithTags(tags ...Tag) MetricProvider {
	return &eventMetricProvider{
		context: emp.context,
		tags:    append(emp.tags, tags...),
	}
}

// Counter obtains a counter for the given name.
func (emp *eventMetricProvider) Counter(n string, m *MetricOptions) CounterMetric {
	return CounterMetricFunc(func(i int64, t ...Tag) {
		e := event.NewCounter(n, m)
		e.Record(emp.context, i, emp.tagsToLabels(t)...)
	})
}

// Gauge obtains a gauge for the given name.
func (emp *eventMetricProvider) Gauge(n string, m *MetricOptions) GaugeMetric {
	return GaugeMetricFunc(func(f float64, t ...Tag) {
		e := event.NewFloatGauge(n, m)
		e.Record(emp.context, f, emp.tagsToLabels(t)...)
	})
}

// Timer obtains a timer for the given name.
func (emp *eventMetricProvider) Timer(n string, m *MetricOptions) TimerMetric {
	return TimerMetricFunc(func(d time.Duration, t ...Tag) {
		e := event.NewDuration(n, m)
		e.Record(emp.context, d, emp.tagsToLabels(t)...)
	})
}

// Histogram obtains a histogram for the given name.
func (emp *eventMetricProvider) Histogram(n string, m *MetricOptions) HistogramMetric {
	return HistogramMetricFunc(func(i int64, t ...Tag) {
		e := event.NewIntDistribution(n, m)
		e.Record(emp.context, i, emp.tagsToLabels(t)...)
	})
}

// tagsToLabels helper to merge registred tags and additional tags converting to event.Label struct
func (emp *eventMetricProvider) tagsToLabels(tags []Tag) []event.Label {
	l := make([]event.Label, len(emp.tags)+len(tags))
	t := append(emp.tags, tags...)

	for i := range t {
		l[i] = event.String(t[i].Key(), t[i].Value())
	}

	return l
}
