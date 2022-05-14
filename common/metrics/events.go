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

	"go.opentelemetry.io/otel/metric"
	"golang.org/x/exp/event"
	"golang.org/x/exp/event/otel"
)

type (
	eventMetricProvider struct {
		exporter *event.Exporter
		context  context.Context
		tags     []Tag
	}

	Option interface {
		apply(*event.ExporterOptions)
	}

	OptionFunc func(*event.ExporterOptions)
)

func (o OptionFunc) apply(eo *event.ExporterOptions) { o(eo) }

var _ MetricProvider = (*eventMetricProvider)(nil)

// NewMetricEventExporter provides an event.Exporter struct given event.Handler and options
func NewMetricEventExporter(h event.Handler, opts ...Option) *event.Exporter {
	eo := &event.ExporterOptions{}
	for _, opt := range opts {
		opt.apply(eo)
	}

	return event.NewExporter(h, eo)
}

// NewEventHandler provides an event.Handler given OpenTelemetryProvider to supply an otel.Meter
func NewEventHandler(m metric.Meter) event.Handler {
	return otel.NewMetricHandler(m)
}

// NewEventMetricProvider provides an eventMetricProvider given event.Exporter struct
func NewEventMetricProvider(e *event.Exporter) *eventMetricProvider {
	return &eventMetricProvider{
		exporter: e,
		context:  event.WithExporter(context.Background(), e),
	}
}

// WithLoggingDisabled disables logging
func WithLoggingDisabled() Option {
	return OptionFunc(func(eo *event.ExporterOptions) {
		eo.DisableLogging = true
	})
}

// WithTracingDisabled disables tracing
func WithTracingDisabled() Option {
	return OptionFunc(func(eo *event.ExporterOptions) {
		eo.DisableTracing = true
	})
}

// WithAnnotationsDisabled disables annotations
func WithAnnotationsDisabled() Option {
	return OptionFunc(func(eo *event.ExporterOptions) {
		eo.DisableAnnotations = true
	})
}

// WithMetricsDisabled disables metrics
func WithMetricsDisabled() Option {
	return OptionFunc(func(eo *event.ExporterOptions) {
		eo.DisableMetrics = true
	})
}

/// WithNamespacingDisabled disables namespacing
func WithNamespacingDisabled() Option {
	return OptionFunc(func(eo *event.ExporterOptions) {
		eo.EnableNamespaces = false
	})
}

func metricOptions(opts ...MetricOption) *event.MetricOptions {
	emo := &event.MetricOptions{}
	for _, opt := range opts {
		opt.apply(emo)
	}

	return emo
}

// WithTags creates a new MetricProvder with provided []Tag
func (emp *eventMetricProvider) WithTags(tags ...Tag) MetricProvider {
	return &eventMetricProvider{
		exporter: emp.exporter,
		context:  emp.context,
		tags:     tags,
	}
}

// Counter obtains a counter for the given name.
func (emp *eventMetricProvider) Counter(n string, opts ...MetricOption) CounterMetric {
	return CounterMetricFunc(func(i int64, t ...Tag) {
		e := event.NewCounter(n, metricOptions(opts...))
		e.Record(emp.context, i, emp.tagsToLabels(t)...)
	})
}

// Gauge obtains a gauge for the given name.
func (emp *eventMetricProvider) Gauge(n string, opts ...MetricOption) GaugeMetric {
	return GaugeMetricFunc(func(f float64, t ...Tag) {
		e := event.NewFloatGauge(n, metricOptions(opts...))
		e.Record(emp.context, f, emp.tagsToLabels(t)...)
	})
}

// Timer obtains a timer for the given name.
func (emp *eventMetricProvider) Timer(n string, opts ...MetricOption) TimerMetric {
	return TimerMetricFunc(func(d time.Duration, t ...Tag) {
		e := event.NewDuration(n, metricOptions(opts...))
		e.Record(emp.context, d, emp.tagsToLabels(t)...)
	})
}

// Histogram obtains a histogram for the given name.
func (emp *eventMetricProvider) Histogram(n string, opts ...MetricOption) HistogramMetric {
	return HistogramMetricFunc(func(i int64, t ...Tag) {
		e := event.NewIntDistribution(n, metricOptions(opts...))
		e.Record(emp.context, i, emp.tagsToLabels(t)...)
	})
}

// Tags returns registered []Tag
func (emp *eventMetricProvider) Tags() []Tag {
	return emp.tags
}

// tagsToLabels helper to merge registred tags and additional tags converting to event.Label struct
func (emp *eventMetricProvider) tagsToLabels(tags []Tag) []event.Label {
	l := make([]event.Label, len(tags))
	t := append(emp.Tags(), tags...)

	for i := range t {
		l[i] = event.String(t[i].Key(), t[i].Value())
	}

	return l
}
