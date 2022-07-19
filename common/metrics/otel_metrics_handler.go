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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/instrument"
	otelunit "go.opentelemetry.io/otel/metric/unit"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// MetricsHandler is an event.Handler for OpenTelemetry metrics.
// Its Event method handles Metric events and ignores all others.
type otelMetricsHandler struct {
	l           log.Logger
	tags        []Tag
	provider    OpenTelemetryProvider
	excludeTags excludeTags
}

var _ MetricsHandler = (*otelMetricsHandler)(nil)

func NewOtelMetricsHandler(l log.Logger, o OpenTelemetryProvider, cfg ClientConfig) *otelMetricsHandler {
	return &otelMetricsHandler{
		provider:    o,
		excludeTags: configExcludeTags(cfg),
	}
}

// WithTags creates a new MetricProvder with provided []Tag
// Tags are merged with registered Tags from the source MetricsHandler
func (omp *otelMetricsHandler) WithTags(tags ...Tag) MetricsHandler {
	return &otelMetricsHandler{
		provider:    omp.provider,
		excludeTags: omp.excludeTags,
		tags:        append(omp.tags, tags...),
	}
}

// Counter obtains a counter for the given name and MetricOptions.
func (omp *otelMetricsHandler) Counter(counter string) CounterMetric {
	c, err := omp.provider.GetMeter().SyncInt64().Counter(counter)
	if err != nil {
		omp.l.Fatal("error getting metric", tag.NewStringTag("MetricName", counter), tag.Error(err))
	}

	return CounterMetricFunc(func(i int64, t ...Tag) {
		c.Add(context.Background(), i, tagsToAttributes(omp.tags, t, omp.excludeTags)...)
	})
}

// Gauge obtains a gauge for the given name and MetricOptions.
func (omp *otelMetricsHandler) Gauge(gauge string) GaugeMetric {
	c, err := omp.provider.GetMeter().SyncFloat64().UpDownCounter(gauge)
	if err != nil {
		omp.l.Fatal("error getting metric", tag.NewStringTag("MetricName", gauge), tag.Error(err))
	}

	return GaugeMetricFunc(func(i float64, t ...Tag) {
		c.Add(context.Background(), i, tagsToAttributes(omp.tags, t, omp.excludeTags)...)
	})
}

// Timer obtains a timer for the given name and MetricOptions.
func (omp *otelMetricsHandler) Timer(timer string) TimerMetric {
	c, err := omp.provider.GetMeter().SyncInt64().Histogram(timer, instrument.WithUnit(otelunit.Unit(Milliseconds)))
	if err != nil {
		omp.l.Fatal("error getting metric", tag.NewStringTag("MetricName", timer), tag.Error(err))
	}

	return TimerMetricFunc(func(i time.Duration, t ...Tag) {
		c.Record(context.Background(), i.Nanoseconds(), tagsToAttributes(omp.tags, t, omp.excludeTags)...)
	})
}

// Histogram obtains a histogram for the given name and MetricOptions.
func (omp *otelMetricsHandler) Histogram(histogram string, unit MetricUnit) HistogramMetric {
	c, err := omp.provider.GetMeter().SyncInt64().Histogram(histogram, instrument.WithUnit(otelunit.Unit(unit)))
	if err != nil {
		omp.l.Fatal("error getting metric", tag.NewStringTag("MetricName", histogram), tag.Error(err))
	}

	return CounterMetricFunc(func(i int64, t ...Tag) {
		c.Record(context.Background(), i, tagsToAttributes(omp.tags, t, omp.excludeTags)...)
	})
}

func (omp *otelMetricsHandler) Stop(l log.Logger) {
	omp.provider.Stop(l)
}

// tagsToAttributes helper to merge registred tags and additional tags converting to attribute.KeyValue struct
func tagsToAttributes(t1 []Tag, t2 []Tag, e excludeTags) []attribute.KeyValue {
	var attrs []attribute.KeyValue

	convert := func(tag Tag) attribute.KeyValue {
		if vals, ok := e[tag.Key()]; ok {
			if _, ok := vals[tag.Value()]; !ok {
				return attribute.String(tag.Key(), tagExcludedValue)
			}
		}

		return attribute.String(tag.Key(), tag.Value())
	}

	for i := range t1 {
		attrs = append(attrs, convert(t1[i]))
	}

	for i := range t2 {
		attrs = append(attrs, convert(t2[i]))
	}

	return attrs
}
