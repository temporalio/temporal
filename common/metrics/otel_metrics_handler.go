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
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// otelMetricsHandler is an adapter around an OpenTelemetry [metric.Meter] that implements the [Handler] interface.
type otelMetricsHandler struct {
	l           log.Logger
	tags        []Tag
	provider    OpenTelemetryProvider
	excludeTags excludeTags
	catalog     catalog
}

var _ Handler = (*otelMetricsHandler)(nil)

// NewOtelMetricsHandler returns a new Handler that uses the provided OpenTelemetry [metric.Meter] to record metrics.
// This OTel handler supports metric descriptions for metrics registered with the New*Def functions. However, those
// functions must be called before this constructor. Otherwise, the descriptions will be empty. This is because the
// OTel metric descriptions are generated from the globalRegistry. You may also record metrics that are not registered
// via the New*Def functions. In that case, the metric description will be the OTel default (the metric name itself).
func NewOtelMetricsHandler(
	l log.Logger,
	o OpenTelemetryProvider,
	cfg ClientConfig,
) (*otelMetricsHandler, error) {
	c, err := globalRegistry.buildCatalog()
	if err != nil {
		return nil, fmt.Errorf("failed to build metrics catalog: %w", err)
	}
	return &otelMetricsHandler{
		l:           l,
		provider:    o,
		excludeTags: configExcludeTags(cfg),
		catalog:     c,
	}, nil
}

// WithTags creates a new Handler with the provided Tag list.
// Tags are merged with the existing tags.
func (omp *otelMetricsHandler) WithTags(tags ...Tag) Handler {
	newHandler := *omp
	newHandler.tags = append(newHandler.tags, tags...)
	return &newHandler
}

// Counter obtains a counter for the given name.
func (omp *otelMetricsHandler) Counter(counter string) CounterIface {
	opts := addOptions(omp, counterOptions{}, counter)
	c, err := omp.provider.GetMeter().Int64Counter(counter, opts...)
	if err != nil {
		omp.l.Error("error getting metric", tag.NewStringTag("MetricName", counter), tag.Error(err))
		return CounterFunc(func(i int64, t ...Tag) {})
	}

	return CounterFunc(func(i int64, t ...Tag) {
		option := metric.WithAttributes(tagsToAttributes(omp.tags, t, omp.excludeTags)...)
		c.Add(context.Background(), i, option)
	})
}

// Gauge obtains a gauge for the given name.
func (omp *otelMetricsHandler) Gauge(gauge string) GaugeIface {
	opts := addOptions(omp, gaugeOptions{}, gauge)
	c, err := omp.provider.GetMeter().Float64ObservableGauge(gauge, opts...)
	if err != nil {
		omp.l.Error("error getting metric", tag.NewStringTag("MetricName", gauge), tag.Error(err))
		return GaugeFunc(func(i float64, t ...Tag) {})
	}

	return GaugeFunc(func(i float64, t ...Tag) {
		_, err = omp.provider.GetMeter().RegisterCallback(func(ctx context.Context, o metric.Observer) error {
			option := metric.WithAttributes(tagsToAttributes(omp.tags, t, omp.excludeTags)...)
			o.ObserveFloat64(c, i, option)
			return nil
		}, c)
		if err != nil {
			omp.l.Error("error setting callback metric update", tag.NewStringTag("MetricName", gauge), tag.Error(err))
		}
	})
}

// Timer obtains a timer for the given name.
func (omp *otelMetricsHandler) Timer(timer string) TimerIface {
	opts := addOptions(omp, histogramOptions{metric.WithUnit(Milliseconds)}, timer)
	c, err := omp.provider.GetMeter().Int64Histogram(timer, opts...)
	if err != nil {
		omp.l.Error("error getting metric", tag.NewStringTag("MetricName", timer), tag.Error(err))
		return TimerFunc(func(i time.Duration, t ...Tag) {})
	}

	return TimerFunc(func(i time.Duration, t ...Tag) {
		option := metric.WithAttributes(tagsToAttributes(omp.tags, t, omp.excludeTags)...)
		c.Record(context.Background(), i.Milliseconds(), option)
	})
}

// Histogram obtains a histogram for the given name.
func (omp *otelMetricsHandler) Histogram(histogram string, unit MetricUnit) HistogramIface {
	opts := addOptions(omp, histogramOptions{metric.WithUnit(string(unit))}, histogram)
	c, err := omp.provider.GetMeter().Int64Histogram(histogram, opts...)
	if err != nil {
		omp.l.Error("error getting metric", tag.NewStringTag("MetricName", histogram), tag.Error(err))
		return HistogramFunc(func(i int64, t ...Tag) {})
	}

	return HistogramFunc(func(i int64, t ...Tag) {
		option := metric.WithAttributes(tagsToAttributes(omp.tags, t, omp.excludeTags)...)
		c.Record(context.Background(), i, option)
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
