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
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	otelunit "go.opentelemetry.io/otel/metric/unit"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// otelMetricsHandler is a Handler for OpenTelemetry metrics.
type otelMetricsHandler struct {
	l           log.Logger
	tags        []Tag
	provider    OpenTelemetryProvider
	excludeTags excludeTags
}

var _ Handler = (*otelMetricsHandler)(nil)

func NewOtelMetricsHandler(l log.Logger, o OpenTelemetryProvider, cfg ClientConfig) *otelMetricsHandler {
	return &otelMetricsHandler{
		l:           l,
		provider:    o,
		excludeTags: configExcludeTags(cfg),
	}
}

// WithTags creates a new Handler with the provided Tag list.
// Tags are merged with the registered tags from the source Handler.
func (omp *otelMetricsHandler) WithTags(tags ...Tag) Handler {
	return &otelMetricsHandler{
		provider:    omp.provider,
		excludeTags: omp.excludeTags,
		tags:        append(omp.tags, tags...),
	}
}

// Counter obtains a counter for the given name and Option list.
func (omp *otelMetricsHandler) Counter(counter string, opts ...Option) CounterIface {
	c, err := omp.provider.GetMeter().Int64Counter(counter, buildOtelOptions[instrument.Int64Option](
		opts,
	)...)
	if err != nil {
		omp.l.Fatal("error getting metric", tag.NewStringTag("MetricName", counter), tag.Error(err))
	}

	return CounterFunc(func(i int64, t ...Tag) {
		c.Add(context.Background(), i, tagsToAttributes(omp.tags, t, omp.excludeTags)...)
	})
}

// Gauge obtains a gauge for the given name and Option list.
func (omp *otelMetricsHandler) Gauge(gauge string, opts ...Option) GaugeIface {
	c, err := omp.provider.GetMeter().Float64ObservableGauge(gauge, buildOtelOptions[instrument.Float64ObserverOption](
		opts,
	)...)
	if err != nil {
		omp.l.Fatal("error getting metric", tag.NewStringTag("MetricName", gauge), tag.Error(err))
	}

	return GaugeFunc(func(i float64, t ...Tag) {
		_, err = omp.provider.GetMeter().RegisterCallback(func(ctx context.Context, o metric.Observer) error {
			o.ObserveFloat64(c, i, tagsToAttributes(omp.tags, t, omp.excludeTags)...)
			return nil
		}, c)
		if err != nil {
			omp.l.Fatal("error setting callback metric update", tag.NewStringTag("MetricName", gauge), tag.Error(err))
		}
	})
}

// Timer obtains a timer for the given name and Option list.
func (omp *otelMetricsHandler) Timer(timer string, opts ...Option) TimerIface {
	c, err := omp.provider.GetMeter().Int64Histogram(timer, buildOtelOptions[instrument.Int64Option](
		opts,
		instrument.WithUnit(Milliseconds),
	)...)
	if err != nil {
		omp.l.Fatal("error getting metric", tag.NewStringTag("MetricName", timer), tag.Error(err))
	}

	return TimerFunc(func(i time.Duration, t ...Tag) {
		c.Record(context.Background(), i.Milliseconds(), tagsToAttributes(omp.tags, t, omp.excludeTags)...)
	})
}

// Histogram obtains a histogram for the given name and Option list.
func (omp *otelMetricsHandler) Histogram(histogram string, unit MetricUnit, opts ...Option) HistogramIface {
	c, err := omp.provider.GetMeter().Int64Histogram(histogram, buildOtelOptions[instrument.Int64Option](
		opts,
		instrument.WithUnit(otelunit.Unit(unit)),
	)...)
	if err != nil {
		omp.l.Fatal("error getting metric", tag.NewStringTag("MetricName", histogram), tag.Error(err))
	}

	return CounterFunc(func(i int64, t ...Tag) {
		c.Record(context.Background(), i, tagsToAttributes(omp.tags, t, omp.excludeTags)...)
	})
}

// buildOtelOptions returns a list of otel options derived from the Option list, appended to an existing list of otel
// options.
// This function will panic if T is not implemented by instrument.Option, so it must remain unexported.
// See https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#help for a description
// of what the help text is for OpenTelemetry.
func buildOtelOptions[T any](opts []Option, otelOpts ...T) []T {
	if text := buildParams(opts).optionalHelpText; text != nil {
		otelOpts = append(otelOpts, instrument.WithDescription(*text).(T))
	}

	return otelOpts
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
