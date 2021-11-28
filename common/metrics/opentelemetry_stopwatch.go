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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination opentelemetry_stopwatch_mocks.go

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.temporal.io/server/common/clock"
)

type (
	opentelemetryStopwatch struct {
		timeP       clock.TimeSource
		start       time.Time
		toSubstract time.Duration
		metrics     []openTelemetryStopwatchMetric
	}

	openTelemetryStopwatchMetric interface {
		Record(ctx context.Context, value time.Duration)
	}

	openTelemetryStopwatchMetricImpl struct {
		timer  metric.Int64Histogram
		labels []attribute.KeyValue
	}
)

func newOpenTelemetryStopwatchMetric(
	timer metric.Int64Histogram,
	labels []attribute.KeyValue,
) *openTelemetryStopwatchMetricImpl {
	return &openTelemetryStopwatchMetricImpl{
		timer:  timer,
		labels: labels,
	}
}

func newOpenTelemetryStopwatchCustomTimer(
	metricsMeta []openTelemetryStopwatchMetric, timeP clock.TimeSource,
) *opentelemetryStopwatch {
	return &opentelemetryStopwatch{timeP, timeP.Now(), 0, metricsMeta}
}

func newOpenTelemetryStopwatch(metricsMeta []openTelemetryStopwatchMetric) *opentelemetryStopwatch {
	return newOpenTelemetryStopwatchCustomTimer(metricsMeta, clock.NewRealTimeSource())
}

func (o *opentelemetryStopwatch) Stop() {
	ctx := context.Background()
	d := o.timeP.Now().Sub(o.start)
	d -= o.toSubstract

	for _, m := range o.metrics {
		m.Record(ctx, d)
	}
}

func (o *opentelemetryStopwatch) Subtract(toSubstract time.Duration) {
	o.toSubstract = o.toSubstract + toSubstract
}

func (om *openTelemetryStopwatchMetricImpl) Record(ctx context.Context, d time.Duration) {
	om.timer.Record(ctx, d.Nanoseconds(), om.labels...)
}
