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

	"go.opentelemetry.io/otel/metric/instrument"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type openTelemetryUserScope struct {
	meter  metric.Meter
	labels []attribute.KeyValue
	tags   map[string]string

	gaugeCache OtelGaugeCache
}

func NewOpenTelemetryUserScope(
	meter metric.Meter,
	tags map[string]string,
	gaugeCache OtelGaugeCache,
) *openTelemetryUserScope {
	result := &openTelemetryUserScope{
		meter:      meter,
		tags:       tags,
		gaugeCache: gaugeCache,
	}
	result.labels = tagMapToLabelArray(tags)
	return result
}

func (o openTelemetryUserScope) IncCounter(counter string) {
	o.AddCounter(counter, 1)
}

func (o openTelemetryUserScope) AddCounter(counter string, delta int64) {
	ctx := context.Background()

	c, err := o.meter.SyncInt64().Counter(counter)
	if err != nil {
		panic(err)
	}
	c.Add(ctx, delta, o.labels...)
}

func (o openTelemetryUserScope) StartTimer(timer string) Stopwatch {
	h, err := o.meter.SyncInt64().Histogram(timer)
	if err != nil {
		panic(err)
	}

	metric := newOpenTelemetryStopwatchMetric(h, o.labels)
	return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{metric})
}

func (o openTelemetryUserScope) RecordTimer(timer string, d time.Duration) {
	ctx := context.Background()

	h, err := o.meter.SyncInt64().Histogram(timer)
	if err != nil {
		panic(err)
	}
	h.Record(ctx, d.Nanoseconds(), o.labels...)
}

func (o openTelemetryUserScope) RecordDistribution(id string, unit MetricUnit, d int) {
	value := int64(d)
	ctx := context.Background()

	opt := []instrument.Option{unitToOptions(unit)}
	h, err := o.meter.SyncInt64().Histogram(id, opt...)
	if err != nil {
		panic(err)
	}
	h.Record(ctx, value, o.labels...)
}

func (o openTelemetryUserScope) UpdateGauge(gauge string, value float64) {
	o.gaugeCache.Set(gauge, o.tags, value)
}

// Tagged provides new scope with added and/or overriden tags values.
func (o openTelemetryUserScope) Tagged(tags map[string]string) UserScope {
	tagMap := make(map[string]string, len(tags)+len(o.tags))
	for key, value := range o.tags {
		tagMap[key] = value
	}

	for key, value := range tags {
		tagMap[key] = value
	}
	return NewOpenTelemetryUserScope(o.meter, tagMap, o.gaugeCache)
}
