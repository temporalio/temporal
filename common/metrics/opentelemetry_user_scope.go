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
)

type opentelemetryUserScope struct {
	reporter OpentelemetryReporter
	labels   []attribute.KeyValue
	tags     map[string]string

	gaugeCache OtelGaugeCache
}

func newOpentelemetryUserScope(
	reporter OpentelemetryReporter,
	tags map[string]string,
	gaugeCache OtelGaugeCache,
) *opentelemetryUserScope {
	result := &opentelemetryUserScope{
		reporter:   reporter,
		tags:       tags,
		gaugeCache: gaugeCache,
	}
	result.labels = tagMapToLabelArray(tags)
	return result
}

func (o opentelemetryUserScope) IncCounter(counter string) {
	o.AddCounter(counter, 1)
}

func (o opentelemetryUserScope) AddCounter(counter string, delta int64) {
	ctx := context.Background()
	o.reporter.GetMeterMust().NewInt64Counter(counter).Add(ctx, delta, o.labels...)
}

func (o opentelemetryUserScope) StartTimer(timer string) Stopwatch {
	metric := newOpenTelemetryStopwatchMetric(
		o.reporter.GetMeterMust().NewInt64Histogram(timer),
		o.labels)
	return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{metric})
}

func (o opentelemetryUserScope) RecordTimer(timer string, d time.Duration) {
	ctx := context.Background()
	o.reporter.GetMeterMust().NewInt64Histogram(timer).Record(ctx, d.Nanoseconds(), o.labels...)
}

func (o opentelemetryUserScope) RecordDistribution(id string, d int) {
	value := int64(d)
	ctx := context.Background()
	o.reporter.GetMeterMust().NewInt64Histogram(id).Record(ctx, value, o.labels...)
}

func (o opentelemetryUserScope) UpdateGauge(gauge string, value float64) {
	o.gaugeCache.Set(gauge, o.tags, value)
}

// Tagged provides new scope with added and/or overriden tags values.
func (o opentelemetryUserScope) Tagged(tags map[string]string) UserScope {
	tagMap := make(map[string]string, len(tags)+len(o.tags))
	for key, value := range o.tags {
		tagMap[key] = value
	}

	for key, value := range tags {
		tagMap[key] = value
	}
	return newOpentelemetryUserScope(o.reporter, tagMap, o.gaugeCache)
}
