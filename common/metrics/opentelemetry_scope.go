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
)

type (
	openTelemetryScope struct {
		serviceIdx        ServiceIdx
		meter             metric.Meter
		labels            []attribute.KeyValue
		tags              map[string]string
		rootScope         *openTelemetryScope
		defs              map[int]metricDefinition
		isNamespaceTagged bool

		gaugeCache OtelGaugeCache
	}
)

func newOpenTelemetryScope(
	serviceIdx ServiceIdx,
	meter metric.Meter,
	rootScope *openTelemetryScope,
	tags map[string]string,
	defs map[int]metricDefinition,
	isNamespace bool,
	gaugeCache OtelGaugeCache,
	selfAsRoot bool,
) *openTelemetryScope {
	result := &openTelemetryScope{
		serviceIdx:        serviceIdx,
		meter:             meter,
		tags:              tags,
		rootScope:         rootScope,
		defs:              defs,
		isNamespaceTagged: isNamespace,
		gaugeCache:        gaugeCache,
	}
	if selfAsRoot {
		result.rootScope = result
	}
	result.labels = tagMapToLabelArray(tags)
	return result
}

func (m *openTelemetryScope) IncCounter(id int) {
	m.AddCounter(id, 1)
}

func (m *openTelemetryScope) AddCounter(id int, delta int64) {
	def := m.defs[id]
	ctx := context.Background()
	c, err := m.meter.SyncInt64().Counter(def.metricName.String())
	if err != nil {
		panic(err)
	}
	c.Add(ctx, delta, m.labels...)

	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		c, err := m.rootScope.meter.SyncInt64().Counter(def.metricRollupName.String())
		if err != nil {
			panic(err)
		}
		c.Add(ctx, delta, m.rootScope.labels...)
	}
}

func (m *openTelemetryScope) UpdateGauge(id int, value float64) {
	def := m.defs[id]
	m.gaugeCache.Set(def.metricName.String(), m.tags, value)
	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.gaugeCache.Set(def.metricRollupName.String(), m.rootScope.tags, value)
	}
}

func (m *openTelemetryScope) StartTimer(id int) Stopwatch {
	def := m.defs[id]
	opt := make([]instrument.Option, 0, 1)
	if len(def.unit) > 0 {
		opt = append(opt, unitToOptions(def.unit))
	}

	h, err := m.meter.SyncInt64().Histogram(def.metricName.String(), opt...)
	if err != nil {
		panic(err)
	}

	timer := newOpenTelemetryStopwatchMetric(h, m.labels)

	switch {
	case !def.metricRollupName.Empty():
		hRollup, err := m.meter.SyncInt64().Histogram(def.metricRollupName.String(), opt...)
		if err != nil {
			panic(err)
		}

		timerRollup := newOpenTelemetryStopwatchMetric(hRollup, m.rootScope.labels)
		return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{timer, timerRollup})
	case m.isNamespaceTagged:
		allScope := m.taggedString(map[string]string{namespace: namespaceAllValue}, false)

		hAll, err := allScope.meter.SyncInt64().Histogram(def.metricName.String(), opt...)
		if err != nil {
			panic(err)
		}

		timerAll := newOpenTelemetryStopwatchMetric(hAll, allScope.labels)
		return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{timer, timerAll})
	default:
		return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{timer})
	}
}

func (m *openTelemetryScope) RecordTimer(id int, d time.Duration) {
	def := m.defs[id]
	ctx := context.Background()

	opt := make([]instrument.Option, 0, 1)
	if len(def.unit) > 0 {
		opt = append(opt, unitToOptions(def.unit))
	}

	h, err := m.meter.SyncInt64().Histogram(def.metricName.String(), opt...)
	if err != nil {
		panic(err)
	}
	h.Record(ctx, d.Nanoseconds(), m.labels...)

	switch {
	case !def.metricRollupName.Empty() && (m.rootScope != nil):
		hRollup, err := m.rootScope.meter.SyncInt64().Histogram(def.metricRollupName.String(), opt...)
		if err != nil {
			panic(err)
		}

		hRollup.Record(ctx, d.Nanoseconds(), m.rootScope.labels...)
	case m.isNamespaceTagged:
		hAll, err := m.meter.SyncInt64().Histogram(def.metricName.String(), opt...)
		if err != nil {
			panic(err)
		}

		hAll.Record(
			ctx,
			d.Nanoseconds(),
			m.taggedString(map[string]string{namespace: namespaceAllValue}, false).labels...,
		)
	}
}

func (m *openTelemetryScope) RecordDistribution(id int, d int) {
	value := int64(d)
	def := m.defs[id]
	opt := make([]instrument.Option, 0, 1)
	if len(def.unit) > 0 {
		opt = append(opt, unitToOptions(def.unit))
	}

	ctx := context.Background()
	h, err := m.meter.SyncInt64().Histogram(def.metricName.String(), opt...)
	if err != nil {
		panic(err)
	}
	h.Record(ctx, value, m.labels...)

	switch {
	case !def.metricRollupName.Empty() && (m.rootScope != nil):
		hRollup, err := m.rootScope.meter.SyncInt64().Histogram(def.metricRollupName.String(), opt...)
		if err != nil {
			panic(err)
		}

		hRollup.Record(ctx, value, m.rootScope.labels...)
	case m.isNamespaceTagged:
		hAll, err := m.meter.SyncInt64().Histogram(def.metricName.String(), opt...)
		if err != nil {
			panic(err)
		}

		hAll.Record(
			ctx,
			value,
			m.taggedString(map[string]string{namespace: namespaceAllValue}, false).labels...,
		)
	}
}

func (m *openTelemetryScope) taggedString(tags map[string]string, selfAsRoot bool) *openTelemetryScope {
	namespaceTagged := m.isNamespaceTagged
	tagMap := make(map[string]string, len(tags)+len(m.labels))
	for k, v := range m.tags {
		tagMap[k] = v
	}

	for k, v := range tags {
		if m.namespaceTagged(k, v) {
			namespaceTagged = true
		}
		tagMap[k] = v
	}
	return newOpenTelemetryScope(m.serviceIdx, m.meter, m.rootScope, tagMap, m.defs, namespaceTagged, m.gaugeCache, selfAsRoot)
}

func (m *openTelemetryScope) Tagged(tags ...Tag) Scope {
	return m.TaggedInternal(tags...)
}

func (m *openTelemetryScope) namespaceTagged(key string, value string) bool {
	return key == namespace && value != namespaceAllValue
}

func (m *openTelemetryScope) AddCounterInternal(name string, delta int64) {
	panic("should not be used")
}

func (m *openTelemetryScope) StartTimerInternal(timer string) Stopwatch {
	panic("should not be used")
}

func (m *openTelemetryScope) RecordTimerInternal(timer string, d time.Duration) {
	panic("should not be used")
}

func (m *openTelemetryScope) RecordDistributionInternal(id string, unit MetricUnit, d int) {
	panic("should not be used")
}

func (m *openTelemetryScope) TaggedInternal(tags ...Tag) internalScope {
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		tagMap[tag.Key()] = tag.Value()
	}

	return m.taggedString(tagMap, false)
}

func unitToOptions(unit MetricUnit) instrument.Option {
	return instrument.WithUnit(otelunit.Unit(unit))
}
