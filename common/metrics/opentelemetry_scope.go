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
	otelunit "go.opentelemetry.io/otel/metric/unit"
)

type (
	opentelemetryScope struct {
		serviceIdx        ServiceIdx
		reporter          OpentelemetryReporter
		labels            []attribute.KeyValue
		tags              map[string]string
		rootScope         *opentelemetryScope
		defs              map[int]metricDefinition
		isNamespaceTagged bool

		gaugeCache OtelGaugeCache
	}
)

func newOpentelemetryScope(
	serviceIdx ServiceIdx,
	reporter OpentelemetryReporter,
	rootScope *opentelemetryScope,
	tags map[string]string,
	defs map[int]metricDefinition,
	isNamespace bool,
	gaugeCache OtelGaugeCache,
	selfAsRoot bool,
) *opentelemetryScope {
	result := &opentelemetryScope{
		serviceIdx:        serviceIdx,
		reporter:          reporter,
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

func (m *opentelemetryScope) IncCounter(id int) {
	m.AddCounter(id, 1)
}

func (m *opentelemetryScope) AddCounter(id int, delta int64) {
	def := m.defs[id]
	ctx := context.Background()
	m.reporter.GetMeterMust().NewInt64Counter(def.metricName.String()).Add(ctx, delta, m.labels...)

	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.rootScope.reporter.GetMeterMust().NewInt64Counter(def.metricRollupName.String()).Add(
			ctx, delta, m.rootScope.labels...,
		)
	}
}

func (m *opentelemetryScope) UpdateGauge(id int, value float64) {
	def := m.defs[id]
	m.gaugeCache.Set(def.metricName.String(), m.tags, value)
	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.gaugeCache.Set(def.metricRollupName.String(), m.rootScope.tags, value)
	}
}

func (m *opentelemetryScope) StartTimer(id int) Stopwatch {
	def := m.defs[id]
	opt := make([]metric.InstrumentOption, 0, 1)
	if len(def.unit) > 0 {
		opt = append(opt, unitToOptions(def.unit))
	}

	timer := newOpenTelemetryStopwatchMetric(
		m.reporter.GetMeterMust().NewInt64Histogram(def.metricName.String(), opt...),
		m.labels)
	switch {
	case !def.metricRollupName.Empty():
		timerRollup := newOpenTelemetryStopwatchMetric(
			m.rootScope.reporter.GetMeterMust().NewInt64Histogram(def.metricName.String(), opt...),
			m.rootScope.labels)
		return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{timer, timerRollup})
	case m.isNamespaceTagged:
		allScope := m.taggedString(map[string]string{namespace: namespaceAllValue}, false)
		timerAll := newOpenTelemetryStopwatchMetric(
			allScope.reporter.GetMeterMust().NewInt64Histogram(def.metricName.String(), opt...),
			allScope.labels)
		return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{timer, timerAll})
	default:
		return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{timer})
	}
}

func (m *opentelemetryScope) RecordTimer(id int, d time.Duration) {
	def := m.defs[id]
	ctx := context.Background()

	opt := make([]metric.InstrumentOption, 0, 1)
	if len(def.unit) > 0 {
		opt = append(opt, unitToOptions(def.unit))
	}

	m.reporter.GetMeterMust().NewInt64Histogram(def.metricName.String(), opt...).Record(ctx, d.Nanoseconds(), m.labels...)

	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.rootScope.reporter.GetMeterMust().NewInt64Histogram(def.metricRollupName.String(), opt...).Record(
			ctx, d.Nanoseconds(), m.rootScope.labels...,
		)
	}

	switch {
	case !def.metricRollupName.Empty() && (m.rootScope != nil):
		m.rootScope.reporter.GetMeterMust().NewInt64Histogram(def.metricRollupName.String(), opt...).Record(
			ctx, d.Nanoseconds(), m.rootScope.labels...,
		)
	case m.isNamespaceTagged:
		m.reporter.GetMeterMust().NewInt64Histogram(def.metricName.String(), opt...).Record(
			ctx,
			d.Nanoseconds(),
			m.taggedString(map[string]string{namespace: namespaceAllValue}, false).labels...,
		)
	}
}

func (m *opentelemetryScope) RecordDistribution(id int, d int) {
	value := int64(d)
	def := m.defs[id]
	opt := make([]metric.InstrumentOption, 0, 1)
	if len(def.unit) > 0 {
		opt = append(opt, unitToOptions(def.unit))
	}

	ctx := context.Background()
	m.reporter.GetMeterMust().NewInt64Histogram(def.metricName.String(), opt...).Record(ctx, value, m.labels...)

	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.rootScope.reporter.GetMeterMust().NewInt64Histogram(def.metricRollupName.String(), opt...).Record(
			ctx, value, m.rootScope.labels...,
		)
	}

	switch {
	case !def.metricRollupName.Empty() && (m.rootScope != nil):
		m.rootScope.reporter.GetMeterMust().NewInt64Histogram(def.metricRollupName.String(), opt...).Record(
			ctx, value, m.rootScope.labels...,
		)
	case m.isNamespaceTagged:
		m.reporter.GetMeterMust().NewInt64Histogram(def.metricName.String(), opt...).Record(
			ctx,
			value,
			m.taggedString(map[string]string{namespace: namespaceAllValue}, false).labels...,
		)
	}
}

func (m *opentelemetryScope) taggedString(tags map[string]string, selfAsRoot bool) *opentelemetryScope {
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
	return newOpentelemetryScope(m.serviceIdx, m.reporter, m.rootScope, tagMap, m.defs, namespaceTagged, m.gaugeCache, selfAsRoot)
}

func (m *opentelemetryScope) Tagged(tags ...Tag) Scope {
	return m.TaggedInternal(tags...)
}

func (m *opentelemetryScope) namespaceTagged(key string, value string) bool {
	return key == namespace && value != namespaceAllValue
}

func (m *opentelemetryScope) userScope() UserScope {
	return newOpentelemetryUserScope(m.reporter, m.tags, m.gaugeCache)
}

func (m *opentelemetryScope) AddCounterInternal(name string, delta int64) {
	panic("should not be used")
}

func (m *opentelemetryScope) StartTimerInternal(timer string) Stopwatch {
	panic("should not be used")
}

func (m *opentelemetryScope) RecordTimerInternal(timer string, d time.Duration) {
	panic("should not be used")
}

func (m *opentelemetryScope) RecordDistributionInternal(id string, unit MetricUnit, d int) {
	panic("should not be used")
}

func (m *opentelemetryScope) TaggedInternal(tags ...Tag) internalScope {
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		tagMap[tag.Key()] = tag.Value()
	}

	return m.taggedString(tagMap, false)
}

func unitToOptions(unit MetricUnit) metric.InstrumentOption {
	return metric.WithUnit(otelunit.Unit(unit))
}
