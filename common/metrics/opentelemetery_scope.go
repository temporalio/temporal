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

	"go.opentelemetry.io/otel/label"
	"go.opentelemetry.io/otel/metric"
)

type (
	openTelemetryScope struct {
		serviceIdx        ServiceIdx
		reporter          *OpenTelemetryReporter
		labels            []label.KeyValue
		rootScope         *openTelemetryScope
		defs              map[int]metricDefinition
		isNamespaceTagged bool
	}

	openTelemetryStopwatch struct {
		start   time.Time
		metrics []openTelemetryStopwatchMetric
	}

	openTelemetryStopwatchMetric struct {
		timer  metric.Float64ValueRecorder
		labels []label.KeyValue
	}
)

func NewOpenTelemetryScope(
	serviceIdx ServiceIdx,
	reporter *OpenTelemetryReporter,
	rootScope *openTelemetryScope,
	tags map[string]string,
	defs map[int]metricDefinition,
	isNamespace bool,
) *openTelemetryScope {
	result := &openTelemetryScope{
		serviceIdx:        serviceIdx,
		reporter:          reporter,
		rootScope:         rootScope,
		defs:              defs,
		isNamespaceTagged: isNamespace,
	}
	result.labels = result.tagMapToLabelArray(tags)
	return result
}

func (m *openTelemetryScope) tagMapToLabelArray(tags map[string]string) []label.KeyValue {
	// todomigryz: this might require predefined sorting if OT doesn't handle it for us.
	result := make([]label.KeyValue, len(tags))
	idx := 0
	for k, v := range tags {
		result[idx] = label.String(k, v)
		idx++
	}
	return result
}

func (m *openTelemetryScope) IncCounter(id int) {
	m.AddCounter(id, 1)
}

func (m *openTelemetryScope) AddCounter(id int, delta int64) {
	def := m.defs[id]
	ctx := context.Background()
	m.reporter.GetMeterMust().NewInt64Counter(def.metricName.String()).Add(ctx, delta, m.labels...)

	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.rootScope.reporter.GetMeterMust().NewInt64Counter(def.metricRollupName.String()).Add(
			ctx, delta, m.rootScope.labels...,
		)
	}
}

func (m *openTelemetryScope) UpdateGauge(id int, value float64) {
	def := m.defs[id]
	ctx := context.Background()
	m.reporter.GetMeterMust().NewFloat64ValueRecorder(def.metricName.String()).Record(ctx, value, m.labels...)

	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.rootScope.reporter.GetMeterMust().NewFloat64ValueRecorder(def.metricRollupName.String()).Record(
			ctx, value, m.rootScope.labels...,
		)
	}
}

func (m *openTelemetryScope) StartTimer(id int) Stopwatch {
	def := m.defs[id]

	return NewStopwatch()
	timer := openTelemetryStopwatchMetric{
		timer:  m.reporter.GetMeterMust().NewFloat64ValueRecorder(def.metricName.String()),
		labels: m.labels,
	}
	switch {
	case !def.metricRollupName.Empty():
		timerRollup := openTelemetryStopwatchMetric{
			timer:  m.rootScope.reporter.GetMeterMust().NewFloat64ValueRecorder(def.metricName.String()),
			labels: m.rootScope.labels,
		}
		return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{timer, timerRollup})
	case m.isNamespaceTagged:
		allScope := m.taggedString(map[string]string{namespace: namespaceAllValue})
		timerAll := openTelemetryStopwatchMetric{
			timer:  allScope.reporter.GetMeterMust().NewFloat64ValueRecorder(def.metricName.String()),
			labels: allScope.labels,
		}
		return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{timer, timerAll})
	default:
		return newOpenTelemetryStopwatch([]openTelemetryStopwatchMetric{timer})
	}
}

func (m *openTelemetryScope) RecordTimer(id int, d time.Duration) {
	def := m.defs[id]
	ctx := context.Background()
	m.reporter.GetMeterMust().NewInt64ValueRecorder(def.metricName.String()).Record(ctx, d.Nanoseconds(), m.labels...)

	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.rootScope.reporter.GetMeterMust().NewInt64ValueRecorder(def.metricRollupName.String()).Record(
			ctx, d.Nanoseconds(), m.rootScope.labels...,
		)
	}

	switch {
	case !def.metricRollupName.Empty() && (m.rootScope != nil):
		m.rootScope.reporter.GetMeterMust().NewInt64ValueRecorder(def.metricRollupName.String()).Record(
			ctx, d.Nanoseconds(), m.rootScope.labels...,
		)
	case m.isNamespaceTagged:
		m.reporter.GetMeterMust().NewInt64ValueRecorder(def.metricName.String()).Record(
			ctx,
			d.Nanoseconds(),
			m.taggedString(map[string]string{namespace: namespaceAllValue}).labels...,
		)
	}
}

func (m *openTelemetryScope) RecordDistribution(id int, d int) {
	// todomigryz: We report distribution as timer in tally case. Need to verify value changes.
	//  check defaultHistogramBuckets.
	value := int64(d)
	def := m.defs[id]

	ctx := context.Background()
	m.reporter.GetMeterMust().NewInt64ValueRecorder(def.metricName.String()).Record(ctx, value, m.labels...)

	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.rootScope.reporter.GetMeterMust().NewInt64ValueRecorder(def.metricRollupName.String()).Record(
			ctx, value, m.rootScope.labels...,
		)
	}

	switch {
	case !def.metricRollupName.Empty() && (m.rootScope != nil):
		m.rootScope.reporter.GetMeterMust().NewInt64ValueRecorder(def.metricRollupName.String()).Record(
			ctx, value, m.rootScope.labels...,
		)
	case m.isNamespaceTagged:
		m.reporter.GetMeterMust().NewInt64ValueRecorder(def.metricName.String()).Record(
			ctx,
			value,
			m.taggedString(map[string]string{namespace: namespaceAllValue}).labels...,
		)
	}
}

func (m *openTelemetryScope) RecordHistogramDuration(id int, value time.Duration) {
	def := m.defs[id]
	ctx := context.Background()
	m.reporter.GetMeterMust().NewInt64ValueRecorder(def.metricName.String()).Record(ctx, value.Nanoseconds(), m.labels...)

	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.rootScope.reporter.GetMeterMust().NewInt64ValueRecorder(def.metricRollupName.String()).Record(
			ctx, value.Nanoseconds(), m.rootScope.labels...,
		)
	}
}

func (m *openTelemetryScope) RecordHistogramValue(id int, value float64) {
	def := m.defs[id]
	ctx := context.Background()
	m.reporter.GetMeterMust().NewFloat64ValueRecorder(def.metricName.String()).Record(ctx, value, m.labels...)

	if !def.metricRollupName.Empty() && (m.rootScope != nil) {
		m.rootScope.reporter.GetMeterMust().NewFloat64ValueRecorder(def.metricRollupName.String()).Record(
			ctx, value, m.rootScope.labels...,
		)
	}
}

func (m *openTelemetryScope) taggedString(tags map[string]string) *openTelemetryScope {
	namespaceTagged := m.isNamespaceTagged
	tagMap := make(map[string]string, len(tags)+len(m.labels))
	for _, lbl := range m.labels {
		tagMap[string(lbl.Key)] = lbl.Value.AsString()
	}

	for k, v := range tags {
		if m.namespaceTagged(k, v) {
			namespaceTagged = true
		}
		tagMap[k] = v
	}
	return NewOpenTelemetryScope(m.serviceIdx, m.reporter, m.rootScope, tagMap, m.defs, namespaceTagged)
}

func (m *openTelemetryScope) Tagged(tags ...Tag) Scope {
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		tagMap[tag.Key()] = tag.Value()
	}

	return m.taggedString(tagMap)
}

func (m *openTelemetryScope) namespaceTagged(key string, value string) bool {
	return key == namespace && value != namespaceAllValue
}

func newOpenTelemetryStopwatch(metricsMeta []openTelemetryStopwatchMetric) openTelemetryStopwatch {
	return openTelemetryStopwatch{time.Now().UTC(), metricsMeta}
}

func (o openTelemetryStopwatch) Stop() {
	panic("implement me")
}
