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
	"time"

	"github.com/uber-go/tally/v4"
)

type tallyScope struct {
	scope             tally.Scope
	rootScope         internalScope
	defs              map[int]metricDefinition
	isNamespaceTagged bool
	perUnitBuckets    map[MetricUnit]tally.Buckets
}

func newTallyScopeInternal(
	rootScope internalScope,
	scope tally.Scope,
	defs map[int]metricDefinition,
	isNamespace bool,
	perUnitBuckets map[MetricUnit]tally.Buckets,
) internalScope {
	if perUnitBuckets == nil {
		perUnitBuckets = make(map[MetricUnit]tally.Buckets)
	}
	return &tallyScope{
		scope:             scope,
		rootScope:         rootScope,
		defs:              defs,
		isNamespaceTagged: isNamespace,
		perUnitBuckets:    perUnitBuckets,
	}
}

func (m *tallyScope) IncCounter(id int) {
	m.AddCounter(id, 1)
}

func (m *tallyScope) AddCounter(id int, delta int64) {
	def := m.defs[id]
	m.scope.Counter(def.metricName.String()).Inc(delta)
	if !def.metricRollupName.Empty() {
		m.rootScope.AddCounterInternal(def.metricRollupName.String(), delta)
	}
}

func (m *tallyScope) AddCounterInternal(name string, delta int64) {
	m.scope.Counter(name).Inc(delta)
}

func (m *tallyScope) UpdateGauge(id int, value float64) {
	def := m.defs[id]
	m.scope.Gauge(def.metricName.String()).Update(value)
	if !def.metricRollupName.Empty() {
		m.scope.Gauge(def.metricRollupName.String()).Update(value)
	}
}

func (m *tallyScope) StartTimer(id int) Stopwatch {
	def := m.defs[id]
	timer := NewStopwatch(m.scope.Timer(def.metricName.String()))
	switch {
	case !def.metricRollupName.Empty():
		return NewCompositeStopwatch(timer, m.rootScope.StartTimerInternal(def.metricRollupName.String()))
	case m.isNamespaceTagged:
		timerAll := m.rootScope.StartTimerInternal(def.metricName.String() + totalMetricSuffix)
		return NewCompositeStopwatch(timer, timerAll)
	default:
		return timer
	}
}

func (m *tallyScope) StartTimerInternal(timer string) Stopwatch {
	return NewStopwatch(m.scope.Timer(timer))
}

func (m *tallyScope) RecordTimer(id int, d time.Duration) {
	def := m.defs[id]
	m.scope.Timer(def.metricName.String()).Record(d)
	switch {
	case !def.metricRollupName.Empty():
		m.rootScope.RecordTimerInternal(def.metricRollupName.String(), d)
	case m.isNamespaceTagged:
		m.rootScope.RecordTimerInternal(def.metricName.String()+totalMetricSuffix, d)
	}
}

func (m *tallyScope) RecordTimerInternal(name string, d time.Duration) {
	m.scope.Timer(name).Record(d)
}

func (m *tallyScope) RecordDistribution(id int, d int) {
	def := m.defs[id]
	m.RecordDistributionInternal(def.metricName.String(), def.unit, d)

	switch {
	case !def.metricRollupName.Empty():
		m.rootScope.RecordDistributionInternal(def.metricRollupName.String(), def.unit, d)
	case m.isNamespaceTagged:
		m.rootScope.RecordDistributionInternal(def.metricName.String()+totalMetricSuffix, def.unit, d)
	}
}

func (m *tallyScope) RecordDistributionInternal(name string, unit MetricUnit, d int) {
	buckets := m.perUnitBuckets[unit]
	// if buckets == nil, Histogram will fall back to default buckets
	m.scope.Histogram(name, buckets).RecordValue(float64(d))
}

func (m *tallyScope) Tagged(tags ...Tag) Scope {
	return m.TaggedInternal(tags...)
}

func (m *tallyScope) TaggedInternal(tags ...Tag) internalScope {
	namespaceTagged := m.isNamespaceTagged
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		if isNamespaceTagged(tag) {
			namespaceTagged = true
		}
		tagMap[tag.Key()] = tag.Value()
	}
	return newTallyScopeInternal(m.rootScope, m.scope.Tagged(tagMap), m.defs, namespaceTagged, m.perUnitBuckets)
}

// todo: Add root service definition in metrics and remove after. See use in server.go.
// Method used to allow reporting metrics outside of services and initializing SDK client.
// Should be generalized with OpenTelemetry after SDK supports OpenTelemetry.
func (m *tallyScope) GetScope() tally.Scope {
	return m.scope
}

func isNamespaceTagged(tag Tag) bool {
	return tag.Key() == namespace && tag.Value() != namespaceAllValue
}
