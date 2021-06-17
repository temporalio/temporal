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

	"github.com/uber-go/tally"
)

type tallyScope struct {
	scope             tally.Scope
	rootScope         tally.Scope
	defs              map[int]metricDefinition
	isNamespaceTagged bool
}

func newTallyScope(
	rootScope tally.Scope,
	scope tally.Scope,
	defs map[int]metricDefinition,
	isNamespace bool,
) Scope {
	return &tallyScope{
		scope:             scope,
		rootScope:         rootScope,
		defs:              defs,
		isNamespaceTagged: isNamespace,
	}
}

// NoopScope returns a noop scope of metrics
func NoopScope(serviceIdx ServiceIdx) Scope {
	return &tallyScope{
		scope:     tally.NoopScope,
		rootScope: tally.NoopScope,
		defs:      getMetricDefs(serviceIdx),
	}
}

func (m *tallyScope) IncCounter(id int) {
	m.AddCounter(id, 1)
}

func (m *tallyScope) AddCounter(id int, delta int64) {
	def := m.defs[id]
	m.scope.Counter(def.metricName.String()).Inc(delta)
	if !def.metricRollupName.Empty() {
		m.rootScope.Counter(def.metricRollupName.String()).Inc(delta)
	}
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
	timer := m.scope.Timer(def.metricName.String())
	switch {
	case !def.metricRollupName.Empty():
		return NewStopwatch(timer, m.rootScope.Timer(def.metricRollupName.String()))
	case m.isNamespaceTagged:
		// remove next line in v1.20.0
		timerAllDeprecated := m.scope.Tagged(map[string]string{namespace: namespaceAllValue}).Timer(def.metricName.String())
		timerAll := m.rootScope.Timer(def.metricName.String() + totalMetricSuffix)
		return NewStopwatch(timer, timerAll, timerAllDeprecated)
	default:
		return NewStopwatch(timer)
	}
}

func (m *tallyScope) RecordTimer(id int, d time.Duration) {
	def := m.defs[id]
	m.scope.Timer(def.metricName.String()).Record(d)
	switch {
	case !def.metricRollupName.Empty():
		m.rootScope.Timer(def.metricRollupName.String()).Record(d)
	case m.isNamespaceTagged:
		// remove next line in v1.20.0
		m.scope.Tagged(map[string]string{namespace: namespaceAllValue}).Timer(def.metricName.String()).Record(d)
		m.rootScope.Timer(def.metricName.String() + totalMetricSuffix).Record(d)
	}
}

func (m *tallyScope) RecordDistribution(id int, d int) {
	dist := time.Duration(d * distributionToTimerRatio)
	def := m.defs[id]
	m.scope.Timer(def.metricName.String()).Record(dist)
	switch {
	case !def.metricRollupName.Empty():
		m.rootScope.Timer(def.metricRollupName.String()).Record(dist)
	case m.isNamespaceTagged:
		// remove next line in v1.20.0
		m.scope.Tagged(map[string]string{namespace: namespaceAllValue}).Timer(def.metricName.String()).Record(dist)
		m.rootScope.Timer(def.metricName.String() + totalMetricSuffix).Record(dist)
	}
}

// Deprecated: not used
func (m *tallyScope) RecordHistogramDuration(id int, value time.Duration) {
	def := m.defs[id]
	m.scope.Histogram(def.metricName.String(), m.getBuckets(id)).RecordDuration(value)
	if !def.metricRollupName.Empty() {
		m.rootScope.Histogram(def.metricRollupName.String(), m.getBuckets(id)).RecordDuration(value)
	}
}

// Deprecated: not used
func (m *tallyScope) RecordHistogramValue(id int, value float64) {
	def := m.defs[id]
	m.scope.Histogram(def.metricName.String(), m.getBuckets(id)).RecordValue(value)
	if !def.metricRollupName.Empty() {
		m.rootScope.Histogram(def.metricRollupName.String(), m.getBuckets(id)).RecordValue(value)
	}
}

func (m *tallyScope) Tagged(tags ...Tag) Scope {
	namespaceTagged := m.isNamespaceTagged
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		if isNamespaceTagged(tag) {
			namespaceTagged = true
		}
		tagMap[tag.Key()] = tag.Value()
	}
	return newTallyScope(m.rootScope, m.scope.Tagged(tagMap), m.defs, namespaceTagged)
}

// todo: Add root service definition in metrics and remove after. See use in server.go.
// Method used to allow reporting metrics outside of services and initializing SDK client.
// Should be generalized with OpenTelemetry after SDK supports OpenTelemetry.
func (m *tallyScope) GetScope() tally.Scope {
	return m.scope
}

func (m *tallyScope) getBuckets(id int) tally.Buckets {
	if m.defs[id].buckets != nil {
		return m.defs[id].buckets
	}
	return tally.DefaultBuckets
}

func isNamespaceTagged(tag Tag) bool {
	return tag.Key() == namespace && tag.Value() != namespaceAllValue
}
