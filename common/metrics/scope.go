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

type metricsScope struct {
	scope             tally.Scope
	defs              map[int]metricDefinition
	isNamespaceTagged bool
}

func newMetricsScope(scope tally.Scope, defs map[int]metricDefinition, isNamespace bool) Scope {
	return &metricsScope{scope, defs, isNamespace}
}

// NoopScope returns a noop scope of metrics
func NoopScope(serviceIdx ServiceIdx) Scope {
	return &metricsScope{tally.NoopScope, getMetricDefs(serviceIdx), false}
}

func (m *metricsScope) IncCounter(id int) {
	name := string(m.defs[id].metricName)
	m.scope.Counter(name).Inc(1)
}

func (m *metricsScope) AddCounter(id int, delta int64) {
	name := string(m.defs[id].metricName)
	m.scope.Counter(name).Inc(delta)
}

func (m *metricsScope) UpdateGauge(id int, value float64) {
	name := string(m.defs[id].metricName)
	m.scope.Gauge(name).Update(value)
}

func (m *metricsScope) StartTimer(id int) Stopwatch {
	name := string(m.defs[id].metricName)
	timer := m.scope.Timer(name)
	if m.isNamespaceTagged {
		timerAll := m.scope.Tagged(map[string]string{namespace: namespaceAllValue}).Timer(name)
		return NewStopwatch(timer, timerAll)
	}
	return NewStopwatch(timer)
}

func (m *metricsScope) RecordTimer(id int, d time.Duration) {
	name := string(m.defs[id].metricName)
	m.scope.Timer(name).Record(d)
	if m.isNamespaceTagged {
		m.scope.Tagged(map[string]string{namespace: namespaceAllValue}).Timer(name).Record(d)
	}
}

func (m *metricsScope) RecordHistogramDuration(id int, value time.Duration) {
	name := string(m.defs[id].metricName)
	m.scope.Histogram(name, m.getBuckets(id)).RecordDuration(value)
}

func (m *metricsScope) RecordHistogramValue(id int, value float64) {
	name := string(m.defs[id].metricName)
	m.scope.Histogram(name, m.getBuckets(id)).RecordValue(value)
}

func (m *metricsScope) Tagged(tags ...Tag) Scope {
	namespaceTagged := false
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		if isNamespaceTagged(tag) {
			namespaceTagged = true
		}
		tagMap[tag.Key()] = tag.Value()
	}
	return newMetricsScope(m.scope.Tagged(tagMap), m.defs, namespaceTagged)
}

func (m *metricsScope) getBuckets(id int) tally.Buckets {
	if m.defs[id].buckets != nil {
		return m.defs[id].buckets
	}
	return tally.DefaultBuckets
}

func isNamespaceTagged(tag Tag) bool {
	return tag.Key() == namespace && tag.Value() != namespaceAllValue
}
