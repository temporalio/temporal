// Copyright (c) 2017 Uber Technologies, Inc.
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

// N.B - this is a temporary Stopwatch struct to allow us to migrate to the
// new style metric names

// Stopwatch is a helper for simpler tracking of elapsed time, use the
// Stop() method to report time elapsed since its created back to the
// timer or histogram.
type Stopwatch struct {
	start time.Time
	scope tally.Scope
	name1 string
	name2 string
}

// NewStopwatch creates a new immutable stopwatch for recording the start
// time to a stopwatch reporter.
func NewStopwatch(scope tally.Scope, name1, name2 string) Stopwatch {
	return Stopwatch{time.Now(), scope, name1, name2}
}

// NewTestStopwatch returns a new test stopwatch
func NewTestStopwatch() Stopwatch {
	return Stopwatch{time.Now(), tally.NoopScope, "timer_1", "timer_2"}
}

// Stop reports time elapsed since the stopwatch start to the recorder.
func (sw Stopwatch) Stop() {
	d := time.Now().Sub(sw.start)
	sw.scope.Timer(sw.name1).Record(d)
	sw.scope.Timer(sw.name2).Record(d)
}

// ClientImpl is used for reporting metrics by various Cadence services
type ClientImpl struct {
	//parentReporter is the parent scope for the metrics
	parentScope tally.Scope
	childScopes map[int]tally.Scope
	metricDefs  map[int]metricDefinition
	serviceIdx  ServiceIdx
}

// NewClient creates and returns a new instance of
// Client implementation
// reporter holds the common tags for the service
// serviceIdx indicates the service type in (InputhostIndex, ... StorageIndex)
func NewClient(scope tally.Scope, serviceIdx ServiceIdx) Client {
	totalScopes := len(ScopeDefs[Common]) + len(ScopeDefs[serviceIdx])
	metricsClient := &ClientImpl{
		parentScope: scope,
		childScopes: make(map[int]tally.Scope, totalScopes),
		metricDefs:  getMetricDefs(serviceIdx),
		serviceIdx:  serviceIdx,
	}

	for idx, def := range ScopeDefs[Common] {
		scopeTags := map[string]string{
			OperationTagName: def.operation,
		}
		mergeMapToRight(def.tags, scopeTags)
		metricsClient.childScopes[idx] = scope.Tagged(scopeTags)
	}

	for idx, def := range ScopeDefs[serviceIdx] {
		scopeTags := map[string]string{
			OperationTagName: def.operation,
		}
		mergeMapToRight(def.tags, scopeTags)
		metricsClient.childScopes[idx] = scope.Tagged(scopeTags)
	}

	return metricsClient
}

// IncCounter increments one for a counter and emits
// to metrics backend
func (m *ClientImpl) IncCounter(scopeIdx int, counterIdx int) {
	name := string(m.metricDefs[counterIdx].metricName)
	oldName := string(m.metricDefs[counterIdx].oldMetricName)
	m.childScopes[scopeIdx].Counter(name).Inc(1)
	m.childScopes[scopeIdx].Counter(oldName).Inc(1)
}

// AddCounter adds delta to the counter and
// emits to the metrics backend
func (m *ClientImpl) AddCounter(scopeIdx int, counterIdx int, delta int64) {
	name := string(m.metricDefs[counterIdx].metricName)
	oldName := string(m.metricDefs[counterIdx].oldMetricName)
	m.childScopes[scopeIdx].Counter(name).Inc(delta)
	m.childScopes[scopeIdx].Counter(oldName).Inc(delta)
}

// StartTimer starts a timer for the given
// metric name
func (m *ClientImpl) StartTimer(scopeIdx int, timerIdx int) Stopwatch {
	name := string(m.metricDefs[timerIdx].metricName)
	oldName := string(m.metricDefs[timerIdx].oldMetricName)
	return NewStopwatch(m.childScopes[scopeIdx], name, oldName)
}

// RecordTimer record and emit a timer for the given
// metric name
func (m *ClientImpl) RecordTimer(scopeIdx int, timerIdx int, d time.Duration) {
	name := string(m.metricDefs[timerIdx].metricName)
	oldName := string(m.metricDefs[timerIdx].oldMetricName)
	m.childScopes[scopeIdx].Timer(name).Record(d)
	m.childScopes[scopeIdx].Timer(oldName).Record(d)
}

// UpdateGauge reports Gauge type metric
func (m *ClientImpl) UpdateGauge(scopeIdx int, gaugeIdx int, value float64) {
	name := string(m.metricDefs[gaugeIdx].metricName)
	oldName := string(m.metricDefs[gaugeIdx].oldMetricName)
	m.childScopes[scopeIdx].Gauge(name).Update(value)
	m.childScopes[scopeIdx].Gauge(oldName).Update(value)
}

// Scope return a new internal metrics scope that can be used to add additional
// information to the metrics emitted
func (m *ClientImpl) Scope(scopeIdx int, tags ...Tag) Scope {
	return newMetricsScope(m.childScopes[scopeIdx], m.metricDefs).Tagged(tags...)
}

func getMetricDefs(serviceIdx ServiceIdx) map[int]metricDefinition {
	defs := make(map[int]metricDefinition)
	for idx, def := range MetricDefs[Common] {
		defs[idx] = def
	}

	for idx, def := range MetricDefs[serviceIdx] {
		defs[idx] = def
	}

	return defs
}

func mergeMapToRight(src map[string]string, dest map[string]string) {
	for k, v := range src {
		dest[k] = v
	}
}
