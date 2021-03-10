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
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

import (
	"time"

	"github.com/uber-go/tally"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
)

// ClientImpl is used for reporting metrics by various Temporal services
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
			namespace:        namespaceAllValue,
		}
		mergeMapToRight(def.tags, scopeTags)
		metricsClient.childScopes[idx] = scope.Tagged(scopeTags)
	}

	for idx, def := range ScopeDefs[serviceIdx] {
		scopeTags := map[string]string{
			OperationTagName: def.operation,
			namespace:        namespaceAllValue,
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
	m.childScopes[scopeIdx].Counter(name).Inc(1)
}

// AddCounter adds delta to the counter and
// emits to the metrics backend
func (m *ClientImpl) AddCounter(scopeIdx int, counterIdx int, delta int64) {
	name := string(m.metricDefs[counterIdx].metricName)
	m.childScopes[scopeIdx].Counter(name).Inc(delta)
}

// StartTimer starts a timer for the given
// metric name
func (m *ClientImpl) StartTimer(scopeIdx int, timerIdx int) Stopwatch {
	name := string(m.metricDefs[timerIdx].metricName)
	timer := m.childScopes[scopeIdx].Timer(name)
	return NewStopwatch(timer)
}

// RecordTimer record and emit a timer for the given
// metric name
func (m *ClientImpl) RecordTimer(scopeIdx int, timerIdx int, d time.Duration) {
	name := string(m.metricDefs[timerIdx].metricName)
	m.childScopes[scopeIdx].Timer(name).Record(d)
}

// RecordDistribution record and emit a distribution (wrapper on top of timer) for the given
// metric name
func (m *ClientImpl) RecordDistribution(scopeIdx int, timerIdx int, d int) {
	dist := time.Duration(d * distributionToTimerRatio)
	name := string(m.metricDefs[timerIdx].metricName)
	m.childScopes[scopeIdx].Timer(name).Record(dist)
}

// UpdateGauge reports Gauge type metric
func (m *ClientImpl) UpdateGauge(scopeIdx int, gaugeIdx int, value float64) {
	name := string(m.metricDefs[gaugeIdx].metricName)
	m.childScopes[scopeIdx].Gauge(name).Update(value)
}

// Scope return a new internal metrics scope that can be used to add additional
// information to the metrics emitted
func (m *ClientImpl) Scope(scopeIdx int, tags ...Tag) Scope {
	scope := m.childScopes[scopeIdx]
	return newMetricsScope(scope, scope, m.metricDefs, false).Tagged(tags...)
}

// GetMetricsServiceIdx returns the metrics name
func GetMetricsServiceIdx(serviceName string, logger log.Logger) ServiceIdx {
	switch serviceName {
	case primitives.FrontendService:
		return Frontend
	case primitives.HistoryService:
		return History
	case primitives.MatchingService:
		return Matching
	case primitives.WorkerService:
		return Worker
	default:
		logger.Fatal("Unknown service name for metrics!", tag.Service(serviceName))
	}

	// this should never happen!
	return NumServices
}
