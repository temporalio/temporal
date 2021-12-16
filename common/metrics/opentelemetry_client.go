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

	"go.temporal.io/server/common/log"
)

// opentelemetryClient is used for reporting metrics by various Temporal services
type (
	opentelemetryClient struct {
		// parentReporter is the parent scope for the metrics
		rootScope    *opentelemetryScope
		childScopes  map[int]Scope
		metricDefs   map[int]metricDefinition
		serviceIdx   ServiceIdx
		scopeWrapper func(impl internalScope) internalScope
		gaugeCache   OtelGaugeCache
		userScope    *opentelemetryUserScope
	}
)

// NewOpentelemeteryClientByReporter creates and returns a new instance of Client implementation
// serviceIdx indicates the service type in (InputhostIndex, ... StorageIndex)
func newOpentelemeteryClient(clientConfig *ClientConfig, serviceIdx ServiceIdx, reporter OpentelemetryReporter, logger log.Logger, gaugeCache OtelGaugeCache) (Client, error) {
	tagsFilterConfig := NewTagFilteringScopeConfig(clientConfig.ExcludeTags)

	scopeWrapper := func(impl internalScope) internalScope {
		return NewTagFilteringScope(tagsFilterConfig, impl)
	}

	userScope := newOpentelemetryUserScope(reporter, clientConfig.Tags, gaugeCache)

	globalRootScope := newOpentelemetryScope(serviceIdx, reporter, nil, clientConfig.Tags, getMetricDefs(serviceIdx), false, gaugeCache, false)

	totalScopes := len(ScopeDefs[Common]) + len(ScopeDefs[serviceIdx])
	metricsClient := &opentelemetryClient{
		rootScope:    globalRootScope,
		childScopes:  make(map[int]Scope, totalScopes),
		metricDefs:   getMetricDefs(serviceIdx),
		serviceIdx:   serviceIdx,
		scopeWrapper: scopeWrapper,
		gaugeCache:   gaugeCache,
		userScope:    userScope,
	}

	for idx, def := range ScopeDefs[Common] {
		scopeTags := map[string]string{
			OperationTagName: def.operation,
			namespace:        namespaceAllValue,
		}
		mergeMapToRight(def.tags, scopeTags)
		metricsClient.childScopes[idx] = scopeWrapper(globalRootScope.taggedString(scopeTags, true))
	}

	for idx, def := range ScopeDefs[serviceIdx] {
		scopeTags := map[string]string{
			OperationTagName: def.operation,
			namespace:        namespaceAllValue,
		}
		mergeMapToRight(def.tags, scopeTags)
		metricsClient.childScopes[idx] = scopeWrapper(globalRootScope.taggedString(scopeTags, true))
	}

	return metricsClient, nil
}

// IncCounter increments one for a counter and emits
// to metrics backend
func (m *opentelemetryClient) IncCounter(scopeIdx int, counterIdx int) {
	m.childScopes[scopeIdx].IncCounter(counterIdx)
}

// AddCounter adds delta to the counter and
// emits to the metrics backend
func (m *opentelemetryClient) AddCounter(scopeIdx int, counterIdx int, delta int64) {
	m.childScopes[scopeIdx].AddCounter(counterIdx, delta)
}

// StartTimer starts a timer for the given
// metric name
func (m *opentelemetryClient) StartTimer(scopeIdx int, timerIdx int) Stopwatch {
	return m.childScopes[scopeIdx].StartTimer(timerIdx)
}

// RecordTimer records and emits a timer for the given metric name
func (m *opentelemetryClient) RecordTimer(scopeIdx int, timerIdx int, d time.Duration) {
	m.childScopes[scopeIdx].RecordTimer(timerIdx, d)
}

// RecordDistribution records and emits a distribution (wrapper on top of timer) for the given
// metric name
func (m *opentelemetryClient) RecordDistribution(scopeIdx int, timerIdx int, d int) {
	m.childScopes[scopeIdx].RecordDistribution(timerIdx, d)
}

// UpdateGauge reports Gauge type metric
func (m *opentelemetryClient) UpdateGauge(scopeIdx int, gaugeIdx int, value float64) {
	m.childScopes[scopeIdx].UpdateGauge(gaugeIdx, value)
}

// Scope returns a new internal metrics scope that can be used to add additional
// information to the metrics emitted
func (m *opentelemetryClient) Scope(scopeIdx int, tags ...Tag) Scope {
	return m.childScopes[scopeIdx].Tagged(tags...)
}

// UserScope returns a new metrics scope that can be used to add additional
// information to the metrics emitted by user code.
func (m *opentelemetryClient) UserScope() UserScope {
	return m.userScope
}
