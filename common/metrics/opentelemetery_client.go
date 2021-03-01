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

// openTelemetryClient is used for reporting metrics by various Temporal services
type (
	openTelemetryClient struct {
		//parentReporter is the parent scope for the metrics
		rootScope   Scope
		childScopes map[int]Scope
		metricDefs  map[int]metricDefinition
		serviceIdx  ServiceIdx
	}
)

// NewOpentelemeteryClientByReporter creates and returns a new instance of Client implementation
// serviceIdx indicates the service type in (InputhostIndex, ... StorageIndex)
func NewOpentelemeteryClientByReporter(cfg *Metrics, serviceIdx ServiceIdx, reporter *OpenTelemetryReporter, logger log.Logger) (Client, error) {
	rootScope := NewOpenTelemetryScope(serviceIdx, reporter, nil, cfg.Tags, getMetricDefs(serviceIdx), false)

	totalScopes := len(ScopeDefs[Common]) + len(ScopeDefs[serviceIdx])
	metricsClient := &openTelemetryClient{
		rootScope:   rootScope,
		childScopes: make(map[int]Scope, totalScopes),
		metricDefs:  getMetricDefs(serviceIdx),
		serviceIdx:  serviceIdx,
	}

	for idx, def := range ScopeDefs[Common] {
		scopeTags := map[string]string{
			OperationTagName: def.operation,
			namespace:        namespaceAllValue,
		}
		mergeMapToRight(def.tags, scopeTags)
		metricsClient.childScopes[idx] = rootScope.taggedString(scopeTags)
	}

	for idx, def := range ScopeDefs[serviceIdx] {
		scopeTags := map[string]string{
			OperationTagName: def.operation,
			namespace:        namespaceAllValue,
		}
		mergeMapToRight(def.tags, scopeTags)
		metricsClient.childScopes[idx] = rootScope.taggedString(scopeTags)
	}

	return metricsClient, nil
}

// NewOpentelemeteryClient creates and returns a new instance of Client implementation
// serviceIdx indicates the service type in (InputhostIndex, ... StorageIndex)
func NewOpentelemeteryClient(cfg *Metrics, serviceIdx ServiceIdx, logger log.Logger) (Client, error) {
	// todomigryz: validate support for separator, prefix, sanitizeoptions
	reporter, err := NewOpentelemeteryReporter(cfg, logger)
	if err != nil {
		return nil, err
	}
	return NewOpentelemeteryClientByReporter(cfg, serviceIdx, reporter, logger)
}

// IncCounter increments one for a counter and emits
// to metrics backend
func (m *openTelemetryClient) IncCounter(scopeIdx int, counterIdx int) {
	m.childScopes[scopeIdx].IncCounter(counterIdx)
}

// AddCounter adds delta to the counter and
// emits to the metrics backend
func (m *openTelemetryClient) AddCounter(scopeIdx int, counterIdx int, delta int64) {
	m.childScopes[scopeIdx].AddCounter(counterIdx, delta)
}

// StartTimer starts a timer for the given
// metric name
func (m *openTelemetryClient) StartTimer(scopeIdx int, timerIdx int) Stopwatch {
	return m.childScopes[scopeIdx].StartTimer(timerIdx)
}

// RecordTimer record and emit a timer for the given
// metric name
func (m *openTelemetryClient) RecordTimer(scopeIdx int, timerIdx int, d time.Duration) {
	m.childScopes[scopeIdx].RecordTimer(timerIdx, d)
}

// RecordDistribution record and emit a distribution (wrapper on top of timer) for the given
// metric name
func (m *openTelemetryClient) RecordDistribution(scopeIdx int, timerIdx int, d int) {
	m.childScopes[scopeIdx].RecordDistribution(timerIdx, d)
}

// UpdateGauge reports Gauge type metric
func (m *openTelemetryClient) UpdateGauge(scopeIdx int, gaugeIdx int, value float64) {
	m.childScopes[scopeIdx].UpdateGauge(gaugeIdx, value)
}

// Scope return a new internal metrics scope that can be used to add additional
// information to the metrics emitted
func (m *openTelemetryClient) Scope(scopeIdx int, tags ...Tag) Scope {
	return m.childScopes[scopeIdx].Tagged(tags...)
}
