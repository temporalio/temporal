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

	"golang.org/x/exp/maps"
)

type (
	metricsUserScope struct {
		provider MetricProvider
		tags     map[string]string
	}
)

var _ UserScope = (*metricsUserScope)(nil)

func newMetricsUserScope(provider MetricProvider, tags map[string]string) *metricsUserScope {
	return &metricsUserScope{
		provider: provider,
		tags:     tags,
	}
}

// IncCounter increments a counter metric
func (e *metricsUserScope) IncCounter(counter string) {
	e.AddCounter(counter, 1)
}

// AddCounter adds delta to the counter metric
func (e *metricsUserScope) AddCounter(counter string, delta int64) {
	e.provider.Counter(counter).Record(delta, mapToTags(e.tags)...)
}

// StartTimer starts a timer for the given metric name.
// Time will be recorded when stopwatch is stopped.
func (e *metricsUserScope) StartTimer(timer string) Stopwatch {
	return &metricsStopwatch{
		recordFunc: func(d time.Duration) {
			e.provider.Timer(timer).Record(d, mapToTags(e.tags)...)
		},
		start: time.Now(),
	}
}

// RecordTimer records a timer for the given metric name
func (e *metricsUserScope) RecordTimer(timer string, d time.Duration) {
	e.provider.Timer(timer).Record(d, mapToTags(e.tags)...)
}

// RecordDistribution records a distribution (wrapper on top of timer) for the given
// metric name
func (e *metricsUserScope) RecordDistribution(id string, unit MetricUnit, d int) {
	e.provider.Histogram(id, unit).Record(int64(d), mapToTags(e.tags)...)
}

// UpdateGauge reports Gauge type absolute value metric
func (e *metricsUserScope) UpdateGauge(gauge string, value float64) {
	e.provider.Gauge(gauge).Record(value, mapToTags(e.tags)...)
}

// Tagged returns a new scope with added and/or overriden tags values that can be used
// to provide additional information to metrics
func (e *metricsUserScope) Tagged(tags map[string]string) UserScope {
	if len(tags) == 0 {
		return newMetricsUserScope(e.provider, e.tags)
	}

	if len(e.tags) == 0 {
		return newMetricsUserScope(e.provider, tags)
	}

	m := maps.Clone(e.tags)
	maps.Copy(m, tags)
	return newMetricsUserScope(e.provider, m)
}

func mapToTags(m map[string]string) []Tag {
	if len(m) == 0 {
		return nil
	}

	t := make([]Tag, 0, len(m))
	for _, k := range maps.Keys(m) {
		t = append(t, StringTag(k, m[k]))
	}

	return t
}
