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
	"fmt"
	"time"

	"golang.org/x/exp/event"
	"golang.org/x/exp/maps"
)

type (
	eventsScope struct {
		provider   MetricProvider
		serviceIdx ServiceIdx
		scopeId    int
		scopeTags  []Tag
		scopeDef   scopeDefinition
	}

	eventsStopwatch struct {
		recordFunc timerRecordFunc
		start      time.Time
	}

	timerRecordFunc func(time.Duration)
)

var (
	_ Scope     = (*eventsScope)(nil)
	_ Stopwatch = (*eventsStopwatch)(nil)
)

func newEventsScope(provider MetricProvider, idx ServiceIdx, id int) eventsScope {
	def, ok := ScopeDefs[idx][id]
	if !ok {
		def, ok = ScopeDefs[Common][id]
		if !ok {
			panic(fmt.Errorf("failed to lookup scope by id %v and service %v", id, idx))
		}
	}

	return eventsScope{
		provider:   provider,
		serviceIdx: idx,
		scopeId:    id,
		scopeTags:  scopeDefToTags(def),
		scopeDef:   def,
	}
}

// IncCounter increments a counter metric
func (e eventsScope) IncCounter(counter int) {
	e.AddCounter(counter, 1)
}

// AddCounter adds delta to the counter metric
func (e eventsScope) AddCounter(counter int, delta int64) {
	m := getDefinition(e.serviceIdx, counter)

	e.provider.Counter(m.metricName.String(), &MetricOptions{
		Unit: event.Unit(m.unit),
	}).Record(int64(delta), e.scopeTags...)

	if !m.metricRollupName.Empty() {
		e.provider.Counter(m.metricRollupName.String(), &event.MetricOptions{
			Unit: event.Unit(m.unit),
		}).Record(int64(delta), e.scopeTags...)
	}
}

// StartTimer starts a timer for the given metric name.
// Time will be recorded when stopwatch is stopped.
func (e eventsScope) StartTimer(timer int) Stopwatch {
	return &eventsStopwatch{
		recordFunc: func(d time.Duration) {
			m := getDefinition(e.serviceIdx, timer)

			e.provider.Timer(m.metricName.String(), nil).Record(d, e.scopeTags...)

			if !m.metricRollupName.Empty() {
				e.provider.Timer(m.metricRollupName.String(), nil).Record(d, e.scopeTags...)
			}
		},
		start: time.Now(),
	}
}

// RecordTimer records a timer for the given metric name
func (e eventsScope) RecordTimer(timer int, d time.Duration) {
	m := getDefinition(e.serviceIdx, timer)

	e.provider.Timer(m.metricName.String(), &MetricOptions{
		Unit: event.Unit(m.unit),
	}).Record(d, e.scopeTags...)

	if !m.metricRollupName.Empty() {
		e.provider.Timer(m.metricRollupName.String(), &event.MetricOptions{
			Unit: event.Unit(m.unit),
		}).Record(d, e.scopeTags...)
	}
}

// RecordDistribution records a distribution (wrapper on top of timer) for the given
// metric name
func (e eventsScope) RecordDistribution(id int, d int) {
	m := getDefinition(e.serviceIdx, id)

	e.provider.Histogram(m.metricName.String(), &MetricOptions{
		Unit: event.Unit(m.unit),
	}).Record(int64(d), e.scopeTags...)

	if !m.metricRollupName.Empty() {
		e.provider.Histogram(m.metricRollupName.String(), &event.MetricOptions{
			Unit: event.Unit(m.unit),
		}).Record(int64(d), e.scopeTags...)
	}
}

// UpdateGauge reports Gauge type absolute value metric
func (e eventsScope) UpdateGauge(gauge int, value float64) {
	m := getDefinition(e.serviceIdx, gauge)

	e.provider.Gauge(m.metricName.String(), &MetricOptions{
		Unit: event.Unit(m.unit),
	}).Record(value, e.scopeTags...)

	if !m.metricRollupName.Empty() {
		e.provider.Gauge(m.metricRollupName.String(), &event.MetricOptions{
			Unit: event.Unit(m.unit),
		}).Record(value, e.scopeTags...)
	}
}

// Tagged returns an internal scope that can be used to add additional
// information to metrics
func (e eventsScope) Tagged(tags ...Tag) Scope {
	return newEventsScope(e.provider.WithTags(tags...), e.serviceIdx, e.scopeId)
}

func scopeDefToTags(s scopeDefinition) []Tag {
	t := append(make([]Tag, 0, len(s.tags)+1), OperationTag(s.operation))

	for _, k := range maps.Keys(s.tags) {
		t = append(t, StringTag(k, s.tags[k]))
	}

	return t
}

// Stop records time elapsed from time of creation.
func (e eventsStopwatch) Stop() {
	e.recordFunc(time.Since(e.start))
}

// Subtract adds value to subtract from recorded duration.
func (e *eventsStopwatch) Subtract(d time.Duration) {
	e.start = e.start.Add(d)
}

func getDefinition(idx ServiceIdx, id int) metricDefinition {
	m, ok := MetricDefs[idx][id]
	if !ok {
		m, ok = MetricDefs[Common][id]
		if !ok {
			panic(fmt.Errorf("failed to lookup metric by id %v and service %v", id, idx))
		}
	}

	return m
}
