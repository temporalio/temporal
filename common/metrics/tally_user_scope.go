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

type tallyUserScope struct {
	perUnitBuckets map[MetricUnit]tally.Buckets
	scope          tally.Scope
}

func newTallyUserScope(clientConfig *ClientConfig, scope tally.Scope) UserScope {
	perUnitBuckets := make(map[MetricUnit]tally.Buckets)
	for unit, boundariesList := range clientConfig.PerUnitHistogramBoundaries {
		perUnitBuckets[MetricUnit(unit)] = tally.ValueBuckets(boundariesList)
	}

	return &tallyUserScope{scope: scope, perUnitBuckets: perUnitBuckets}
}

func newTallyUserScopeWithUnitBuckets(perUnitBuckets map[MetricUnit]tally.Buckets, scope tally.Scope) UserScope {
	return &tallyUserScope{scope: scope, perUnitBuckets: perUnitBuckets}
}

func (t tallyUserScope) IncCounter(counter string) {
	t.AddCounter(counter, 1)
}

func (t tallyUserScope) AddCounter(counter string, delta int64) {
	t.scope.Counter(counter).Inc(delta)
}

func (t tallyUserScope) StartTimer(timer string) Stopwatch {
	tm := t.scope.Timer(timer)
	return NewStopwatch(tm)
}

func (t tallyUserScope) RecordTimer(timer string, d time.Duration) {
	t.scope.Timer(timer).Record(d)
}

func (t tallyUserScope) RecordDistribution(id string, unit MetricUnit, d int) {
	buckets, _ := t.perUnitBuckets[unit]
	t.scope.Histogram(id, buckets).RecordValue(float64(d))
}

func (t tallyUserScope) UpdateGauge(gauge string, value float64) {
	t.scope.Gauge(gauge).Update(value)
}

func (t tallyUserScope) Tagged(tags map[string]string) UserScope {
	return newTallyUserScopeWithUnitBuckets(t.perUnitBuckets, t.scope.Tagged(tags))
}
