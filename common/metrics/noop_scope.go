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
)

type (
	NoopScopeImpl struct{}
)

func (n NoopScopeImpl) AddCounterInternal(name string, delta int64) {
}

func (n NoopScopeImpl) StartTimerInternal(timer string) Stopwatch {
	return nopStopwatch{}
}

func (n NoopScopeImpl) RecordTimerInternal(timer string, d time.Duration) {
}

func (n NoopScopeImpl) RecordDistributionInternal(id string, unit MetricUnit, d int) {
}

func (n NoopScopeImpl) TaggedInternal(tags ...Tag) internalScope {
	return n
}

// NoopScope returns a noop scope of metrics
func NoopScope(serviceIdx ServiceIdx) internalScope {
	return &NoopScopeImpl{}
}

func (n NoopScopeImpl) IncCounter(counter int) {}

func (n NoopScopeImpl) AddCounter(counter int, delta int64) {}

func (n NoopScopeImpl) StartTimer(timer int) Stopwatch {
	return nopStopwatch{}
}

func (n NoopScopeImpl) RecordTimer(timer int, d time.Duration) {}

func (n NoopScopeImpl) RecordDistribution(id int, d int) {}

func (n NoopScopeImpl) UpdateGauge(gauge int, value float64) {}

func (n NoopScopeImpl) Tagged(tags ...Tag) Scope {
	return n
}
