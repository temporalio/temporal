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
	NoopMetricsClient    struct{}
	NoopMetricsScope     struct{}
	NoopMetricsUserScope struct{}
)

func NewNoopMetricsUserScope() *NoopMetricsUserScope {
	return &NoopMetricsUserScope{}
}

func (n NoopMetricsUserScope) IncCounter(counter string) {}

func (n NoopMetricsUserScope) AddCounter(counter string, delta int64) {}

func (n NoopMetricsUserScope) StartTimer(timer string) Stopwatch {
	return NopStopwatch()
}

func (n NoopMetricsUserScope) RecordTimer(timer string, d time.Duration) {}

func (n NoopMetricsUserScope) RecordDistribution(id string, d int) {}

func (n NoopMetricsUserScope) UpdateGauge(gauge string, value float64) {}

func (n NoopMetricsUserScope) Tagged(tags map[string]string) UserScope {
	return n
}

func NewNoopMetricsScope() *NoopMetricsScope {
	return &NoopMetricsScope{}
}

func (n NoopMetricsScope) IncCounter(counter int) {}

func (n NoopMetricsScope) AddCounter(counter int, delta int64) {}

func (n NoopMetricsScope) StartTimer(timer int) Stopwatch {
	return NopStopwatch()
}

func (n NoopMetricsScope) RecordTimer(timer int, d time.Duration) {}

func (n NoopMetricsScope) RecordDistribution(id int, d int) {}

func (n NoopMetricsScope) UpdateGauge(gauge int, value float64) {}

func (n NoopMetricsScope) Tagged(tags ...Tag) Scope {
	return n
}

func NewNoopMetricsClient() *NoopMetricsClient {
	return &NoopMetricsClient{}
}

func (m NoopMetricsClient) IncCounter(scope int, counter int) {}

func (m NoopMetricsClient) AddCounter(scope int, counter int, delta int64) {}

func (m NoopMetricsClient) StartTimer(scope int, timer int) Stopwatch {
	return NopStopwatch()
}

func (m NoopMetricsClient) RecordTimer(scope int, timer int, d time.Duration) {}

func (m NoopMetricsClient) RecordDistribution(scope int, timer int, d int) {}

func (m NoopMetricsClient) UpdateGauge(scope int, gauge int, value float64) {}

func (m NoopMetricsClient) Scope(scope int, tags ...Tag) Scope {
	return NewNoopMetricsScope()
}

func (m NoopMetricsClient) UserScope() UserScope {
	return NewNoopMetricsUserScope()
}
