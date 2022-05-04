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

var (
	NoopReporter      Reporter      = newNoopReporter()
	NoopClient        Client        = newNoopClient()
	noopInternalScope internalScope = newNoopScope()
	NoopScope         Scope         = noopInternalScope
	NoopUserScope     UserScope     = newNoopUserScope()
	NoopStopwatch     Stopwatch     = newNoopStopwatch()
)

type (
	noopReporterImpl     struct{}
	noopClientImpl       struct{}
	noopMetricsUserScope struct{}
	noopStopwatchImpl    struct{}
	noopScopeImpl        struct{}
)

func newNoopReporter() *noopReporterImpl {
	return &noopReporterImpl{}
}

func (_ *noopReporterImpl) NewClient(logger log.Logger, serviceIdx ServiceIdx) (Client, error) {
	return NoopClient, nil
}

func (_ *noopReporterImpl) Stop(logger log.Logger) {}

func (_ *noopReporterImpl) UserScope() UserScope {
	return NoopUserScope
}

func newNoopUserScope() *noopMetricsUserScope {
	return &noopMetricsUserScope{}
}

func (n *noopMetricsUserScope) IncCounter(counter string) {}

func (n *noopMetricsUserScope) AddCounter(counter string, delta int64) {}

func (n *noopMetricsUserScope) StartTimer(timer string) Stopwatch {
	return NoopStopwatch
}

func (n *noopMetricsUserScope) RecordTimer(timer string, d time.Duration) {}

func (n *noopMetricsUserScope) RecordDistribution(id string, unit MetricUnit, d int) {}

func (n *noopMetricsUserScope) UpdateGauge(gauge string, value float64) {}

func (n *noopMetricsUserScope) Tagged(tags map[string]string) UserScope {
	return n
}

func newNoopClient() *noopClientImpl {
	return &noopClientImpl{}
}

func (m *noopClientImpl) IncCounter(scope int, counter int) {}

func (m *noopClientImpl) AddCounter(scope int, counter int, delta int64) {}

func (m *noopClientImpl) StartTimer(scope int, timer int) Stopwatch {
	return NoopStopwatch
}

func (m *noopClientImpl) RecordTimer(scope int, timer int, d time.Duration) {}

func (m *noopClientImpl) RecordDistribution(scope int, timer int, d int) {}

func (m *noopClientImpl) UpdateGauge(scope int, gauge int, value float64) {}

func (m *noopClientImpl) Scope(scope int, tags ...Tag) Scope {
	return NoopScope
}

func (m *noopClientImpl) UserScope() UserScope {
	return NoopUserScope
}

// NoopScope returns a noop scope of metrics
func newNoopScope() internalScope {
	return &noopScopeImpl{}
}

func (n *noopScopeImpl) AddCounterInternal(name string, delta int64) {
}

func (n *noopScopeImpl) StartTimerInternal(timer string) Stopwatch {
	return NoopStopwatch
}

func (n *noopScopeImpl) RecordTimerInternal(timer string, d time.Duration) {
}

func (n *noopScopeImpl) RecordDistributionInternal(id string, unit MetricUnit, d int) {
}

func (n *noopScopeImpl) TaggedInternal(tags ...Tag) internalScope {
	return n
}

func (n *noopScopeImpl) IncCounter(counter int) {}

func (n *noopScopeImpl) AddCounter(counter int, delta int64) {}

func (n *noopScopeImpl) StartTimer(timer int) Stopwatch {
	return NoopStopwatch
}

func (n *noopScopeImpl) RecordTimer(timer int, d time.Duration) {}

func (n *noopScopeImpl) RecordDistribution(id int, d int) {}

func (n *noopScopeImpl) UpdateGauge(gauge int, value float64) {}

func (n *noopScopeImpl) Tagged(tags ...Tag) Scope {
	return n
}

// NopStopwatch return a fake tally stop watch
func newNoopStopwatch() *noopStopwatchImpl {
	return &noopStopwatchImpl{}
}

func (n *noopStopwatchImpl) Stop()                       {}
func (n *noopStopwatchImpl) Subtract(nsec time.Duration) {}
