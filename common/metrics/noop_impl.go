//// The MIT License
////
//// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
////
//// Copyright (c) 2020 Uber Technologies, Inc.
////
//// Permission is hereby granted, free of charge, to any person obtaining a copy
//// of this software and associated documentation files (the "Software"), to deal
//// in the Software without restriction, including without limitation the rights
//// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//// copies of the Software, and to permit persons to whom the Software is
//// furnished to do so, subject to the following conditions:
////
//// The above copyright notice and this permission notice shall be included in
//// all copies or substantial portions of the Software.
////
//// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//// THE SOFTWARE.
//
package metrics

import (
	"time"

	"go.temporal.io/server/common/log"
)

//
//import (
//	"time"
//
//	"go.temporal.io/server/common/log"
//)
//
var (
	//NoopClient         Client         = newNoopClient()
	//NoopScope          Scope          = newNoopScope()
	//NoopStopwatch      Stopwatch      = newNoopStopwatch()
	NoopMetricsHandler Handler         = newNoopMetricsHandler()
	NoopMetricsCounter CounterMetric   = CounterMetricFunc(func(i int64, t ...Tag) {})
	NoopMetricsGauge   GaugeMetric     = GaugeMetricFunc(func(f float64, t ...Tag) {})
	NoopMetricsTimer   TimerMetric     = TimerMetricFunc(func(d time.Duration, t ...Tag) {})
	NoopHistogram      HistogramMetric = HistogramMetricFunc(func(i int64, t ...Tag) {})
)

//
type (
	//noopClientImpl     struct{}
	//noopStopwatchImpl  struct{}
	//noopScopeImpl      struct{}
	noopMetricsHandler struct{}
)

//
//func newNoopClient() *noopClientImpl {
//	return &noopClientImpl{}
//}
//
//func (m *noopClientImpl) IncCounter(scope int, counter int) {}
//
//func (m *noopClientImpl) AddCounter(scope int, counter int, delta int64) {}
//
//func (m *noopClientImpl) StartTimer(scope int, timer int) Stopwatch {
//	return NoopStopwatch
//}
//
//func (m *noopClientImpl) RecordTimer(scope int, timer int, d time.Duration) {}
//
//func (m *noopClientImpl) RecordDistribution(scope int, timer int, d int) {}
//
//func (m *noopClientImpl) UpdateGauge(scope int, gauge int, value float64) {}
//
//func (m *noopClientImpl) Scope(scope int, tags ...Tag) Scope {
//	return NoopScope
//}
//
//// NoopScope returns a noop scope of metrics
//func newNoopScope() *noopScopeImpl {
//	return &noopScopeImpl{}
//}
//
//func (n *noopScopeImpl) AddCounterInternal(name string, delta int64) {
//}
//
//func (n *noopScopeImpl) StartTimerInternal(timer string) Stopwatch {
//	return NoopStopwatch
//}
//
//func (n *noopScopeImpl) RecordTimerInternal(timer string, d time.Duration) {
//}
//
//func (n *noopScopeImpl) RecordDistributionInternal(id string, unit MetricUnit, d int) {
//}
//
//func (n *noopScopeImpl) IncCounter(counter int) {}
//
//func (n *noopScopeImpl) AddCounter(counter int, delta int64) {}
//
//func (n *noopScopeImpl) StartTimer(timer int) Stopwatch {
//	return NoopStopwatch
//}
//
//func (n *noopScopeImpl) RecordTimer(timer int, d time.Duration) {}
//
//func (n *noopScopeImpl) RecordDistribution(id int, d int) {}
//
//func (n *noopScopeImpl) UpdateGauge(gauge int, value float64) {}
//
//func (n *noopScopeImpl) Tagged(tags ...Tag) Scope {
//	return n
//}
//
//// NopStopwatch return a fake tally stop watch
//func newNoopStopwatch() *noopStopwatchImpl {
//	return &noopStopwatchImpl{}
//}
//
//func (n *noopStopwatchImpl) Stop()                       {}
//func (n *noopStopwatchImpl) Subtract(nsec time.Duration) {}

func newNoopMetricsHandler() *noopMetricsHandler { return &noopMetricsHandler{} }

// WithTags creates a new metrics handler with provided []Tag
// Tags are merged with registered Tags from the source MetricsHandler
func (n *noopMetricsHandler) WithTags(...Tag) Handler {
	return n
}

// Counter obtains a counter for the given name and MetricOptions.
func (*noopMetricsHandler) Counter(string) CounterMetric {
	return NoopMetricsCounter
}

// Gauge obtains a gauge for the given name and MetricOptions.
func (*noopMetricsHandler) Gauge(string) GaugeMetric {
	return NoopMetricsGauge
}

// Timer obtains a timer for the given name and MetricOptions.
func (*noopMetricsHandler) Timer(string) TimerMetric {
	return NoopMetricsTimer
}

// Histogram obtains a histogram for the given name and MetricOptions.
func (*noopMetricsHandler) Histogram(string, MetricUnit) HistogramMetric {
	return NoopHistogram
}

func (*noopMetricsHandler) Stop(log.Logger) {}
