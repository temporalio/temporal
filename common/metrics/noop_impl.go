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
	NoopMetricsHandler MetricsHandler = newNoopMetricsHandler()
)

type (
	noopMetricsHandler struct{}
)

func newNoopMetricsHandler() *noopMetricsHandler { return &noopMetricsHandler{} }

// WithTags creates a new MetricProvder with provided []Tag
// Tags are merged with registered Tags from the source MetricsHandler
func (n *noopMetricsHandler) WithTags(...Tag) MetricsHandler {
	return n
}

// Counter obtains a counter for the given name and MetricOptions.
func (*noopMetricsHandler) Counter(string) CounterMetric {
	return NoopCounterMetricFunc
}

// Gauge obtains a gauge for the given name and MetricOptions.
func (*noopMetricsHandler) Gauge(string) GaugeMetric {
	return NoopGaugeMetricFunc
}

// Timer obtains a timer for the given name and MetricOptions.
func (*noopMetricsHandler) Timer(string) TimerMetric {
	return NoopTimerMetricFunc
}

// Histogram obtains a histogram for the given name and MetricOptions.
func (*noopMetricsHandler) Histogram(string, MetricUnit) HistogramMetric {
	return NoopHistogramMetricFunc
}

func (*noopMetricsHandler) Stop(log.Logger) {}

var NoopCounterMetricFunc = CounterMetricFunc(func(i int64, t ...Tag) {})
var NoopGaugeMetricFunc = GaugeMetricFunc(func(f float64, t ...Tag) {})
var NoopTimerMetricFunc = TimerMetricFunc(func(d time.Duration, t ...Tag) {})
var NoopHistogramMetricFunc = HistogramMetricFunc(func(i int64, t ...Tag) {})
