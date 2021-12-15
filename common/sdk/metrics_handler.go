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

package sdk

import (
	"time"

	sdkclient "go.temporal.io/sdk/client"

	"go.temporal.io/server/common/metrics"
)

type (
	MetricsHandler struct {
		scope metrics.UserScope
	}

	metricsCounter struct {
		name  string
		scope metrics.UserScope
	}

	metricsGauge struct {
		name  string
		scope metrics.UserScope
	}

	metricsTimer struct {
		name  string
		scope metrics.UserScope
	}
)

var _ sdkclient.MetricsHandler = &MetricsHandler{}

func NewMetricHandler(scope metrics.UserScope) *MetricsHandler {
	return &MetricsHandler{scope: scope}
}

func (m *MetricsHandler) WithTags(tags map[string]string) sdkclient.MetricsHandler {
	return NewMetricHandler(m.scope.Tagged(tags))
}

func (m *MetricsHandler) Counter(name string) sdkclient.MetricsCounter {
	return &metricsCounter{name: name, scope: m.scope}
}

func (m *MetricsHandler) Gauge(name string) sdkclient.MetricsGauge {
	return &metricsGauge{name: name, scope: m.scope}
}

func (m *MetricsHandler) Timer(name string) sdkclient.MetricsTimer {
	return &metricsTimer{name: name, scope: m.scope}
}

func (m metricsCounter) Inc(i int64) {
	m.scope.AddCounter(m.name, i)
}

func (m metricsGauge) Update(f float64) {
	m.scope.UpdateGauge(m.name, f)
}

func (m metricsTimer) Record(duration time.Duration) {
	m.scope.RecordTimer(m.name, duration)
}
