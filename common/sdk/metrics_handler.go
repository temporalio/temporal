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
		provider metrics.MetricsHandler
	}

	metricsCounter struct {
		name     string
		provider metrics.MetricsHandler
	}

	metricsGauge struct {
		name     string
		provider metrics.MetricsHandler
	}

	metricsTimer struct {
		name     string
		provider metrics.MetricsHandler
	}
)

var _ sdkclient.MetricsHandler = &MetricsHandler{}

func NewMetricsHandler(provider metrics.MetricsHandler) *MetricsHandler {
	return &MetricsHandler{provider: provider}
}

func (m *MetricsHandler) WithTags(tags map[string]string) sdkclient.MetricsHandler {
	t := make([]metrics.Tag, 0, len(tags))
	for k, v := range tags {
		t = append(t, metrics.StringTag(k, v))
	}

	return NewMetricsHandler(m.provider.WithTags(t...))
}

func (m *MetricsHandler) Counter(name string) sdkclient.MetricsCounter {
	return &metricsCounter{name: name, provider: m.provider}
}

func (m *MetricsHandler) Gauge(name string) sdkclient.MetricsGauge {
	return &metricsGauge{name: name, provider: m.provider}
}

func (m *MetricsHandler) Timer(name string) sdkclient.MetricsTimer {
	return &metricsTimer{name: name, provider: m.provider}
}

func (m metricsCounter) Inc(i int64) {
	m.provider.Counter(m.name).Record(i)
}

func (m metricsGauge) Update(f float64) {
	m.provider.Gauge(m.name).Record(f)
}

func (m metricsTimer) Record(duration time.Duration) {
	m.provider.Timer(m.name).Record(duration)
}
