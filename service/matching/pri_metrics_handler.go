// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package matching

import (
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	priMetricHandler struct {
		handler metrics.Handler
	}
	priMetricsTimer struct {
		name    string
		handler metrics.Handler
	}
	priMetricsCounter struct {
		name    string
		handler metrics.Handler
	}
)

// TODO(pri): cleanup; delete this
func newPriMetricsHandler(handler metrics.Handler) priMetricHandler {
	return priMetricHandler{
		handler: handler,
	}
}

func (p priMetricHandler) Stop(logger log.Logger) {
	p.handler.Stop(logger)
}

func (p priMetricHandler) Counter(name string) metrics.CounterIface {
	return &priMetricsCounter{name: name, handler: p.handler}
}
func (p priMetricHandler) Timer(name string) metrics.TimerIface {
	return &priMetricsTimer{name: name, handler: p.handler}
}

func (p priMetricHandler) Gauge(name string) metrics.GaugeIface {
	panic("not implemented")
}

func (p priMetricHandler) WithTags(...metrics.Tag) metrics.Handler {
	panic("not implemented")
}

func (p priMetricHandler) Histogram(string, metrics.MetricUnit) metrics.HistogramIface {
	panic("not implemented")
}

func (p priMetricHandler) StartBatch(string) metrics.BatchHandler {
	panic("not implemented")
}

func (c priMetricsCounter) Record(i int64, tag ...metrics.Tag) {
	c.handler.Counter(c.name).Record(i, tag...)
	c.handler.Counter(withPriPrefix(c.name)).Record(i, tag...)
}

func (t priMetricsTimer) Record(duration time.Duration, tag ...metrics.Tag) {
	t.handler.Timer(t.name).Record(duration, tag...)
	t.handler.Timer(withPriPrefix(t.name)).Record(duration, tag...)
}

func withPriPrefix(name string) string {
	return "pri_" + name
}
