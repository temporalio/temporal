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

package archiver

import (
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"

	"go.temporal.io/server/common/metrics"
)

type (
	replayMetricsHandler struct {
		metricsHandler metrics.Handler
		ctx            workflow.Context
	}
)

// NewReplayMetricsHandler creates a metrics handler which is aware of temporal's replay mode
func NewReplayMetricsHandler(metricsHandler metrics.Handler, ctx workflow.Context) metrics.Handler {
	return &replayMetricsHandler{
		metricsHandler: metricsHandler,
		ctx:            ctx,
	}
}

func (r *replayMetricsHandler) WithTags(tag ...metrics.Tag) metrics.Handler {
	return NewReplayMetricsHandler(r.metricsHandler.WithTags(tag...), r.ctx)
}

func (r *replayMetricsHandler) Counter(metricsName string) metrics.CounterMetric {
	if workflow.IsReplaying(r.ctx) {
		return metrics.NoopMetricsCounter
	}
	return r.metricsHandler.Counter(metricsName)
}

func (r *replayMetricsHandler) Gauge(metricsName string) metrics.GaugeMetric {
	if workflow.IsReplaying(r.ctx) {
		return metrics.NoopMetricsGauge
	}
	return r.metricsHandler.Gauge(metricsName)
}

func (r *replayMetricsHandler) Timer(metricsName string) metrics.TimerMetric {
	if workflow.IsReplaying(r.ctx) {
		return metrics.NoopMetricsTimer
	}
	return r.metricsHandler.Timer(metricsName)
}

func (r *replayMetricsHandler) Histogram(metricsName string, unit metrics.MetricUnit) metrics.HistogramMetric {
	if workflow.IsReplaying(r.ctx) {
		return metrics.NoopHistogram
	}
	return r.metricsHandler.Histogram(metricsName, unit)
}

func (r *replayMetricsHandler) Stop(logger log.Logger) {
	r.metricsHandler.Stop(logger)
}
