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
	"time"

	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	replayMetricsClient struct {
		metricsHandler metrics.MetricsHandler
		ctx            workflow.Context
	}
)

// NewReplayMetricsClient creates a metrics client which is aware of temporal's replay mode
func NewReplayMetricsClient(metricsHandler metrics.MetricsHandler, ctx workflow.Context) metrics.MetricsHandler {
	return &replayMetricsClient{
		metricsHandler: metricsHandler,
		ctx:            ctx,
	}
}

func (r *replayMetricsClient) WithTags(tags ...metrics.Tag) metrics.MetricsHandler {
	if workflow.IsReplaying(r.ctx) {
		return r
	}
	return NewReplayMetricsClient(r.metricsHandler.WithTags(tags...), r.ctx)
}

func (r *replayMetricsClient) Counter(s string) metrics.CounterMetric {
	if workflow.IsReplaying(r.ctx) {
		return metrics.CounterMetricFunc(func(i int64, t ...metrics.Tag) {})
	}
	return r.metricsHandler.Counter(s)
}

func (r *replayMetricsClient) Gauge(s string) metrics.GaugeMetric {
	if workflow.IsReplaying(r.ctx) {
		return metrics.GaugeMetricFunc(func(i float64, t ...metrics.Tag) {})
	}
	return r.metricsHandler.Gauge(s)
}

func (r *replayMetricsClient) Timer(s string) metrics.TimerMetric {
	if workflow.IsReplaying(r.ctx) {
		return metrics.TimerMetricFunc(func(ti time.Duration, t ...metrics.Tag) {})
	}
	return r.metricsHandler.Timer(s)
}

func (r *replayMetricsClient) Histogram(s string, unit metrics.MetricUnit) metrics.HistogramMetric {
	if workflow.IsReplaying(r.ctx) {
		return metrics.HistogramMetricFunc(func(i int64, t ...metrics.Tag) {})
	}
	return r.metricsHandler.Histogram(s, unit)
}

func (r *replayMetricsClient) Stop(logger log.Logger) {
	if workflow.IsReplaying(r.ctx) {
		return
	}
	r.metricsHandler.Stop(logger)
}
