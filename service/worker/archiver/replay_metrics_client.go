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

	replayMetricsScope struct {
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

//// NewReplayMetricsScope creates a metrics scope which is aware of temporal's replay mode
//func NewReplayMetricsScope(metricsHandler metrics.MetricsHandler, ctx workflow.Context) metrics.MetricsHandler {
//	return &replayMetricsScope{
//		metricsHandler: metricsHandler,
//		ctx:   ctx,
//	}
//}
//
//// IncCounter increments a counter metric
//func (r *replayMetricsScope) IncCounter(counter int) {
//	if workflow.IsReplaying(r.ctx) {
//		return
//	}
//	r.scope.IncCounter(counter)
//}
//
//// AddCounter adds delta to the counter metric
//func (r *replayMetricsScope) AddCounter(counter int, delta int64) {
//	if workflow.IsReplaying(r.ctx) {
//		return
//	}
//	r.scope.AddCounter(counter, delta)
//}
//
//// StartTimer starts a timer for the given metric name. Time will be recorded when stopwatch is stopped.
//func (r *replayMetricsScope) StartTimer(timer int) metrics.Stopwatch {
//	if workflow.IsReplaying(r.ctx) {
//		return metrics.NoopStopwatch
//	}
//	return r.scope.StartTimer(timer)
//}
//
//// RecordTimer starts a timer for the given metric name
//func (r *replayMetricsScope) RecordTimer(timer int, d time.Duration) {
//	if workflow.IsReplaying(r.ctx) {
//		return
//	}
//	r.scope.RecordTimer(timer, d)
//}
//
//// RecordDistribution records a distribution (wrapper on top of timer) for the given
//// metric name
//func (r *replayMetricsScope) RecordDistribution(timer int, d int) {
//	if workflow.IsReplaying(r.ctx) {
//		return
//	}
//	r.scope.RecordDistribution(timer, d)
//}
//
//// UpdateGauge reports Gauge type absolute value metric
//func (r *replayMetricsScope) UpdateGauge(gauge int, value float64) {
//	if workflow.IsReplaying(r.ctx) {
//		return
//	}
//	r.scope.UpdateGauge(gauge, value)
//}
//
//// Tagged return an internal replay aware scope that can be used to add additional information to metrics
//func (r *replayMetricsScope) Tagged(tags ...metrics.Tag) metrics.Scope {
//	return NewReplayMetricsScope(r.scope.Tagged(tags...), r.ctx)
//}
//
//func (r *replayMetricsScope) WithTags(tag ...metrics.Tag) metrics.MetricsHandler {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (r *replayMetricsScope) Counter(s string) metrics.CounterMetric {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (r *replayMetricsScope) Gauge(s string) metrics.GaugeMetric {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (r *replayMetricsScope) Timer(s string) metrics.TimerMetric {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (r *replayMetricsScope) Histogram(s string, unit metrics.MetricUnit) metrics.HistogramMetric {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (r *replayMetricsScope) Stop(logger log.Logger) {
//	//TODO implement me
//	panic("implement me")
//}
