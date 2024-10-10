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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination batchmetrics_mock.go

package metrics

import "time"

// The BatchMetrics interfaces provide a way to emit "wide events" within the codebase. If a wide event
// implementation is not provided we default to the implementation provided below which will maintain
// backwards compatibility by emitting each field as an individual metric. In custom implementations, fields
// can be labeled using the metricDefinition.Name() accessor within the respective With() methods.

type (
	BatchMetricsHandler interface {
		CreateBatch(string, ...Tag) BatchMetric
	}

	BatchMetric interface {
		WithHistogram(histogramDefinition, int64) BatchMetric
		WithTimer(timerDefinition, time.Duration) BatchMetric
		WithCounter(counterDefinition, int64) BatchMetric
		WithGauge(gaugeDefinition, float64) BatchMetric
		Emit()
	}

	BatchHandlerImpl struct {
		metricsHandler Handler
	}
	BatchMetricImpl struct {
		emitter Handler
	}
)

func NewBatchMetricsHandler(metricHandler Handler) BatchHandlerImpl {
	return BatchHandlerImpl{metricHandler}
}

func (bh BatchHandlerImpl) CreateBatch(_ string, tags ...Tag) BatchMetric {
	emitter := bh.metricsHandler
	if len(tags) > 0 {
		emitter = emitter.WithTags(tags...)
	}
	return &BatchMetricImpl{
		emitter: emitter,
	}
}

func (bm *BatchMetricImpl) WithHistogram(def histogramDefinition, value int64) BatchMetric {
	def.With(bm.emitter).Record(value)
	return bm
}

func (bm *BatchMetricImpl) WithTimer(def timerDefinition, value time.Duration) BatchMetric {
	def.With(bm.emitter).Record(value)
	return bm
}

func (bm *BatchMetricImpl) WithCounter(def counterDefinition, value int64) BatchMetric {
	def.With(bm.emitter).Record(value)
	return bm
}

func (bm *BatchMetricImpl) WithGauge(def gaugeDefinition, value float64) BatchMetric {
	def.With(bm.emitter).Record(value)
	return bm
}

// Emit is a no-op in this implementation since we don't actually send wide events.
func (bm *BatchMetricImpl) Emit() {}
