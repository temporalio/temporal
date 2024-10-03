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
// implementation is not provided we default to the implementations provided below which will maintain
// backwards compatibility by emitting each field as an individual metric. Fields can be labeled using
// the metricDefinition.Name() accessor in the respective With() methods.

type (
	BatchMetricsHandler interface {
		CreateBatch(string, ...Tag) BatchMetric
	}

	BatchMetric interface {
		WithHistogram(histogramDefinition, int64) BatchMetric
		WithTimer(timerDefinition, int64) BatchMetric
		WithCounter(counterDefinition, int64) BatchMetric
		WithGauge(gaugeDefinition, int64) BatchMetric
		Emit()
	}

	BatchHandlerImpl struct {
		metricsHandler Handler
	}
	BatchMetricImpl struct {
		emitter Handler
		tags    []Tag
	}
)

func NewBatchMetricHandler(metricHandler Handler) *BatchHandlerImpl {
	return &BatchHandlerImpl{metricHandler}
}

// CreateBatch will emit a BatchMetric that will emit
func (bh *BatchHandlerImpl) CreateBatch(_ string, tags ...Tag) *BatchMetricImpl {
	return &BatchMetricImpl{
		emitter: bh.metricsHandler.WithTags(tags...),
		tags:    tags,
	}
}

func (bm *BatchMetricImpl) WithHistogram(def histogramDefinition, value int64) *BatchMetricImpl {
	def.With(bm.emitter).Record(value)
	return bm
}

func (bm *BatchMetricImpl) WithTimer(def timerDefinition, value time.Duration) *BatchMetricImpl {
	def.With(bm.emitter).Record(value)
	return bm
}

func (bm *BatchMetricImpl) WithCounter(def counterDefinition, value int64) *BatchMetricImpl {
	def.With(bm.emitter).Record(value)
	return bm
}

func (bm *BatchMetricImpl) WithGauge(def gaugeDefinition, value float64) *BatchMetricImpl {
	def.With(bm.emitter).Record(value)
	return bm
}

// Emit is a no-op since there's no actual wide event being sent in this implementation.
func (bm *BatchMetricImpl) Emit() {}
