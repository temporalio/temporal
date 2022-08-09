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

// Mostly cribbed from
// https://github.com/temporalio/sdk-go/blob/master/internal/common/metrics/handler.go
// and adapted to depend on golang.org/x/exp/event
type (
	// MetricsHandler represents the main dependency for instrumentation
	MetricsHandler interface {
		// WithTags creates a new MetricProvder with provided []Tag
		// Tags are merged with registered Tags from the source MetricsHandler
		WithTags(...Tag) MetricsHandler

		// Counter obtains a counter for the given name and MetricOptions.
		Counter(string) CounterMetric

		// Gauge obtains a gauge for the given name and MetricOptions.
		Gauge(string) GaugeMetric

		// Timer obtains a timer for the given name and MetricOptions.
		Timer(string) TimerMetric

		// Histogram obtains a histogram for the given name and MetricOptions.
		Histogram(string, MetricUnit) HistogramMetric

		Stop(log.Logger)
	}

	// CounterMetric is an ever-increasing counter.
	CounterMetric interface {
		// Record increments the counter value.
		// Tags provided are merged with the source MetricsHandler
		Record(int64, ...Tag)
	}
	// GaugeMetric can be set to any float and repesents a latest value instrument.
	GaugeMetric interface {
		// Record updates the gauge value.
		// Tags provided are merged with the source MetricsHandler
		Record(float64, ...Tag)
	}

	// TimerMetric records time durations.
	TimerMetric interface {
		// Record sets the timer value.
		// Tags provided are merged with the source MetricsHandler
		Record(time.Duration, ...Tag)
	}

	// HistogramMetric records a distribution of values.
	HistogramMetric interface {
		// Record adds a value to the distribution
		// Tags provided are merged with the source MetricsHandler
		Record(int64, ...Tag)
	}

	CounterMetricFunc   func(int64, ...Tag)
	GaugeMetricFunc     func(float64, ...Tag)
	TimerMetricFunc     func(time.Duration, ...Tag)
	HistogramMetricFunc func(int64, ...Tag)
)

func (c CounterMetricFunc) Record(v int64, tags ...Tag)       { c(v, tags...) }
func (c GaugeMetricFunc) Record(v float64, tags ...Tag)       { c(v, tags...) }
func (c TimerMetricFunc) Record(v time.Duration, tags ...Tag) { c(v, tags...) }
func (c HistogramMetricFunc) Record(v int64, tags ...Tag)     { c(v, tags...) }
