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

	"golang.org/x/exp/event"
)

// Mostly cribbed from
// https://github.com/temporalio/sdk-go/blob/master/internal/common/metrics/handler.go
// and adapted to depend on golang.org/x/exp/event
type (
	// MetricProvider represents the main dependency for instrumentation
	MetricProvider interface {
		// WithTags creates a new MetricProvder with provided []Tag
		// Tags are merged with registered Tags from the source MetricProvider
		WithTags(...Tag) MetricProvider

		// Counter obtains a counter for the given name.
		Counter(string, ...MetricOption) CounterMetric

		// Gauge obtains a gauge for the given name.
		Gauge(string, ...MetricOption) GaugeMetric

		// Timer obtains a timer for the given name.
		Timer(string, ...MetricOption) TimerMetric

		// Histogram obtains a histogram for the given name.
		Histogram(string, ...MetricOption) HistogramMetric
	}

	// CounterMetric is an ever-increasing counter.
	CounterMetric interface {
		// Record increments the counter value.
		// Tags provided are merged with the source MetricProvider
		Record(int64, ...Tag)
	}
	// GaugeMetric can be set to any float.
	GaugeMetric interface {
		// Record updates the gauge value.
		// Tags provided are merged with the source MetricProvider
		Record(float64, ...Tag)
	}

	// TimerMetric records time durations.
	TimerMetric interface {
		// Record sets the timer value.
		// Tags provided are merged with the source MetricProvider
		Record(time.Duration, ...Tag)
	}

	// HistogramMetric records a distribution of values.
	HistogramMetric interface {
		// Record adds a value to the distribution
		// Tags provided are merged with the source MetricProvider
		Record(int64, ...Tag)
	}

	CounterMetricFunc   func(int64, ...Tag)
	GaugeMetricFunc     func(float64, ...Tag)
	TimerMetricFunc     func(time.Duration, ...Tag)
	HistogramMetricFunc func(int64, ...Tag)

	MetricOption interface {
		apply(*event.MetricOptions)
	}

	MetricOptionFunc func(*event.MetricOptions)
)

// WithMetricUnit provides MetricUnit MetricOption for metric
func WithMetricUnit(unit MetricUnit) MetricOption {
	return MetricOptionFunc(func(mo *event.MetricOptions) {
		mo.Unit = event.Unit(unit)
	})
}

// WithMetricDescription provides description MetricOption for metric
func WithMetricDescription(description string) MetricOption {
	return MetricOptionFunc(func(mo *event.MetricOptions) {
		mo.Description = description
	})
}

// WithMetricNamespace provides namespace MetricOption for metric
func WithMetricNamespace(namespace string) MetricOption {
	return MetricOptionFunc(func(mo *event.MetricOptions) {
		mo.Namespace = namespace
	})
}

func (c CounterMetricFunc) Record(v int64, tags ...Tag)       { c(v, tags...) }
func (c GaugeMetricFunc) Record(v float64, tags ...Tag)       { c(v, tags...) }
func (c TimerMetricFunc) Record(v time.Duration, tags ...Tag) { c(v, tags...) }
func (c HistogramMetricFunc) Record(v int64, tags ...Tag)     { c(v, tags...) }

func (m MetricOptionFunc) apply(mo *event.MetricOptions) { m(mo) }
