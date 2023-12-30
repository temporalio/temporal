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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination metrics_mock.go

package metrics

import (
	"time"

	"go.temporal.io/server/common/log"
)

// Mostly cribbed from
// https://github.com/temporalio/sdk-go/blob/master/internal/common/metrics/handler.go
// and adapted to depend on golang.org/x/exp/event
type (
	// Handler is a wrapper around a metrics client.
	// If you are interacting with metrics registered with New*Def functions, e.g. NewCounterDef, please use the With
	// method of those definitions instead of calling Counter directly on the Handler. This will ensure that you don't
	// accidentally use the wrong metric type, and you don't need to re-specify metric types or units.
	Handler interface {
		// WithTags creates a new Handler with provided Tag list.
		// Tags are merged with registered Tags from the source Handler.
		WithTags(...Tag) Handler

		// Counter obtains a counter for the given name.
		Counter(string) CounterIface

		// Gauge obtains a gauge for the given name.
		Gauge(string) GaugeIface

		// Timer obtains a timer for the given name.
		Timer(string) TimerIface

		// Histogram obtains a histogram for the given name.
		Histogram(string, MetricUnit) HistogramIface

		Stop(log.Logger)
	}

	// CounterIface is an ever-increasing counter.
	CounterIface interface {
		// Record increments the counter value.
		// Tags provided are merged with the source MetricsHandler
		Record(int64, ...Tag)
	}
	// GaugeIface can be set to any float and repesents a latest value instrument.
	GaugeIface interface {
		// Record updates the gauge value.
		// Tags provided are merged with the source MetricsHandler
		Record(float64, ...Tag)
	}

	// TimerIface records time durations.
	TimerIface interface {
		// Record sets the timer value.
		// Tags provided are merged with the source MetricsHandler
		Record(time.Duration, ...Tag)
	}

	// HistogramIface records a distribution of values.
	HistogramIface interface {
		// Record adds a value to the distribution
		// Tags provided are merged with the source MetricsHandler
		Record(int64, ...Tag)
	}

	CounterFunc   func(int64, ...Tag)
	GaugeFunc     func(float64, ...Tag)
	TimerFunc     func(time.Duration, ...Tag)
	HistogramFunc func(int64, ...Tag)
)

func (c CounterFunc) Record(v int64, tags ...Tag)       { c(v, tags...) }
func (c GaugeFunc) Record(v float64, tags ...Tag)       { c(v, tags...) }
func (c TimerFunc) Record(v time.Duration, tags ...Tag) { c(v, tags...) }
func (c HistogramFunc) Record(v int64, tags ...Tag)     { c(v, tags...) }
