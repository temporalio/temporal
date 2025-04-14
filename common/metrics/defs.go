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

// types used/defined by the package
type (
	histogramDefinition struct {
		metricDefinition
	}

	counterDefinition struct {
		metricDefinition
	}

	gaugeDefinition struct {
		metricDefinition
	}

	timerDefinition struct {
		metricDefinition
	}
)

func NewTimerDef(name string, opts ...Option) timerDefinition {
	// This line cannot be combined with others!
	// This ensures the stack trace has information of the caller.
	def := newMetricDefinition(name, opts...)
	globalRegistry.register(def)
	return timerDefinition{def}
}

func NewBytesHistogramDef(name string, opts ...Option) histogramDefinition {
	// This line cannot be combined with others!
	// This ensures the stack trace has information of the caller.
	def := newMetricDefinition(name, append(opts, WithUnit(Bytes))...)
	globalRegistry.register(def)
	return histogramDefinition{def}
}

func NewDimensionlessHistogramDef(name string, opts ...Option) histogramDefinition {
	// This line cannot be combined with others!
	// This ensures the stack trace has information of the caller.
	def := newMetricDefinition(name, append(opts, WithUnit(Dimensionless))...)
	globalRegistry.register(def)
	return histogramDefinition{def}
}

func NewCounterDef(name string, opts ...Option) counterDefinition {
	// This line cannot be combined with others!
	// This ensures the stack trace has information of the caller.
	def := newMetricDefinition(name, opts...)
	globalRegistry.register(def)
	return counterDefinition{def}
}

func NewGaugeDef(name string, opts ...Option) gaugeDefinition {
	// This line cannot be combined with others!
	// This ensures the stack trace has information of the caller.
	def := newMetricDefinition(name, opts...)
	globalRegistry.register(def)
	return gaugeDefinition{def}
}

func (d histogramDefinition) With(handler Handler) HistogramIface {
	return handler.Histogram(d.name, d.unit)
}

func (d counterDefinition) With(handler Handler) CounterIface {
	return handler.Counter(d.name)
}

func (d gaugeDefinition) With(handler Handler) GaugeIface {
	return handler.Gauge(d.name)
}

func (d timerDefinition) With(handler Handler) TimerIface {
	return handler.Timer(d.name)
}
