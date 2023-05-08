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
	// MetricName is the name of the metric
	MetricName string

	// MetricType is the type of the metric
	MetricType int

	MetricUnit string

	// metricDefinition contains the definition for a metric
	metricDefinition struct {
		metricType MetricType // metric type
		metricName MetricName // metric name
		unit       MetricUnit
		opts       []Option
	}

	// ServiceIdx is an index that uniquely identifies the service
	ServiceIdx int
)

// MetricUnit supported values
// Values are pulled from https://pkg.go.dev/golang.org/x/exp/event#Unit
const (
	Dimensionless = "1"
	Milliseconds  = "ms"
	Bytes         = "By"
)

// MetricTypes which are supported
const (
	Counter MetricType = iota
	Timer
	Gauge
	Histogram
)

// Empty returns true if the metricName is an empty string
func (mn MetricName) Empty() bool {
	return mn == ""
}

// String returns string representation of this metric name
func (mn MetricName) String() string {
	return string(mn)
}

func (md metricDefinition) GetMetricType() MetricType {
	return md.metricType
}

func (md metricDefinition) GetMetricName() string {
	return md.metricName.String()
}

func (md metricDefinition) GetMetricUnit() MetricUnit {
	return md.unit
}

func (md metricDefinition) GetOptions() []Option {
	return md.opts
}

func NewTimerDef(name string, opts ...Option) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Timer, unit: Milliseconds, opts: opts}
}

func NewBytesHistogramDef(name string, opts ...Option) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Histogram, unit: Bytes, opts: opts}
}

func NewDimensionlessHistogramDef(name string, opts ...Option) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Histogram, unit: Dimensionless, opts: opts}
}

func NewCounterDef(name string, opts ...Option) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Counter, opts: opts}
}

func NewGaugeDef(name string, opts ...Option) metricDefinition {
	return metricDefinition{metricName: MetricName(name), metricType: Gauge, opts: opts}
}
