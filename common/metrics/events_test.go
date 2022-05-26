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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/export/aggregation"
	"go.opentelemetry.io/otel/sdk/metric/metrictest"
	"go.opentelemetry.io/otel/sdk/metric/number"
)

type (
	eventsSuite struct {
		suite.Suite
		*require.Assertions
		testexporter  *metrictest.Exporter
		meterProvider metric.MeterProvider
	}
)

func TestEventsSuite(t *testing.T) {
	s := new(eventsSuite)
	suite.Run(t, s)
}

func (s *eventsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.meterProvider, s.testexporter = metrictest.NewTestMeterProvider()
}

func (s *eventsSuite) TestEventMetricProvider_WithTags() {
	tests := []struct {
		name string
		tags []Tag
		want eventMetricProvider
	}{
		{
			"empty tags",
			[]Tag{},
			eventMetricProvider{
				tags: []Tag{},
			},
		},
		{
			"operation tag",
			[]Tag{OperationTag("awesome")},
			eventMetricProvider{
				tags: []Tag{
					&tagImpl{
						key:   "operation",
						value: "awesome",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			emp := NewEventMetricProvider(NewMetricEventExporter(NewOtelMetricHandler(s.meterProvider.Meter("test"))))
			got := emp.WithTags(tt.tags...).(*eventMetricProvider)
			s.Equal(tt.want.tags, got.tags)
		})
	}
}

func (s *eventsSuite) TestCounterMetricFunc_Record() {
	tests := []struct {
		name string
		v    int64
		tags []Tag
		want []metrictest.ExportRecord
	}{
		{
			"test-counter",
			2,
			nil,
			[]metrictest.ExportRecord{
				{
					InstrumentName:         "go.temporal.io/server/common/metrics/test-counter",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.SumKind,
					Sum:                    number.NewInt64Number(2),
				},
			},
		},
		{
			"test-counter2",
			4,
			[]Tag{OperationTag("awesome")},
			[]metrictest.ExportRecord{
				{
					InstrumentName:         "go.temporal.io/server/common/metrics/test-counter2",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.SumKind,
					Sum:                    number.NewInt64Number(4),
					Attributes: []attribute.KeyValue{
						attribute.String("operation", "awesome"),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			emp := NewEventMetricProvider(NewMetricEventExporter(NewOtelMetricHandler(s.meterProvider.Meter("test"))))
			emp.Counter(tt.name).Record(tt.v, tt.tags...)
			s.testexporter.Collect(emp.context)

			s.NotEmpty(s.testexporter.Records)
			s.Equal(tt.want, s.testexporter.Records)
		})
	}
}

func (s *eventsSuite) TestGaugeMetricFunc_Record() {
	tests := []struct {
		name string
		v    float64
		tags []Tag
		want []metrictest.ExportRecord
	}{
		{
			"test-gauge",
			2.0,
			nil,
			[]metrictest.ExportRecord{
				{
					InstrumentName:         "go.temporal.io/server/common/metrics/test-gauge",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.SumKind,
					Sum:                    number.NewFloat64Number(2.0),
					NumberKind:             number.Float64Kind,
				},
			},
		},
		{
			"test-gauge2",
			4.0,
			[]Tag{OperationTag("awesome")},
			[]metrictest.ExportRecord{
				{
					InstrumentName:         "go.temporal.io/server/common/metrics/test-gauge2",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.SumKind,
					Sum:                    number.NewFloat64Number(4.0),
					NumberKind:             number.Float64Kind,
					Attributes: []attribute.KeyValue{
						attribute.String("operation", "awesome"),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			emp := NewEventMetricProvider(NewMetricEventExporter(NewOtelMetricHandler(s.meterProvider.Meter("test"))))
			emp.Gauge(tt.name).Record(tt.v, tt.tags...)
			s.testexporter.Collect(emp.context)

			s.NotEmpty(s.testexporter.Records)
			s.Equal(tt.want, s.testexporter.Records)
		})
	}
}

func (s *eventsSuite) TestTimerMetricFunc_Record() {
	tests := []struct {
		name string
		v    time.Duration
		tags []Tag
		want []metrictest.ExportRecord
	}{
		{
			"test-timer",
			2 * time.Hour,
			nil,
			[]metrictest.ExportRecord{
				{
					InstrumentName:         "go.temporal.io/server/common/metrics/test-timer",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.HistogramKind,
					Sum:                    number.NewInt64Number(int64(2 * time.Hour)),
					Count:                  1,
					Histogram: aggregation.Buckets{
						Boundaries: []float64{5000, 10000, 25000, 50000, 100000, 250000, 500000, 1e+06, 2.5e+06, 5e+06, 1e+07},
						Counts:     []uint64{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
					},
				},
			},
		},
		{
			"test-timer2",
			4 * time.Hour,
			[]Tag{OperationTag("awesome")},
			[]metrictest.ExportRecord{
				{
					InstrumentName:         "go.temporal.io/server/common/metrics/test-timer2",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.HistogramKind,
					Sum:                    number.NewInt64Number(int64(4 * time.Hour)),
					Count:                  1,
					Histogram: aggregation.Buckets{
						Boundaries: []float64{5000, 10000, 25000, 50000, 100000, 250000, 500000, 1e+06, 2.5e+06, 5e+06, 1e+07},
						Counts:     []uint64{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
					},
					Attributes: []attribute.KeyValue{
						attribute.String("operation", "awesome"),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			emp := NewEventMetricProvider(NewMetricEventExporter(NewOtelMetricHandler(s.meterProvider.Meter("test"))))
			emp.Timer(tt.name, WithMetricUnit("ms")).Record(tt.v, tt.tags...)
			s.testexporter.Collect(emp.context)

			s.NotEmpty(s.testexporter.Records)
			s.Equal(tt.want, s.testexporter.Records)
		})
	}
}

func (s *eventsSuite) TestHistogramMetricFunc_Record() {
	tests := []struct {
		name string
		v    int64
		tags []Tag
		want []metrictest.ExportRecord
	}{
		{
			"test-histogram",
			2,
			nil,
			[]metrictest.ExportRecord{
				{
					InstrumentName:         "go.temporal.io/server/common/metrics/test-histogram",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.HistogramKind,
					Sum:                    number.NewInt64Number(2),
					Count:                  1,
					Histogram: aggregation.Buckets{
						Boundaries: []float64{5000, 10000, 25000, 50000, 100000, 250000, 500000, 1e+06, 2.5e+06, 5e+06, 1e+07},
						Counts:     []uint64{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
					},
				},
			},
		},
		{
			"test-histogram2",
			4,
			[]Tag{OperationTag("awesome")},
			[]metrictest.ExportRecord{
				{
					InstrumentName:         "go.temporal.io/server/common/metrics/test-histogram2",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.HistogramKind,
					Sum:                    number.NewInt64Number(4.0),
					Count:                  1,
					Histogram: aggregation.Buckets{
						Boundaries: []float64{5000, 10000, 25000, 50000, 100000, 250000, 500000, 1e+06, 2.5e+06, 5e+06, 1e+07},
						Counts:     []uint64{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
					},
					Attributes: []attribute.KeyValue{
						attribute.String("operation", "awesome"),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			emp := NewEventMetricProvider(NewMetricEventExporter(NewOtelMetricHandler(s.meterProvider.Meter("test"))))
			emp.Histogram(tt.name, WithMetricUnit("By")).Record(tt.v, tt.tags...)
			s.testexporter.Collect(emp.context)

			s.NotEmpty(s.testexporter.Records)
			s.Equal(tt.want, s.testexporter.Records)
		})
	}
}

func (s *eventsSuite) TestCounterMetricWithTagsMergeFunc_Record() {
	tests := []struct {
		name     string
		v        int64
		rootTags []Tag
		tags     []Tag
		want     []metrictest.ExportRecord
	}{
		{
			"test-counter",
			2,
			[]Tag{OperationTag("awesome")},
			nil,
			[]metrictest.ExportRecord{
				{
					InstrumentName:         "go.temporal.io/server/common/metrics/test-counter",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.SumKind,
					Sum:                    number.NewInt64Number(2),
					Attributes: []attribute.KeyValue{
						attribute.String("operation", "awesome"),
					},
				},
			},
		},
		{
			"test-counter2",
			4,
			[]Tag{OperationTag("awesome")},
			[]Tag{StringTag("new-tag", "new-value")},
			[]metrictest.ExportRecord{
				{
					InstrumentName:         "go.temporal.io/server/common/metrics/test-counter2",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.SumKind,
					Sum:                    number.NewInt64Number(4),
					Attributes: []attribute.KeyValue{
						attribute.String("operation", "awesome"),
						attribute.String("new-tag", "new-value"),
					},
				},
			},
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			emp := NewEventMetricProvider(NewMetricEventExporter(NewOtelMetricHandler(s.meterProvider.Meter("test")))).WithTags(tt.rootTags...).(*eventMetricProvider)
			emp.Counter(tt.name).Record(tt.v, tt.tags...)
			s.testexporter.Collect(emp.context)

			s.NotEmpty(s.testexporter.Records)

			sort.Slice(tt.want[0].Attributes, func(i, j int) bool {
				return tt.want[0].Attributes[i].Key < tt.want[0].Attributes[j].Key
			})

			sort.Slice(s.testexporter.Records[0].Attributes, func(i, j int) bool {
				return s.testexporter.Records[0].Attributes[i].Key < s.testexporter.Records[0].Attributes[j].Key
			})

			s.Equal(tt.want, s.testexporter.Records)
		})
	}
}

func BenchmarkParallelHistogram(b *testing.B) {
	emp := NewEventMetricProvider(nil)
	b.ResetTimer()
	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				emp.Histogram("test-bench-histogram", WithMetricUnit(Bytes)).Record(1024, OperationTag("everything-is-awesome-3"))
			}
		},
	)
}

func BenchmarkParallelCounter(b *testing.B) {
	emp := NewEventMetricProvider(nil)
	b.ResetTimer()
	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				emp.Counter("test-bench-counter").Record(1024, OperationTag("everything-is-awesome-1"))
			}
		},
	)
}

func BenchmarkParallelGauge(b *testing.B) {
	emp := NewEventMetricProvider(nil)
	b.ResetTimer()
	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				emp.Gauge("test-bench-gauge").Record(1024, OperationTag("everything-is-awesome-2"))
			}
		},
	)
}

func BenchmarkParallelTimer(b *testing.B) {
	emp := NewEventMetricProvider(nil)
	b.ResetTimer()
	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				emp.Timer("test-bench-timer").Record(time.Hour, OperationTag("everything-is-awesome-4"))
			}
		},
	)
}

func BenchmarkAllTheMetrics(b *testing.B) {
	emp := NewEventMetricProvider(nil)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		emp.Histogram("test-bench-histogram", WithMetricUnit(Bytes)).Record(1024, OperationTag("everything-is-awesome-3"))
		emp.Counter("test-bench-counter").Record(1024, OperationTag("everything-is-awesome-1"))
		emp.Gauge("test-bench-gauge").Record(1024, OperationTag("everything-is-awesome-2"))
		emp.Timer("test-bench-timer").Record(time.Hour, OperationTag("everything-is-awesome-4"))
	}
}
