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
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/export/aggregation"
	"go.opentelemetry.io/otel/sdk/metric/metrictest"
	"go.opentelemetry.io/otel/sdk/metric/number"
	"golang.org/x/exp/event"
	"golang.org/x/exp/event/eventtest"

	"go.temporal.io/server/common/log"
)

type (
	eventsSuite struct {
		suite.Suite
		*require.Assertions
	}

	testProvider struct {
		meter metric.Meter
	}
)

func (t *testProvider) GetMeter() metric.Meter {
	return t.meter
}

func (t *testProvider) Stop(log.Logger) {}

func TestEventsSuite(t *testing.T) {
	s := new(eventsSuite)
	suite.Run(t, s)
}

func (s *eventsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *eventsSuite) TestEventsMetricProvider_WithTags() {
	tests := []struct {
		name string
		tags []Tag
		want eventsMetricProvider
	}{
		{
			"empty tags",
			[]Tag{},
			eventsMetricProvider{
				tags: nil,
			},
		},
		{
			"operation tag",
			[]Tag{OperationTag("awesome")},
			eventsMetricProvider{
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
			emp := NewEventsMetricProvider(NoopMetricHandler)
			got := emp.WithTags(tt.tags...).(*eventsMetricProvider)

			if diff := cmp.Diff(tt.want, *got,
				cmp.Comparer(valuesEqual),
				cmpopts.IgnoreFields(eventsMetricProvider{}, "ctx"),
				cmp.AllowUnexported(eventsMetricProvider{}),
				cmp.AllowUnexported(tagImpl{})); diff != "" {
				t.Errorf("mismatch (-want, got):\n%s", diff)
			}
		})
	}
}

func (s *eventsSuite) TestCounterMetricFunc_Record() {
	meterProvider, testexporter := metrictest.NewTestMeterProvider()
	ctx := event.WithExporter(context.Background(), event.NewExporter(&eventtest.CaptureHandler{}, nil))
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
					InstrumentName:         "test-counter",
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
					InstrumentName:         "test-counter2",
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
			emp := NewEventsMetricProvider(NewOtelMetricHandler(log.NewTestLogger(), &testProvider{meter: meterProvider.Meter("test")}, ClientConfig{}))
			emp.Counter(tt.name, &MetricOptions{
				Description: "what you see is not a test",
			}).Record(tt.v, tt.tags...)
			testexporter.Collect(ctx)

			s.NotEmpty(testexporter.Records)
			if diff := cmp.Diff(tt.want, testexporter.Records, cmp.Comparer(valuesEqual)); diff != "" {
				t.Errorf("mismatch (-want, got):\n%s", diff)
			}
		})
	}
}

func (s *eventsSuite) TestGaugeMetricFunc_Record() {
	meterProvider, testexporter := metrictest.NewTestMeterProvider()
	ctx := event.WithExporter(context.Background(), event.NewExporter(&eventtest.CaptureHandler{}, nil))

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
					InstrumentName:         "test-gauge",
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
					InstrumentName:         "test-gauge2",
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
			emp := NewEventsMetricProvider(NewOtelMetricHandler(log.NewTestLogger(), &testProvider{meter: meterProvider.Meter("test")}, ClientConfig{}))
			emp.Gauge(tt.name, &MetricOptions{
				Description: "what you see is not a test",
			}).Record(tt.v, tt.tags...)
			testexporter.Collect(ctx)

			s.NotEmpty(testexporter.Records)
			if diff := cmp.Diff(tt.want, testexporter.Records, cmp.Comparer(valuesEqual)); diff != "" {
				t.Errorf("mismatch (-want, got):\n%s", diff)
			}
		})
	}
}

func (s *eventsSuite) TestTimerMetricFunc_Record() {
	meterProvider, testexporter := metrictest.NewTestMeterProvider()
	ctx := event.WithExporter(context.Background(), event.NewExporter(&eventtest.CaptureHandler{}, nil))

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
					InstrumentName:         "test-timer",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.HistogramKind,
					Sum:                    number.NewInt64Number(int64(2 * time.Hour)),
					Count:                  1,
					Histogram: aggregation.Buckets{
						Counts: []uint64{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
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
					InstrumentName:         "test-timer2",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.HistogramKind,
					Sum:                    number.NewInt64Number(int64(4 * time.Hour)),
					Count:                  1,
					Histogram: aggregation.Buckets{
						Counts: []uint64{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
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
			emp := NewEventsMetricProvider(NewOtelMetricHandler(log.NewTestLogger(), &testProvider{meter: meterProvider.Meter("test")}, ClientConfig{}))
			emp.Timer(tt.name, &MetricOptions{
				Description: "what you see is not a test",
				Unit:        Milliseconds,
			}).Record(tt.v, tt.tags...)
			testexporter.Collect(ctx)

			s.NotEmpty(testexporter.Records)
			if diff := cmp.Diff(tt.want, testexporter.Records, cmp.Comparer(valuesEqual), cmpopts.IgnoreFields(aggregation.Buckets{}, "Boundaries")); diff != "" {
				t.Errorf("mismatch (-want, got):\n%s", diff)
			}
		})
	}
}

func (s *eventsSuite) TestHistogramMetricFunc_Record() {
	meterProvider, testexporter := metrictest.NewTestMeterProvider()
	ctx := event.WithExporter(context.Background(), event.NewExporter(&eventtest.CaptureHandler{}, nil))

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
					InstrumentName:         "test-histogram",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.HistogramKind,
					Sum:                    number.NewInt64Number(2),
					Count:                  1,
					Histogram: aggregation.Buckets{
						Counts: []uint64{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
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
					InstrumentName:         "test-histogram2",
					InstrumentationLibrary: metrictest.Library{InstrumentationName: "test"},
					AggregationKind:        aggregation.HistogramKind,
					Sum:                    number.NewInt64Number(4.0),
					Count:                  1,
					Histogram: aggregation.Buckets{
						Counts: []uint64{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
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
			emp := NewEventsMetricProvider(NewOtelMetricHandler(log.NewTestLogger(), &testProvider{meter: meterProvider.Meter("test")}, ClientConfig{}))
			emp.Histogram(tt.name, &MetricOptions{
				Description: "what you see is not a test",
				Unit:        Bytes,
			}).Record(tt.v, tt.tags...)
			testexporter.Collect(ctx)

			s.NotEmpty(testexporter.Records)
			if diff := cmp.Diff(tt.want, testexporter.Records, cmp.Comparer(valuesEqual), cmpopts.IgnoreFields(aggregation.Buckets{}, "Boundaries")); diff != "" {
				t.Errorf("mismatch (-want, got):\n%s", diff)
			}
		})
	}
}

func (s *eventsSuite) TestCounterMetricWithTagsMergeFunc_Record() {
	meterProvider, testexporter := metrictest.NewTestMeterProvider()
	ctx := event.WithExporter(context.Background(), event.NewExporter(&eventtest.CaptureHandler{}, nil))

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
					InstrumentName:         "test-counter",
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
					InstrumentName:         "test-counter2",
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
			emp := NewEventsMetricProvider(NewOtelMetricHandler(log.NewTestLogger(), &testProvider{meter: meterProvider.Meter("test")}, ClientConfig{})).WithTags(tt.rootTags...).(*eventsMetricProvider)
			emp.Counter(tt.name, &MetricOptions{
				Description: "what you see is not a test",
			}).Record(tt.v, tt.tags...)
			testexporter.Collect(ctx)

			s.NotEmpty(testexporter.Records)

			if diff := cmp.Diff(tt.want, testexporter.Records, cmp.Comparer(valuesEqual), cmpopts.SortSlices(func(x, y attribute.KeyValue) bool {
				return x.Key < y.Key
			})); diff != "" {
				t.Errorf("mismatch (-want, got):\n%s", diff)
			}
		})
	}
}

func BenchmarkParallelHistogram(b *testing.B) {
	var emp MetricProvider = NewEventsMetricProvider(NoopMetricHandler).WithTags(OperationTag("everything-is-awesome-3"))
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				emp.Histogram("test-bench-histogram", &MetricOptions{
					Description: "what you see is not a test",
					Unit:        Bytes,
				}).Record(1024)
			}
		},
	)
}

func BenchmarkParallelCounter(b *testing.B) {
	var emp MetricProvider = NewEventsMetricProvider(NoopMetricHandler).WithTags(OperationTag("everything-is-awesome-1"))
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				emp.Counter("test-bench-counter", nil).Record(1024)
			}
		},
	)
}

func BenchmarkParallelGauge(b *testing.B) {
	var emp MetricProvider = NewEventsMetricProvider(NoopMetricHandler).WithTags(OperationTag("everything-is-awesome-2"))
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				emp.Gauge("test-bench-gauge", nil).Record(1024)
			}
		},
	)
}

func BenchmarkParallelTimer(b *testing.B) {
	var emp MetricProvider = NewEventsMetricProvider(NoopMetricHandler).WithTags(OperationTag("everything-is-awesome-4"))
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				emp.Timer("test-bench-timer", nil).Record(time.Hour)
			}
		},
	)
}

func BenchmarkAllTheMetrics(b *testing.B) {
	var emp MetricProvider = NewEventsMetricProvider(NoopMetricHandler).WithTags(OperationTag("everything-is-awesome-3"))
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				emp.Histogram("test-bench-histogram", &MetricOptions{
					Unit: Bytes,
				}).Record(1024, ServiceTypeTag("test-service"))
				emp.Counter("test-bench-counter", nil).Record(1024, ServiceTypeTag("test-service"))
				emp.Gauge("test-bench-gauge", nil).Record(1024, ServiceTypeTag("test-service"))
				emp.Timer("test-bench-timer", &MetricOptions{
					Unit: Milliseconds,
				}).Record(time.Hour, ServiceTypeTag("test-service"))
			}
		},
	)
}

func BenchmarkAllTheMetricsAgain(b *testing.B) {
	var emp MetricProvider = NewEventsMetricProvider(NoopMetricHandler).WithTags(OperationTag("everything-is-awesome-3"))

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				emp.Histogram("test-bench-histogram", &MetricOptions{
					Unit: Bytes,
				}).Record(1024, ServiceTypeTag("test-service"))
				emp.Counter("test-bench-counter", nil).Record(1024, ServiceTypeTag("test-service"))
				emp.Gauge("test-bench-gauge", nil).Record(1024, ServiceTypeTag("test-service"))
				emp.Timer("test-bench-timer", &MetricOptions{
					Unit: Milliseconds,
				}).Record(time.Hour, ServiceTypeTag("test-service"))
				emp.Histogram("test-bench-histogram", &MetricOptions{
					Unit: Bytes,
				}).Record(1024, ServiceTypeTag("test-service"))
				emp.Counter("test-bench-counter", nil).Record(1024, ServiceTypeTag("test-service"))
				emp.Gauge("test-bench-gauge", nil).Record(1024, ServiceTypeTag("test-service"))
				emp.Timer("test-bench-timer", &MetricOptions{
					Unit: Milliseconds,
				}).Record(time.Hour, ServiceTypeTag("test-service"))
				emp.Histogram("test-bench-histogram", &MetricOptions{
					Unit: Bytes,
				}).Record(1024, ServiceTypeTag("test-service"))
				emp.Counter("test-bench-counter", nil).Record(1024, ServiceTypeTag("test-service"))
				emp.Gauge("test-bench-gauge", nil).Record(1024, ServiceTypeTag("test-service"))
				emp.Timer("test-bench-timer", &MetricOptions{
					Unit: Milliseconds,
				}).Record(time.Hour, ServiceTypeTag("test-service"))
			}
		},
	)
}
