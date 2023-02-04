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
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/unit"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"go.temporal.io/server/common/log"
)

var (
	minLatency = float64(1248)
	maxLatency = float64(5255)
	testBytes  = float64(1234567)
)

type testProvider struct {
	meter metric.Meter
}

func (t *testProvider) GetMeter() metric.Meter {
	return t.meter
}

func (t *testProvider) Stop(log.Logger) {}

func TestMeter(t *testing.T) {
	ctx := context.Background()
	rdr := sdkmetrics.NewManualReader()
	provider := sdkmetrics.NewMeterProvider(
		sdkmetrics.WithReader(rdr),
		sdkmetrics.WithView(
			sdkmetrics.NewView(
				sdkmetrics.Instrument{
					Kind: sdkmetrics.InstrumentKindSyncHistogram,
					Unit: unit.Bytes,
				},
				sdkmetrics.Stream{
					Aggregation: aggregation.ExplicitBucketHistogram{
						Boundaries: defaultConfig.PerUnitHistogramBoundaries[string(unit.Bytes)],
					},
				},
			),
			sdkmetrics.NewView(
				sdkmetrics.Instrument{
					Kind: sdkmetrics.InstrumentKindSyncHistogram,
					Unit: unit.Dimensionless,
				},
				sdkmetrics.Stream{
					Aggregation: aggregation.ExplicitBucketHistogram{
						Boundaries: defaultConfig.PerUnitHistogramBoundaries[string(unit.Dimensionless)],
					},
				},
			),
			sdkmetrics.NewView(
				sdkmetrics.Instrument{
					Kind: sdkmetrics.InstrumentKindSyncHistogram,
					Unit: unit.Milliseconds,
				},
				sdkmetrics.Stream{
					Aggregation: aggregation.ExplicitBucketHistogram{
						Boundaries: defaultConfig.PerUnitHistogramBoundaries[string(unit.Milliseconds)],
					},
				},
			),
		),
	)
	p := NewOtelMetricsHandler(log.NewTestLogger(), &testProvider{meter: provider.Meter("test")}, defaultConfig)
	recordMetrics(p)

	got, err := rdr.Collect(ctx)
	assert.Nil(t, err)

	want := []metricdata.Metrics{
		{
			Name: "hits",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 8,
					},
				},
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
			},
		},
		{
			Name: "hits-tagged",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						//Attributes: attribute.NewSet(attribute.String("taskqueue", "__sticky__")),
						Value: 11,
					},
				},
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
			},
		},
		{
			Name: "hits-tagged-excluded",
			Data: metricdata.Sum[int64]{
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 14,
					},
				},
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
			},
		},
		{
			Name: "latency",
			Data: metricdata.Histogram{
				DataPoints: []metricdata.HistogramDataPoint{
					{
						Count:        2,
						BucketCounts: []uint64{0, 0, 0, 1, 1, 0},
						Min:          &minLatency,
						Max:          &maxLatency,
						Sum:          6503,
					},
				},
				Temporality: metricdata.CumulativeTemporality,
			},
			Unit: unit.Milliseconds,
		},
		{
			Name: "temp",
			Data: metricdata.Gauge[float64]{
				DataPoints: []metricdata.DataPoint[float64]{
					{
						//Attributes: attribute.NewSet(attribute.String("location", "Mare Imbrium")),
						Value: 100,
					},
				},
			},
		},
		{
			Name: "transmission",
			Data: metricdata.Histogram{
				DataPoints: []metricdata.HistogramDataPoint{
					{
						Count:        1,
						BucketCounts: []uint64{0, 0, 1},
						Min:          &testBytes,
						Max:          &testBytes,
						Sum:          testBytes,
					},
				},
				Temporality: metricdata.CumulativeTemporality,
			},
			Unit: unit.Bytes,
		},
	}
	if diff := cmp.Diff(want, got.ScopeMetrics[0].Metrics, cmp.Comparer(valuesEqual),
		cmpopts.SortSlices(func(x, y metricdata.Metrics) bool {
			return x.Name < y.Name
		}),
		// TODO: No good way to verify metrics tag in attributes as a private field in the attribute.Set.
		cmpopts.IgnoreFields(metricdata.DataPoint[int64]{}, "Attributes", "StartTime", "Time"),
		cmpopts.IgnoreFields(metricdata.DataPoint[float64]{}, "Attributes", "StartTime", "Time"),
		cmpopts.IgnoreFields(metricdata.HistogramDataPoint{}, "Attributes", "StartTime", "Time", "Bounds"),
	); diff != "" {
		t.Errorf("mismatch (-want, got):\n%s", diff)
	}
}

func valuesEqual(v1, v2 attribute.Value) bool {
	return v1.AsInterface() == v2.AsInterface()
}

func recordMetrics(mp Handler) {
	hitsCounter := mp.Counter("hits")
	gauge := mp.Gauge("temp")

	timer := mp.Timer("latency")
	histogram := mp.Histogram("transmission", Bytes)
	hitsTaggedCounter := mp.Counter("hits-tagged")
	hitsTaggedExcludedCounter := mp.Counter("hits-tagged-excluded")

	hitsCounter.Record(8)
	gauge.Record(100, StringTag("location", "Mare Imbrium"))
	timer.Record(time.Duration(minLatency) * time.Millisecond)
	timer.Record(time.Duration(maxLatency) * time.Millisecond)
	histogram.Record(int64(testBytes))
	hitsTaggedCounter.Record(11, TaskQueueTag("__sticky__"))
	hitsTaggedExcludedCounter.Record(14, TaskQueueTag("filtered"))
}
