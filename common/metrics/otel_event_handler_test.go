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
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/export/aggregation"
	"go.opentelemetry.io/otel/sdk/metric/metrictest"
	"go.opentelemetry.io/otel/sdk/metric/number"
	"golang.org/x/exp/event"
)

func TestMeter(t *testing.T) {
	ctx := context.Background()
	mp, exp := metrictest.NewTestMeterProvider()
	mh := NewOtelMetricHandler(mp.Meter("test"))
	ctx = event.WithExporter(ctx, event.NewExporter(mh, nil))
	recordMetrics(ctx)

	err := exp.Collect(ctx)
	assert.Nil(t, err)

	lib := metrictest.Library{InstrumentationName: "test"}
	got := exp.Records

	want := []metrictest.ExportRecord{
		{
			InstrumentName:         "go.temporal.io/server/common/metrics/hits",
			Sum:                    number.NewInt64Number(8),
			Attributes:             nil,
			InstrumentationLibrary: lib,
			AggregationKind:        aggregation.SumKind,
			NumberKind:             number.Int64Kind,
		},
		{
			InstrumentName:         "go.temporal.io/server/common/metrics/latency",
			Sum:                    number.NewInt64Number(int64(2503 * time.Millisecond)),
			Count:                  2,
			Attributes:             nil,
			InstrumentationLibrary: lib,
			AggregationKind:        aggregation.HistogramKind,
			NumberKind:             number.Int64Kind,
			Histogram: aggregation.Buckets{
				Boundaries: []float64{5000, 10000, 25000, 50000, 100000, 250000, 500000, 1e+06, 2.5e+06, 5e+06, 1e+07},
				Counts:     []uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},
			},
		},
		{
			InstrumentName: "go.temporal.io/server/common/metrics/temp",
			Sum:            number.NewFloat64Number(-100),
			Attributes: []attribute.KeyValue{
				{
					Key:   attribute.Key("location"),
					Value: attribute.StringValue("Mare Imbrium"),
				},
			},
			InstrumentationLibrary: lib,
			AggregationKind:        aggregation.SumKind,
			NumberKind:             number.Float64Kind,
		},
	}

	// Sort for comparison
	sort.Slice(got, func(i, j int) bool {
		return got[i].InstrumentName < got[j].InstrumentName
	})

	sort.Slice(want, func(i, j int) bool {
		return want[i].InstrumentName < want[j].InstrumentName
	})

	if diff := cmp.Diff(want, got, cmp.Comparer(valuesEqual)); diff != "" {
		t.Errorf("mismatch (-want, got):\n%s", diff)
	}
}

func valuesEqual(v1, v2 attribute.Value) bool {
	return v1.AsInterface() == v2.AsInterface()
}

func recordMetrics(ctx context.Context) {
	c := event.NewCounter("hits", &event.MetricOptions{Description: "Earth meteorite hits"})
	g := event.NewFloatGauge("temp", &event.MetricOptions{Description: "moon surface temperature in Kelvin"})
	d := event.NewDuration("latency", &event.MetricOptions{Description: "Earth-moon comms lag, milliseconds"})

	c.Record(ctx, 8)
	g.Record(ctx, -100, event.String("location", "Mare Imbrium"))
	d.Record(ctx, 1248*time.Millisecond)
	d.Record(ctx, 1255*time.Millisecond)
}
