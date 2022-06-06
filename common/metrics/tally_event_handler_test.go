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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally/v4"
	"golang.org/x/exp/event"

	"go.temporal.io/server/common/log"
)

func TestTallyScope(t *testing.T) {
	ctx := context.Background()
	scope := tally.NewTestScope("test", map[string]string{})
	mh := NewTallyMetricHandler(log.NewTestLogger(), scope, defaultConfig, defaultConfig.PerUnitHistogramBoundaries)
	ctx = event.WithExporter(ctx, event.NewExporter(mh, nil))
	recordTallyMetrics(ctx)

	snap := scope.Snapshot()
	counters, gauges, timers, histograms := snap.Counters(), snap.Gauges(), snap.Timers(), snap.Histograms()

	assert.EqualValues(t, 8, counters["test.hits+"].Value())
	assert.EqualValues(t, map[string]string{}, counters["test.hits+"].Tags())

	assert.EqualValues(t, float64(-100), gauges["test.temp+location=Mare Imbrium"].Value())
	assert.EqualValues(t, map[string]string{
		"location": "Mare Imbrium",
	}, gauges["test.temp+location=Mare Imbrium"].Tags())

	assert.EqualValues(t, []time.Duration{
		1248 * time.Millisecond,
		1255 * time.Millisecond,
	}, timers["test.latency+"].Values())
	assert.EqualValues(t, map[string]string{}, timers["test.latency+"].Tags())

	assert.EqualValues(t, map[float64]int64{
		1024:            0,
		2048:            0,
		math.MaxFloat64: 1,
	}, histograms["test.transmission+"].Values())
	assert.EqualValues(t, map[time.Duration]int64(nil), histograms["test.transmission+"].Durations())
	assert.EqualValues(t, map[string]string{}, histograms["test.transmission+"].Tags())
}

func recordTallyMetrics(ctx context.Context) {
	c := event.NewCounter("hits", &event.MetricOptions{Description: "Earth meteorite hits"})
	g := event.NewFloatGauge("temp", &event.MetricOptions{Description: "moon surface temperature in Kelvin"})
	d := event.NewDuration("latency", &event.MetricOptions{Description: "Earth-moon comms lag, milliseconds"})
	h := event.NewIntDistribution("transmission", &event.MetricOptions{Description: "Earth-moon comms sent, bytes", Unit: event.UnitBytes})

	c.Record(ctx, 8)
	g.Record(ctx, -100, event.String("location", "Mare Imbrium"))
	d.Record(ctx, 1248*time.Millisecond)
	d.Record(ctx, 1255*time.Millisecond)
	h.Record(ctx, 1234567)
}
