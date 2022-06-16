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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally/v4"
)

func TestTallyScope(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	mp := NewTallyMetricsHandler(defaultConfig, scope)
	recordTallyMetrics(mp)

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

func recordTallyMetrics(mp MetricsHandler) {
	c := mp.Counter("hits")
	g := mp.Gauge("temp")
	d := mp.Timer("latency")
	h := mp.Histogram("transmission", Bytes)

	c.Record(8)
	g.Record(-100, StringTag("location", "Mare Imbrium"))
	d.Record(1248 * time.Millisecond)
	d.Record(1255 * time.Millisecond)
	h.Record(1234567)
}
