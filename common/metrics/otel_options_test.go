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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
)

type testCase struct {
	name         string
	catalog      map[string]metricDefinition
	expectedOpts []metric.InstrumentOption
}

func TestAddOptions(t *testing.T) {
	t.Parallel()

	metricName := "foo"
	inputOpts := []metric.InstrumentOption{
		metric.WithDescription("foo description"),
		metric.WithUnit(Milliseconds),
	}
	for _, c := range []testCase{
		{
			name:         "missing metric",
			catalog:      map[string]metricDefinition{},
			expectedOpts: inputOpts,
		},
		{
			name: "empty metric definition",
			catalog: map[string]metricDefinition{
				metricName: {},
			},
			expectedOpts: inputOpts,
		},
		{
			name: "opts overwritten",
			catalog: map[string]metricDefinition{
				metricName: {
					description: "bar description",
					unit:        Bytes,
				},
			},
			expectedOpts: []metric.InstrumentOption{
				metric.WithDescription("foo description"),
				metric.WithUnit(Milliseconds),
				metric.WithDescription("bar description"),
				metric.WithUnit(Bytes),
			},
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			handler := &otelMetricsHandler{catalog: c.catalog}
			var (
				counter counterOptions
				gauge   gaugeOptions
				hist    histogramOptions
			)
			for _, opt := range inputOpts {
				counter = append(counter, opt.(metric.Int64CounterOption))
				gauge = append(gauge, opt.(metric.Float64ObservableGaugeOption))
				hist = append(hist, opt.(metric.Int64HistogramOption))
			}
			counter = addOptions(handler, counter, metricName)
			gauge = addOptions(handler, gauge, metricName)
			hist = addOptions(handler, hist, metricName)
			require.Len(t, counter, len(c.expectedOpts))
			require.Len(t, gauge, len(c.expectedOpts))
			require.Len(t, hist, len(c.expectedOpts))
			for i, opt := range c.expectedOpts {
				assert.Equal(t, opt, counter[i])
				assert.Equal(t, opt, gauge[i])
				assert.Equal(t, opt, hist[i])
			}
		})
	}
}
