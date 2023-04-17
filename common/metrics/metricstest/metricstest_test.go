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

package metricstest

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

func TestBasic(t *testing.T) {
	logger := log.NewTestLogger()
	handler := MustNewHandler(logger, metrics.ClientConfig{})

	counterName := "counter1"
	counterTags := []metrics.Tag{
		metrics.StringTag("l2", "v2"),
		metrics.StringTag("l1", "v1"),
	}
	expectedSystemTags := []metrics.Tag{
		metrics.StringTag("otel_scope_name", "temporal"),
		metrics.StringTag("otel_scope_version", ""),
	}
	expectedCounterTags := append(expectedSystemTags, counterTags...)
	counter := handler.WithTags(counterTags...).Counter(counterName)
	counter.Record(1)
	counter.Record(1)

	s1 := handler.MustSnapshot()
	require.Equal(t, float64(2), s1.MustCounter(counterName+"_total", expectedCounterTags...))

	gaugeName := "gauge1"
	gaugeTags := []metrics.Tag{
		metrics.StringTag("l3", "v3"),
		metrics.StringTag("l4", "v4"),
	}
	expectedGaugeTags := append(expectedSystemTags, gaugeTags...)
	gauge := handler.WithTags(gaugeTags...).Gauge(gaugeName)
	gauge.Record(-2)
	gauge.Record(10)

	s2 := handler.MustSnapshot()
	require.Equal(t, float64(2), s2.MustCounter(counterName+"_total", expectedCounterTags...))
	require.Equal(t, float64(10), s2.MustGauge(gaugeName, expectedGaugeTags...))
}

func TestHistogram(t *testing.T) {
	logger := log.NewTestLogger()
	handler := MustNewHandler(logger, metrics.ClientConfig{
		PerUnitHistogramBoundaries: map[string][]float64{
			metrics.Dimensionless: {
				1,
				2,
				5,
			},
		},
	})

	histogramName := "histogram1"
	histogramTags := []metrics.Tag{
		metrics.StringTag("l2", "v2"),
		metrics.StringTag("l1", "v1"),
	}
	expectedSystemTags := []metrics.Tag{
		metrics.StringTag("otel_scope_name", "temporal"),
		metrics.StringTag("otel_scope_version", ""),
	}
	expectedHistogramTags := append(expectedSystemTags, histogramTags...)
	histogram := handler.WithTags(histogramTags...).Histogram(histogramName, metrics.Dimensionless)
	histogram.Record(1)
	histogram.Record(3)

	s1 := handler.MustSnapshot()

	expectedBuckets := []histogramBucket{
		{value: 1, upperBound: 1},
		{value: 1, upperBound: 2},
		{value: 2, upperBound: 5},
		{value: 2, upperBound: math.Inf(1)},
	}
	require.Equal(t, expectedBuckets, s1.MustHistogram(histogramName+"_ratio", expectedHistogramTags...))
}
