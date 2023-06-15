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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

func TestBasic(t *testing.T) {
	t.Parallel()
	logger := log.NewTestLogger()
	handler, err := NewHandler(logger, metrics.ClientConfig{})
	require.NoError(t, err)

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

	s1, err := handler.Snapshot()
	require.NoError(t, err)

	counterVal, err := s1.Counter(counterName+"_total", expectedCounterTags...)
	require.NoError(t, err)
	assert.Equal(t, float64(2), counterVal)

	gaugeName := "gauge1"
	gaugeTags := []metrics.Tag{
		metrics.StringTag("l3", "v3"),
		metrics.StringTag("l4", "v4"),
	}
	expectedGaugeTags := append(expectedSystemTags, gaugeTags...)
	gauge := handler.WithTags(gaugeTags...).Gauge(gaugeName)
	gauge.Record(-2)
	gauge.Record(10)

	s2, err := handler.Snapshot()
	require.NoError(t, err)

	counterVal, err = s2.Counter(counterName+"_total", expectedCounterTags...)
	require.NoError(t, err)
	assert.Equal(t, float64(2), counterVal)

	gaugeVal, err := s2.Gauge(gaugeName, expectedGaugeTags...)
	require.NoError(t, err)
	assert.Equal(t, float64(10), gaugeVal)
}

func TestHistogram(t *testing.T) {
	t.Parallel()
	logger := log.NewTestLogger()
	handler, err := NewHandler(logger, metrics.ClientConfig{
		PerUnitHistogramBoundaries: map[string][]float64{
			metrics.Dimensionless: {
				1,
				2,
				5,
			},
		},
	})
	require.NoError(t, err)

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

	s1, err := handler.Snapshot()
	require.NoError(t, err)

	expectedBuckets := []HistogramBucket{
		{value: 1, upperBound: 1},
		{value: 1, upperBound: 2},
		{value: 2, upperBound: 5},
		{value: 2, upperBound: math.Inf(1)},
	}

	histogramVal, err := s1.Histogram(histogramName+"_ratio", expectedHistogramTags...)
	require.NoError(t, err)
	assert.Equal(t, expectedBuckets, histogramVal)
}
