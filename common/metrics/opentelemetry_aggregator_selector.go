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
	"go.opentelemetry.io/otel/metric/sdkapi"
	emetric "go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/histogram"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/lastvalue"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/sum"
)

type (
	// OtelAggregatorSelector handles utilizing correct histogram bucket list for distinct metric unit types.
	OtelAggregatorSelector struct {
		buckets        map[MetricUnit][]histogram.Option
		defaultBuckets []histogram.Option
	}
)

var _ emetric.AggregatorSelector = &OtelAggregatorSelector{}

// Creates new instance of aggregator selector.
func NewOtelAggregatorSelector(
	defaultBoundaries []float64,
	perUnitBoundaries map[string][]float64,
) *OtelAggregatorSelector {
	perUnitBuckets := make(map[MetricUnit][]histogram.Option, len(perUnitBoundaries))
	for unit, buckets := range perUnitBoundaries {
		perUnitBuckets[MetricUnit(unit)] = []histogram.Option{histogram.WithExplicitBoundaries(buckets)}
	}
	return &OtelAggregatorSelector{
		defaultBuckets: []histogram.Option{histogram.WithExplicitBoundaries(defaultBoundaries)},
		buckets:        perUnitBuckets,
	}
}

func (s OtelAggregatorSelector) AggregatorFor(descriptor *sdkapi.Descriptor, aggPtrs ...*emetric.Aggregator) {
	switch descriptor.InstrumentKind() {
	case sdkapi.GaugeObserverInstrumentKind:
		lastValueAggs(aggPtrs)
	case sdkapi.HistogramInstrumentKind:
		options := s.defaultBuckets
		if opts, ok := s.buckets[MetricUnit(descriptor.Unit())]; ok {
			options = opts
		}
		aggs := histogram.New(len(aggPtrs), descriptor, options...)
		for i := range aggPtrs {
			*aggPtrs[i] = &aggs[i]
		}
	default:
		sumAggs(aggPtrs)
	}
}

func sumAggs(aggPtrs []*emetric.Aggregator) {
	aggs := sum.New(len(aggPtrs))
	for i := range aggPtrs {
		*aggPtrs[i] = &aggs[i]
	}
}

func lastValueAggs(aggPtrs []*emetric.Aggregator) {
	aggs := lastvalue.New(len(aggPtrs))
	for i := range aggPtrs {
		*aggPtrs[i] = &aggs[i]
	}
}
