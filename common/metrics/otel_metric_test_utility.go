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
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	"go.opentelemetry.io/otel/sdk/metric/export"
	"go.opentelemetry.io/otel/sdk/metric/export/aggregation"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	selector "go.opentelemetry.io/otel/sdk/metric/selector/simple"

	"go.temporal.io/server/common/log"
)

var _ MetricTestUtility = (*OtelMetricTestUtility)(nil)
var _ OpenTelemetryReporter = (*TestOtelReporter)(nil)

type (
	TestOtelReporter struct {
		// TODO: https://github.com/open-telemetry/opentelemetry-go/issues/2722
		controller *controller.Controller
		userScope  UserScope
	}

	OtelMetricTestUtility struct {
		reporter   *TestOtelReporter
		collection []export.Record
		gaugeCache OtelGaugeCache
	}
)

func NewOtelMetricTestUtility() *OtelMetricTestUtility {
	reporter := NewTestOtelReporter()
	return &OtelMetricTestUtility{
		reporter:   reporter,
		gaugeCache: NewOtelGaugeCache(reporter.GetMeter()),
	}
}

func (t *OtelMetricTestUtility) GetClient(config *ClientConfig, idx ServiceIdx) Client {
	result, err := NewOpenTelemetryClient(config, idx, t.reporter, log.NewNoopLogger(), t.gaugeCache)
	if err != nil {
		panic(err)
	}
	return result
}

func (t *OtelMetricTestUtility) collect() {
	t.reporter.controller.Collect(context.Background())
	t.reporter.controller.ForEach(func(l instrumentation.Library, r export.Reader) error {
		return r.ForEach(aggregation.CumulativeTemporalitySelector(), func(rec export.Record) error {
			for idx, existingRec := range t.collection {
				if existingRec.Descriptor().Name() == rec.Descriptor().Name() &&
					existingRec.Labels().Equals(rec.Labels()) {
					t.collection[idx] = rec
					return nil
				}
			}

			t.collection = append(t.collection, rec)
			return nil
		})
	})
}

func (t *OtelMetricTestUtility) CollectionSize() int {
	t.collect()

	return len(t.collection)
}

func (t *OtelMetricTestUtility) ContainsCounter(name MetricName, labels map[string]string, value int64) error {
	t.collect()
	if len(t.collection) == 0 {
		return fmt.Errorf("no entries were recorded")
	}
	for _, rec := range t.collection {
		if !t.labelsMatch(rec.Labels().ToSlice(), labels) {
			continue
		}
		if rec.Descriptor().Name() != name.String() {
			continue
		}
		sumAgg, ok := rec.Aggregation().(aggregation.Sum)
		if !ok {
			return fmt.Errorf("record should have aggregation type Count")
		}
		sum, err := sumAgg.Sum()
		if err != nil {
			return err
		}

		if sum.CompareInt64(value) != 0 {
			return fmt.Errorf("%s metric value %d != expected %d", name, sum.AsInt64(), value)
		}
		return nil
	}

	return fmt.Errorf("%s not found in batches", name)
}

func (t *OtelMetricTestUtility) ContainsGauge(name MetricName, labels map[string]string, value float64) error {
	t.collect()
	if len(t.collection) == 0 {
		return fmt.Errorf("no entries were recorded")
	}
	for _, rec := range t.collection {
		if !t.labelsMatch(rec.Labels().ToSlice(), labels) {
			continue
		}
		if rec.Descriptor().Name() != name.String() {
			continue
		}
		lastValueAgg, ok := rec.Aggregation().(aggregation.LastValue)
		if !ok {
			return fmt.Errorf("record should have aggregation type Count")
		}
		lastValue, _, err := lastValueAgg.LastValue()
		if err != nil {
			return err
		}

		if lastValue.CompareFloat64(value) != 0 {
			return fmt.Errorf("%s metric value %f != expected %f", name, lastValue.AsFloat64(), value)
		}
		return nil
	}

	return fmt.Errorf("%s not found in batches", name)
}

func (t *OtelMetricTestUtility) ContainsTimer(name MetricName, labels map[string]string, value time.Duration) error {
	t.collect()
	if len(t.collection) == 0 {
		return fmt.Errorf("no entries were recorded")
	}
	for _, rec := range t.collection {
		if !t.labelsMatch(rec.Labels().ToSlice(), labels) {
			continue
		}
		if rec.Descriptor().Name() != name.String() {
			continue
		}
		histAgg, ok := rec.Aggregation().(aggregation.Histogram)
		if !ok {
			return fmt.Errorf("record should have aggregation type Count")
		}
		sum, err := histAgg.Sum()
		if err != nil {
			return err
		}

		if sum.CompareInt64(value.Nanoseconds()) != 0 {
			return fmt.Errorf("%s metric value %d != expected %d", name, sum.AsInt64(), value)
		}
		return nil
	}

	return fmt.Errorf("%s not found in batches", name)
}

func (t *OtelMetricTestUtility) ContainsHistogram(name MetricName, labels map[string]string, value int) error {
	t.collect()
	if len(t.collection) == 0 {
		return fmt.Errorf("no entries were recorded")
	}
	for _, rec := range t.collection {
		if !t.labelsMatch(rec.Labels().ToSlice(), labels) {
			continue
		}
		if rec.Descriptor().Name() != name.String() {
			continue
		}
		histAgg, ok := rec.Aggregation().(aggregation.Histogram)
		if !ok {
			return fmt.Errorf("record should have aggregation type Count")
		}
		sum, err := histAgg.Sum()
		if err != nil {
			return err
		}

		if sum.CompareInt64(int64(value)) != 0 {
			return fmt.Errorf("%s metric value %d != expected %d", name, sum.AsInt64(), value)
		}
		return nil
	}

	return fmt.Errorf("%s not found in batches", name)
}

func (t *OtelMetricTestUtility) labelsMatch(left []attribute.KeyValue, right map[string]string) bool {
	for _, kv := range left {
		v, ok := right[string(kv.Key)]
		if !ok {
			return false
		}
		if v != kv.Value.AsString() {
			return false
		}
	}
	return true
}

func NewTestOtelReporter() *TestOtelReporter {
	ctr := controller.New(
		processor.NewFactory(
			selector.NewWithHistogramDistribution(),
			aggregation.CumulativeTemporalitySelector(),
		),
		controller.WithCollectPeriod(0),
	)
	meter := ctr.Meter("")
	gaugeCache := NewOtelGaugeCache(meter)
	return &TestOtelReporter{
		controller: ctr,
		userScope:  NewOpenTelemetryUserScope(meter, nil, gaugeCache),
	}
}

func (t TestOtelReporter) GetMeter() metric.Meter {
	return t.controller.Meter("")
}

func (t TestOtelReporter) NewClient(logger log.Logger, serviceIdx ServiceIdx) (Client, error) {
	panic("should not be used in current tests setup")
}

func (t TestOtelReporter) Stop(logger log.Logger) {}

func (t TestOtelReporter) UserScope() UserScope {
	return t.userScope
}
