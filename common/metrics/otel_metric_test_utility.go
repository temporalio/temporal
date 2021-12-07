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
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/metrictest"

	"go.temporal.io/server/common/log"
)

var _ MetricTestUtility = (*OtelMetricTestUtility)(nil)
var _ OpentelemetryReporter = (*TestOtelReporter)(nil)

type (
	TestOtelReporter struct {
		MeterProvider *metrictest.MeterProvider
	}

	OtelMetricTestUtility struct {
		reporter   *TestOtelReporter
		gaugeCache OtelGaugeCache
	}
)

func (t *OtelMetricTestUtility) CollectionSize() int {
	return len(t.reporter.MeterProvider.MeasurementBatches)
}

func NewOtelMetricTestUtility() *OtelMetricTestUtility {
	reporter := NewTestOtelReporter()
	return &OtelMetricTestUtility{
		reporter:   reporter,
		gaugeCache: NewOtelGaugeCache(reporter),
	}
}

func (t *OtelMetricTestUtility) GetClient(config *ClientConfig, idx ServiceIdx) Client {
	result, err := newOpentelemeteryClient(config, idx, t.reporter, log.NewNoopLogger(), t.gaugeCache)
	if err != nil {
		panic(err)
	}
	return result
}

func (t *OtelMetricTestUtility) ContainsCounter(name MetricName, labels map[string]string, value int64) error {
	t.reporter.MeterProvider.RunAsyncInstruments()
	batches := t.reporter.MeterProvider.MeasurementBatches
	if len(batches) == 0 {
		return fmt.Errorf("no entries were recorded")
	}
	for _, batch := range batches {
		if !t.labelsMatch(batch.Labels, labels) {
			continue
		}
		measurements := batch.Measurements
		for i := range measurements {
			measurement := measurements[i]
			descriptor := measurement.Instrument.Descriptor()
			if descriptor.Name() == string(name) {
				if measurement.Number.CompareInt64(value) == 0 {
					return nil
				} else {
					return fmt.Errorf("%s metric value %d != expected %d", name, measurement.Number.AsInt64(), value)
				}
			}
		}
	}
	return fmt.Errorf("%s not found in batches", name)
}

func (t *OtelMetricTestUtility) ContainsGauge(name MetricName, labels map[string]string, value float64) error {
	t.reporter.MeterProvider.RunAsyncInstruments()
	batches := t.reporter.MeterProvider.MeasurementBatches
	if len(batches) == 0 {
		return fmt.Errorf("no entries were recorded")
	}
	for _, batch := range batches {
		if !t.labelsMatch(batch.Labels, labels) {
			continue
		}
		measurements := batch.Measurements
		for i := range measurements {
			measurement := measurements[i]
			descriptor := measurement.Instrument.Descriptor()
			if descriptor.Name() == string(name) {
				if measurement.Number.CompareFloat64(value) == 0 {
					return nil
				} else {
					return fmt.Errorf("%s metric value %f != expected %f", name, measurement.Number.AsFloat64(), value)
				}
			}
		}
	}
	return fmt.Errorf("%s not found in batches", name)
}

func (t *OtelMetricTestUtility) ContainsTimer(name MetricName, labels map[string]string, value time.Duration) error {
	t.reporter.MeterProvider.RunAsyncInstruments()
	batches := t.reporter.MeterProvider.MeasurementBatches
	if len(batches) == 0 {
		return fmt.Errorf("no entries were recorded")
	}
	for _, batch := range batches {
		if !t.labelsMatch(batch.Labels, labels) {
			continue
		}
		measurements := batch.Measurements
		for i := range measurements {
			measurement := measurements[i]
			descriptor := measurement.Instrument.Descriptor()
			if descriptor.Name() == string(name) {
				if measurement.Number.CompareInt64(value.Nanoseconds()) == 0 {
					return nil
				} else {
					return fmt.Errorf("%s metric value %d != expected %d", name, measurement.Number.AsInt64(), value.Nanoseconds())
				}
			}
		}
	}
	return fmt.Errorf("%s not found in batches", name)
}

func (t *OtelMetricTestUtility) ContainsHistogram(name MetricName, labels map[string]string, value int) error {
	t.reporter.MeterProvider.RunAsyncInstruments()
	batches := t.reporter.MeterProvider.MeasurementBatches
	if len(batches) == 0 {
		return fmt.Errorf("no entries were recorded")
	}
	for _, batch := range batches {
		if !t.labelsMatch(batch.Labels, labels) {
			continue
		}
		measurements := batch.Measurements
		for i := range measurements {
			measurement := measurements[i]
			descriptor := measurement.Instrument.Descriptor()
			if descriptor.Name() == string(name) {
				if measurement.Number.CompareInt64(int64(value)) == 0 {
					return nil
				} else {
					return fmt.Errorf("%s metric value %d != expected %d", name, measurement.Number.AsInt64(), value)
				}
			}
		}
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
	return &TestOtelReporter{
		MeterProvider: metrictest.NewMeterProvider(),
	}
}

func (t TestOtelReporter) GetMeter() metric.Meter {
	return t.MeterProvider.Meter("")
}

func (t TestOtelReporter) GetMeterMust() metric.MeterMust {
	return metric.Must(t.MeterProvider.Meter(""))
}

func (t TestOtelReporter) NewClient(logger log.Logger, serviceIdx ServiceIdx) (Client, error) {
	panic("should not be used in current tests setup")
}

func (t TestOtelReporter) Stop(logger log.Logger) {}
