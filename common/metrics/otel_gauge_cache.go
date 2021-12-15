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
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/atomic"
)

type (
	OtelGaugeCache interface {
		Set(name string, tags map[string]string, value float64)
	}

	otelGaugeCache struct {
		lock     sync.RWMutex
		gauges   map[string]*gaugeValues
		reporter OpentelemetryReporter
	}

	gaugeValues struct {
		lock            sync.RWMutex
		gaugeMetricName string
		values          map[string]*gaugeValue
	}

	gaugeValue struct {
		// Expected to be ordered
		tags  []attribute.KeyValue
		value atomic.Float64
	}
)

func (o *gaugeValues) getGaugeValue(tags map[string]string) *gaugeValue {
	key := buildCacheKey(tags)

	o.lock.RLock()
	if v, ok := o.values[key]; ok {
		o.lock.RUnlock()
		return v
	}
	o.lock.RUnlock()

	value := newGaugeValue(tags)

	o.lock.Lock()
	defer o.lock.Unlock()

	if v, ok := o.values[key]; ok {
		return v
	}

	o.values[key] = value
	return value
}

func newGaugeValue(tags map[string]string) *gaugeValue {
	return &gaugeValue{
		tags:  tagMapToLabelArray(tags),
		value: atomic.Float64{},
	}
}

func (o *gaugeValues) Update(tags map[string]string, value float64) {
	gaugeValue := o.getGaugeValue(tags)
	gaugeValue.value.Store(value)
}

func NewOtelGaugeCache(reporter OpentelemetryReporter) OtelGaugeCache {
	return &otelGaugeCache{
		lock:     sync.RWMutex{},
		gauges:   make(map[string]*gaugeValues),
		reporter: reporter,
	}
}

func (o *otelGaugeCache) getGaugeValues(name string) *gaugeValues {
	o.lock.RLock()
	if v, ok := o.gauges[name]; ok {
		o.lock.RUnlock()
		return v
	}
	o.lock.RUnlock()

	values := newGaugeValues(name)

	o.lock.Lock()
	defer o.lock.Unlock()

	if v, ok := o.gauges[name]; ok {
		return v
	}

	o.gauges[name] = values
	o.reporter.GetMeterMust().NewFloat64GaugeObserver(name,
		func(ctx context.Context, result metric.Float64ObserverResult) {
			values.lock.RLock()
			defer values.lock.RUnlock()

			for _, v := range values.values {
				result.Observe(v.value.Load(), v.tags...)
			}
		},
	)

	return values
}

func (o *otelGaugeCache) Set(name string, tags map[string]string, value float64) {
	values := o.getGaugeValues(name)
	values.Update(tags, value)
}

func newGaugeValues(name string) *gaugeValues {
	return &gaugeValues{
		lock:            sync.RWMutex{},
		gaugeMetricName: name,
		values:          make(map[string]*gaugeValue),
	}
}

var KVDelimiter = ","
var KVPairsDelimiter = ";"

// Estimate if reducing memalloc here can be beneficial.
func buildCacheKey(tags map[string]string) string {
	keys := make([]string, len(tags))
	i := 0
	for k, _ := range tags {
		keys[i] = k
	}

	sort.Strings(keys)

	sb := strings.Builder{}

	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteString(KVDelimiter)
		sb.WriteString(tags[k])
		sb.WriteString(KVPairsDelimiter)
	}

	return sb.String()
}
