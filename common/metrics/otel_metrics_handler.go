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
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// otelMetricsHandler is an adapter around an OpenTelemetry [metric.Meter] that implements the [Handler] interface.
type (
	otelMetricsHandler struct {
		l           log.Logger
		set         attribute.Set
		provider    OpenTelemetryProvider
		excludeTags map[string]map[string]struct{}
		catalog     catalog
		gauges      *sync.Map // string -> *gaugeAdapter. note: shared between multiple otelMetricsHandlers
	}

	// This is to work around the lack of synchronous gauge:
	// https://github.com/open-telemetry/opentelemetry-specification/issues/2318
	// Basically, otel gauges only support getting a value with a callback, they can't store a
	// value for us. So we have to store it ourselves and supply it to a callback.
	gaugeAdapter struct {
		lock   sync.Mutex
		values map[attribute.Distinct]gaugeValue
	}
	gaugeValue struct {
		value float64
		// In practice, we can use attribute.Set itself as the map key in gaugeAdapter and it
		// works, but according to the API we should use attribute.Distinct. But we can't get a
		// Set back from a Distinct, so we have to store the set here also.
		set attribute.Set
	}
	gaugeAdapterGauge struct {
		omp     *otelMetricsHandler
		adapter *gaugeAdapter
	}
)

var _ Handler = (*otelMetricsHandler)(nil)

// NewOtelMetricsHandler returns a new Handler that uses the provided OpenTelemetry [metric.Meter] to record metrics.
// This OTel handler supports metric descriptions for metrics registered with the New*Def functions. However, those
// functions must be called before this constructor. Otherwise, the descriptions will be empty. This is because the
// OTel metric descriptions are generated from the globalRegistry. You may also record metrics that are not registered
// via the New*Def functions. In that case, the metric description will be the OTel default (the metric name itself).
func NewOtelMetricsHandler(
	l log.Logger,
	o OpenTelemetryProvider,
	cfg ClientConfig,
) (*otelMetricsHandler, error) {
	c, err := globalRegistry.buildCatalog()
	if err != nil {
		return nil, fmt.Errorf("failed to build metrics catalog: %w", err)
	}
	return &otelMetricsHandler{
		l:           l,
		set:         makeInitialSet(cfg.Tags),
		provider:    o,
		excludeTags: configExcludeTags(cfg),
		catalog:     c,
		gauges:      new(sync.Map),
	}, nil
}

// WithTags creates a new Handler with the provided Tag list.
// Tags are merged with the existing tags.
func (omp *otelMetricsHandler) WithTags(tags ...Tag) Handler {
	newHandler := *omp
	newHandler.set = newHandler.makeSet(tags)
	return &newHandler
}

// Counter obtains a counter for the given name.
func (omp *otelMetricsHandler) Counter(counter string) CounterIface {
	opts := addOptions(omp, counterOptions{}, counter)
	c, err := omp.provider.GetMeter().Int64Counter(counter, opts...)
	if err != nil {
		omp.l.Error("error getting metric", tag.NewStringTag("MetricName", counter), tag.Error(err))
		return CounterFunc(func(i int64, t ...Tag) {})
	}

	return CounterFunc(func(i int64, t ...Tag) {
		option := metric.WithAttributeSet(omp.makeSet(t))
		c.Add(context.Background(), i, option)
	})
}

func (omp *otelMetricsHandler) getGaugeAdapter(gauge string) (*gaugeAdapter, error) {
	if v, ok := omp.gauges.Load(gauge); ok {
		return v.(*gaugeAdapter), nil
	}
	adapter := &gaugeAdapter{
		values: make(map[attribute.Distinct]gaugeValue),
	}
	if v, wasLoaded := omp.gauges.LoadOrStore(gauge, adapter); wasLoaded {
		return v.(*gaugeAdapter), nil
	}

	opts := addOptions(omp, gaugeOptions{
		metric.WithFloat64Callback(adapter.callback),
	}, gauge)
	// Register the gauge with otel. It will call our callback when it wants to read the values.
	_, err := omp.provider.GetMeter().Float64ObservableGauge(gauge, opts...)
	if err != nil {
		omp.gauges.Delete(gauge)
		omp.l.Error("error getting metric", tag.NewStringTag("MetricName", gauge), tag.Error(err))
		return nil, err
	}

	return adapter, nil
}

// Gauge obtains a gauge for the given name.
func (omp *otelMetricsHandler) Gauge(gauge string) GaugeIface {
	adapter, err := omp.getGaugeAdapter(gauge)
	if err != nil {
		return GaugeFunc(func(i float64, t ...Tag) {})
	}
	return &gaugeAdapterGauge{
		omp:     omp,
		adapter: adapter,
	}
}

func (a *gaugeAdapter) callback(ctx context.Context, o metric.Float64Observer) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	for _, v := range a.values {
		o.Observe(v.value, metric.WithAttributeSet(v.set))
	}
	return nil
}

func (g *gaugeAdapterGauge) Record(v float64, tags ...Tag) {
	set := g.omp.makeSet(tags)
	g.adapter.lock.Lock()
	defer g.adapter.lock.Unlock()
	g.adapter.values[set.Equivalent()] = gaugeValue{value: v, set: set}
}

// Timer obtains a timer for the given name.
func (omp *otelMetricsHandler) Timer(timer string) TimerIface {
	opts := addOptions(omp, histogramOptions{metric.WithUnit(Milliseconds)}, timer)
	c, err := omp.provider.GetMeter().Int64Histogram(timer, opts...)
	if err != nil {
		omp.l.Error("error getting metric", tag.NewStringTag("MetricName", timer), tag.Error(err))
		return TimerFunc(func(i time.Duration, t ...Tag) {})
	}

	return TimerFunc(func(i time.Duration, t ...Tag) {
		option := metric.WithAttributeSet(omp.makeSet(t))
		c.Record(context.Background(), i.Milliseconds(), option)
	})
}

// Histogram obtains a histogram for the given name.
func (omp *otelMetricsHandler) Histogram(histogram string, unit MetricUnit) HistogramIface {
	opts := addOptions(omp, histogramOptions{metric.WithUnit(string(unit))}, histogram)
	c, err := omp.provider.GetMeter().Int64Histogram(histogram, opts...)
	if err != nil {
		omp.l.Error("error getting metric", tag.NewStringTag("MetricName", histogram), tag.Error(err))
		return HistogramFunc(func(i int64, t ...Tag) {})
	}

	return HistogramFunc(func(i int64, t ...Tag) {
		option := metric.WithAttributeSet(omp.makeSet(t))
		c.Record(context.Background(), i, option)
	})
}

func (omp *otelMetricsHandler) Stop(l log.Logger) {
	omp.provider.Stop(l)
}

// makeSet returns an otel attribute.Set with the given tags merged with the
// otelMetricsHandler's tags.
func (omp *otelMetricsHandler) makeSet(tags []Tag) attribute.Set {
	if len(tags) == 0 {
		return omp.set
	}
	attrs := make([]attribute.KeyValue, 0, omp.set.Len()+len(tags))
	for i := omp.set.Iter(); i.Next(); {
		attrs = append(attrs, i.Attribute())
	}
	for _, t := range tags {
		attrs = append(attrs, omp.convertTag(t))
	}
	return attribute.NewSet(attrs...)
}

func (omp *otelMetricsHandler) convertTag(tag Tag) attribute.KeyValue {
	if vals, ok := omp.excludeTags[tag.Key()]; ok {
		if _, ok := vals[tag.Value()]; !ok {
			return attribute.String(tag.Key(), tagExcludedValue)
		}
	}
	return attribute.String(tag.Key(), tag.Value())
}

func makeInitialSet(tags map[string]string) attribute.Set {
	if len(tags) == 0 {
		return *attribute.EmptySet()
	}
	var attrs []attribute.KeyValue
	for k, v := range tags {
		attrs = append(attrs, attribute.String(k, v))
	}
	return attribute.NewSet(attrs...)
}
