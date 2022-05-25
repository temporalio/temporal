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
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	otelunit "go.opentelemetry.io/otel/metric/unit"
	"golang.org/x/exp/event"
)

// MetricHandler is an event.Handler for OpenTelemetry metrics.
// Its Event method handles Metric events and ignores all others.
type OtelMetricHandler struct {
	meter metric.Meter
	mu    sync.Mutex
	// A map from event.Metrics to, effectively, otel Meters.
	// But since the only thing we need from the Meter is recording a value, we
	// use a function for that that closes over the Meter itself.
	recordFuncs map[event.Metric]recordFunc
}

type recordFunc func(context.Context, event.Label, []attribute.KeyValue)

var _ event.Handler = (*OtelMetricHandler)(nil)

// NewOtelMetricHandler creates a new open telemetry MetricHandler.
func NewOtelMetricHandler(m metric.Meter) *OtelMetricHandler {
	return &OtelMetricHandler{
		meter:       m,
		recordFuncs: map[event.Metric]recordFunc{},
	}
}

func (m *OtelMetricHandler) Event(ctx context.Context, e *event.Event) context.Context {
	if e.Kind != event.MetricKind {
		return ctx
	}
	// Get the otel instrument corresponding to the event's MetricDescriptor,
	// or create a new one.
	mi, ok := event.MetricKey.Find(e)
	if !ok {
		panic(errors.New("no metric key for metric event"))
	}
	em := mi.(event.Metric)
	lval := e.Find(event.MetricVal)
	if !lval.HasValue() {
		panic(errors.New("no metric value for metric event"))
	}
	rf := m.getRecordFunc(em)
	if rf == nil {
		panic(fmt.Errorf("unable to record for metric %v", em))
	}
	rf(ctx, lval, labelsToAttributes(e.Labels))
	return ctx
}

func (m *OtelMetricHandler) getRecordFunc(em event.Metric) recordFunc {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f, ok := m.recordFuncs[em]; ok {
		return f
	}
	f := m.newRecordFunc(em)
	m.recordFuncs[em] = f
	return f
}

func (m *OtelMetricHandler) newRecordFunc(em event.Metric) recordFunc {
	opts := em.Options()
	name := opts.Namespace + "/" + em.Name()
	otelOpts := []instrument.Option{
		instrument.WithDescription(opts.Description),
		instrument.WithUnit(otelunit.Unit(opts.Unit)), // cast OK: same strings
	}
	switch em.(type) {
	case *event.Counter:
		c, _ := m.meter.SyncInt64().Counter(name, otelOpts...)
		return func(ctx context.Context, l event.Label, attrs []attribute.KeyValue) {
			c.Add(ctx, l.Int64(), attrs...)
		}

	case *event.FloatGauge:
		g, _ := m.meter.SyncFloat64().UpDownCounter(name, otelOpts...)
		return func(ctx context.Context, l event.Label, attrs []attribute.KeyValue) {
			g.Add(ctx, l.Float64(), attrs...)
		}

	case *event.DurationDistribution:
		r, _ := m.meter.SyncInt64().Histogram(name, otelOpts...)
		return func(ctx context.Context, l event.Label, attrs []attribute.KeyValue) {
			r.Record(ctx, l.Duration().Nanoseconds(), attrs...)
		}

	case *event.IntDistribution:
		r, _ := m.meter.SyncInt64().Histogram(name, otelOpts...)
		return func(ctx context.Context, l event.Label, attrs []attribute.KeyValue) {
			r.Record(ctx, l.Int64(), attrs...)
		}

	default:
		return nil
	}
}

func labelsToAttributes(ls []event.Label) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	for _, l := range ls {
		if l.Name == string(event.MetricKey) || l.Name == string(event.MetricVal) {
			continue
		}
		attrs = append(attrs, labelToAttribute(l))
	}
	return attrs
}

func labelToAttribute(l event.Label) attribute.KeyValue {
	switch {
	case l.IsString():
		return attribute.String(l.Name, l.String())
	case l.IsInt64():
		return attribute.Int64(l.Name, l.Int64())
	case l.IsFloat64():
		return attribute.Float64(l.Name, l.Float64())
	case l.IsBool():
		return attribute.Bool(l.Name, l.Bool())
	default: // including uint64
		panic(fmt.Errorf("cannot convert label value of type %T to attribute.KeyValue", l.Interface()))
	}
}
