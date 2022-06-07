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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/instrument"
	otelunit "go.opentelemetry.io/otel/metric/unit"
	"golang.org/x/exp/event"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// MetricHandler is an event.Handler for OpenTelemetry metrics.
// Its Event method handles Metric events and ignores all others.
type OtelMetricHandler struct {
	provider    OpenTelemetryProvider
	l           log.Logger
	excludeTags map[string]map[string]struct{}
}

type recordFunc func(context.Context, event.Label, []attribute.KeyValue)

var _ MetricHandler = (*OtelMetricHandler)(nil)

// NewOtelMetricHandler creates a new open telemetry MetricHandler.
func NewOtelMetricHandler(l log.Logger, o OpenTelemetryProvider, cfg ClientConfig) *OtelMetricHandler {
	return &OtelMetricHandler{
		provider:    o,
		l:           l,
		excludeTags: configExcludeTags(cfg),
	}
}

func (m OtelMetricHandler) Event(ctx context.Context, e *event.Event) context.Context {
	if e.Kind != event.MetricKind {
		return ctx
	}

	mi, ok := event.MetricKey.Find(e)
	if !ok {
		m.l.Fatal("no metric key for metric event", tag.NewAnyTag("event", e))
	}

	em := mi.(event.Metric)
	lval := e.Find(event.MetricVal)
	if !lval.HasValue() {
		m.l.Fatal("no metric value for metric event", tag.NewAnyTag("event", e))
	}

	rf := m.getRecordFunc(em)
	if rf == nil {
		m.l.Fatal("unable to record for metric", tag.NewAnyTag("event", e))
	}

	rf(ctx, lval, m.labelsToAttributes(e.Labels))
	return ctx
}

func (m OtelMetricHandler) getRecordFunc(em event.Metric) recordFunc {
	opts := em.Options()
	name := em.Name()
	otelOpts := []instrument.Option{
		instrument.WithDescription(opts.Description),
		instrument.WithUnit(otelunit.Unit(opts.Unit)), // cast OK: same strings
	}
	switch em.(type) {
	case *event.Counter:
		c, err := m.provider.GetMeter().SyncInt64().Counter(name, otelOpts...)
		if err != nil {
			m.l.Fatal("unable to get new recording function", tag.NewAnyTag("MetricType", fmt.Sprintf("%T", em)), tag.Error(err))
			return nil
		}

		return func(ctx context.Context, l event.Label, attrs []attribute.KeyValue) {
			c.Add(ctx, l.Int64(), attrs...)
		}

	case *event.FloatGauge:
		g, err := m.provider.GetMeter().SyncFloat64().UpDownCounter(name, otelOpts...)
		if err != nil {
			m.l.Fatal("unable to get new recording function", tag.NewAnyTag("MetricType", fmt.Sprintf("%T", em)), tag.Error(err))
			return nil
		}

		return func(ctx context.Context, l event.Label, attrs []attribute.KeyValue) {
			g.Add(ctx, l.Float64(), attrs...)
		}

	case *event.DurationDistribution:
		r, err := m.provider.GetMeter().SyncInt64().Histogram(name, otelOpts...)
		if err != nil {
			m.l.Fatal("unable to get new recording function", tag.NewAnyTag("MetricType", fmt.Sprintf("%T", em)), tag.Error(err))
		}

		return func(ctx context.Context, l event.Label, attrs []attribute.KeyValue) {
			r.Record(ctx, l.Duration().Nanoseconds(), attrs...)
		}

	case *event.IntDistribution:
		r, err := m.provider.GetMeter().SyncInt64().Histogram(name, otelOpts...)
		if err != nil {
			m.l.Fatal("unable to get new recording function", tag.NewAnyTag("MetricType", fmt.Sprintf("%T", em)), tag.Error(err))
			return nil
		}

		return func(ctx context.Context, l event.Label, attrs []attribute.KeyValue) {
			r.Record(ctx, l.Int64(), attrs...)
		}

	default:
		return nil
	}
}

func (m OtelMetricHandler) labelsToAttributes(ls []event.Label) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	for _, l := range ls {
		if vals, ok := m.excludeTags[l.Name]; ok {
			if _, ok := vals[l.String()]; ok {
				attrs = append(attrs, attribute.String(l.Name, tagExcludedValue))
				continue
			}
		}

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
	default:
		return attribute.String(l.Name, l.String())
	}
}

func (m OtelMetricHandler) Stop(logger log.Logger) {
	m.provider.Stop(logger)
}

func configExcludeTags(cfg ClientConfig) map[string]map[string]struct{} {
	tagsToFilter := make(map[string]map[string]struct{})
	for key, val := range cfg.ExcludeTags {
		exclusions := make(map[string]struct{})
		for _, val := range val {
			exclusions[val] = struct{}{}
		}
		tagsToFilter[key] = exclusions
	}
	return tagsToFilter
}
