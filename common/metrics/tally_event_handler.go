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

	"github.com/uber-go/tally/v4"
	"golang.org/x/exp/event"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type TallyMetricHandler struct {
	scope          tally.Scope
	l              log.Logger
	excludeTags    map[string]map[string]struct{}
	perUnitBuckets map[MetricUnit]tally.Buckets
}

type tallyRecordFunc func(event.Label)

var _ MetricHandler = (*TallyMetricHandler)(nil)

// NewTallyMetricHandler creates a new tally event.Handler.
func NewTallyMetricHandler(log log.Logger, scope tally.Scope, cfg ClientConfig, perUnitHistogramBoundaries map[string][]float64) *TallyMetricHandler {
	perUnitBuckets := make(map[MetricUnit]tally.Buckets)
	for unit, boundariesList := range perUnitHistogramBoundaries {
		perUnitBuckets[MetricUnit(unit)] = tally.ValueBuckets(boundariesList)
	}

	return &TallyMetricHandler{
		l:              log,
		scope:          scope,
		perUnitBuckets: perUnitBuckets,
		excludeTags:    configExcludeTags(cfg),
	}
}

func (t TallyMetricHandler) Event(ctx context.Context, e *event.Event) context.Context {
	if e.Kind != event.MetricKind {
		return ctx
	}

	mi, ok := event.MetricKey.Find(e)
	if !ok {
		t.l.Fatal("no metric key for metric event", tag.NewAnyTag("event", e))
	}

	em := mi.(event.Metric)
	lval := e.Find(event.MetricVal)
	if !lval.HasValue() {
		t.l.Fatal("no metric value for metric event", tag.NewAnyTag("event", e))
	}

	rf := t.getRecordFunc(em, t.labelsToMap(e.Labels))
	if rf == nil {
		t.l.Fatal("unable to record for metric", tag.NewAnyTag("event", e))
	}

	rf(lval)
	return ctx
}

func (t TallyMetricHandler) getRecordFunc(em event.Metric, tags map[string]string) tallyRecordFunc {
	name := em.Name()
	unit := em.Options().Unit
	switch em.(type) {
	case *event.Counter:
		c := t.scope.Tagged(tags).Counter(name)
		return func(l event.Label) {
			c.Inc(l.Int64())
		}

	case *event.FloatGauge:
		g := t.scope.Tagged(tags).Gauge(name)
		return func(l event.Label) {
			g.Update(l.Float64())
		}

	case *event.DurationDistribution:
		r := t.scope.Tagged(tags).Timer(name)
		return func(l event.Label) {
			r.Record(l.Duration())
		}

	case *event.IntDistribution:
		r := t.scope.Tagged(tags).Histogram(name, t.perUnitBuckets[MetricUnit(unit)])
		return func(l event.Label) {
			r.RecordValue(float64(l.Int64()))
		}

	default:
		return nil
	}
}

func (t TallyMetricHandler) labelsToMap(attrs []event.Label) map[string]string {
	tags := make(map[string]string)
	for _, l := range attrs {
		if vals, ok := t.excludeTags[l.Name]; ok {
			if _, ok := vals[l.String()]; ok {
				tags[l.Name] = tagExcludedValue
				continue
			}
		}

		if l.Name == string(event.MetricKey) || l.Name == string(event.MetricVal) {
			continue
		}
		tags[l.Name] = l.String()
	}
	return tags
}

func (t TallyMetricHandler) Stop(logger log.Logger) {
	// noop
}
