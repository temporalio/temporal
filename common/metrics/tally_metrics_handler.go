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
	"time"

	"github.com/uber-go/tally/v4"

	"go.temporal.io/server/common/log"
)

var sanitizer = tally.NewSanitizer(tally.SanitizeOptions{
	NameCharacters:       tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	KeyCharacters:        tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	ValueCharacters:      tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	ReplacementCharacter: '_',
})

type (
	excludeTags map[string]map[string]struct{}

	tallyMetricsHandler struct {
		scope          tally.Scope
		perUnitBuckets map[MetricUnit]tally.Buckets
		excludeTags    excludeTags
	}
)

var _ Handler = (*tallyMetricsHandler)(nil)

func NewTallyMetricsHandler(cfg ClientConfig, scope tally.Scope) *tallyMetricsHandler {
	perUnitBuckets := make(map[MetricUnit]tally.Buckets)

	for unit, boundariesList := range cfg.PerUnitHistogramBoundaries {
		perUnitBuckets[MetricUnit(unit)] = tally.ValueBuckets(boundariesList)
	}

	return &tallyMetricsHandler{
		scope:          scope,
		perUnitBuckets: perUnitBuckets,
		excludeTags:    configExcludeTags(cfg),
	}
}

// WithTags creates a new MetricProvder with provided []Tag
// Tags are merged with registered Tags from the source MetricsHandler
func (tmp *tallyMetricsHandler) WithTags(tags ...Tag) Handler {
	return &tallyMetricsHandler{
		scope:          tmp.scope.Tagged(tagsToMap(tags, tmp.excludeTags)),
		perUnitBuckets: tmp.perUnitBuckets,
		excludeTags:    tmp.excludeTags,
	}
}

// Counter obtains a counter for the given name.
func (tmp *tallyMetricsHandler) Counter(counter string) CounterIface {
	return CounterFunc(func(i int64, t ...Tag) {
		scope := tmp.scope
		if len(t) > 0 {
			scope = tmp.scope.Tagged(tagsToMap(t, tmp.excludeTags))
		}
		scope.Counter(counter).Inc(i)
	})
}

// Gauge obtains a gauge for the given name.
func (tmp *tallyMetricsHandler) Gauge(gauge string) GaugeIface {
	return GaugeFunc(func(f float64, t ...Tag) {
		scope := tmp.scope
		if len(t) > 0 {
			scope = tmp.scope.Tagged(tagsToMap(t, tmp.excludeTags))
		}
		scope.Gauge(gauge).Update(f)
	})
}

// Timer obtains a timer for the given name.
func (tmp *tallyMetricsHandler) Timer(timer string) TimerIface {
	return TimerFunc(func(d time.Duration, t ...Tag) {
		scope := tmp.scope
		if len(t) > 0 {
			scope = tmp.scope.Tagged(tagsToMap(t, tmp.excludeTags))
		}
		scope.Timer(timer).Record(d)
	})
}

// Histogram obtains a histogram for the given name.
func (tmp *tallyMetricsHandler) Histogram(histogram string, unit MetricUnit) HistogramIface {
	return HistogramFunc(func(i int64, t ...Tag) {
		scope := tmp.scope
		if len(t) > 0 {
			scope = tmp.scope.Tagged(tagsToMap(t, tmp.excludeTags))
		}
		scope.Histogram(histogram, tmp.perUnitBuckets[unit]).RecordValue(float64(i))
	})
}

func (*tallyMetricsHandler) Stop(log.Logger) {}

func tagsToMap(t1 []Tag, e excludeTags) map[string]string {
	if len(t1) == 0 {
		return nil
	}

	m := make(map[string]string, len(t1))

	convert := func(tag Tag) {
		if vals, ok := e[tag.Key()]; ok {
			if _, ok := vals[tag.Value()]; !ok {
				m[tag.Key()] = tagExcludedValue
				return
			}
		}

		m[tag.Key()] = tag.Value()
	}

	for i := range t1 {
		convert(t1[i])
	}

	return m
}
