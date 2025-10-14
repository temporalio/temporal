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
func (tmh *tallyMetricsHandler) WithTags(tags ...Tag) Handler {
	return &tallyMetricsHandler{
		scope:          tmh.scope.Tagged(tagsToMap(tags, tmh.excludeTags)),
		perUnitBuckets: tmh.perUnitBuckets,
		excludeTags:    tmh.excludeTags,
	}
}

// Counter obtains a counter for the given name.
func (tmh *tallyMetricsHandler) Counter(counter string) CounterIface {
	return CounterFunc(func(i int64, t ...Tag) {
		scope := tmh.scope
		if len(t) > 0 {
			scope = tmh.scope.Tagged(tagsToMap(t, tmh.excludeTags))
		}
		scope.Counter(counter).Inc(i)
	})
}

// Gauge obtains a gauge for the given name.
func (tmh *tallyMetricsHandler) Gauge(gauge string) GaugeIface {
	return GaugeFunc(func(f float64, t ...Tag) {
		scope := tmh.scope
		if len(t) > 0 {
			scope = tmh.scope.Tagged(tagsToMap(t, tmh.excludeTags))
		}
		scope.Gauge(gauge).Update(f)
	})
}

// Timer obtains a timer for the given name.
func (tmh *tallyMetricsHandler) Timer(timer string) TimerIface {
	return TimerFunc(func(d time.Duration, t ...Tag) {
		scope := tmh.scope
		if len(t) > 0 {
			scope = tmh.scope.Tagged(tagsToMap(t, tmh.excludeTags))
		}
		scope.Timer(timer).Record(d)
	})
}

// Histogram obtains a histogram for the given name.
func (tmh *tallyMetricsHandler) Histogram(histogram string, unit MetricUnit) HistogramIface {
	return HistogramFunc(func(i int64, t ...Tag) {
		scope := tmh.scope
		if len(t) > 0 {
			scope = tmh.scope.Tagged(tagsToMap(t, tmh.excludeTags))
		}
		scope.Histogram(histogram, tmh.perUnitBuckets[unit]).RecordValue(float64(i))
	})
}

func (*tallyMetricsHandler) Stop(log.Logger) {}

func (*tallyMetricsHandler) Close() error {
	return nil
}

func (tmh *tallyMetricsHandler) StartBatch(_ string) BatchHandler {
	return tmh
}

func tagsToMap(t1 []Tag, e excludeTags) map[string]string {
	if len(t1) == 0 {
		return nil
	}

	m := make(map[string]string, len(t1))

	convert := func(tag Tag) {
		if vals, ok := e[tag.Key]; ok {
			if _, ok := vals[tag.Value]; !ok {
				m[tag.Key] = tagExcludedValue
				return
			}
		}

		m[tag.Key] = tag.Value
	}

	for i := range t1 {
		convert(t1[i])
	}

	return m
}
