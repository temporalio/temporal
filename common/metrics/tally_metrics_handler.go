package metrics

import (
	"sync"
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
		childCache     sync.Map // tagsCacheKey(tags) -> *tallyMetricsHandler
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

// tagsCacheKey builds a compact string key from a tag slice for use as a
// sync.Map lookup key.
func tagsCacheKey(tags []Tag) string {
	if len(tags) == 1 {
		return tags[0].Key + "\x00" + tags[0].Value
	}
	size := len(tags) - 1 // separators between pairs
	for i := range tags {
		size += len(tags[i].Key) + 1 + len(tags[i].Value)
	}
	b := make([]byte, 0, size)
	for i, t := range tags {
		if i > 0 {
			b = append(b, 0)
		}
		b = append(b, t.Key...)
		b = append(b, 0)
		b = append(b, t.Value...)
	}
	return string(b)
}

// WithTags creates a new MetricProvider with provided []Tag
// Tags are merged with registered Tags from the source MetricsHandler.
// Handlers are cached by tag combination so repeated calls avoid allocations.
func (tmh *tallyMetricsHandler) WithTags(tags ...Tag) Handler {
	if len(tags) == 0 {
		return tmh
	}
	key := tagsCacheKey(normalizeTagsForCaching(tags, tmh.excludeTags))
	if v, ok := tmh.childCache.Load(key); ok {
		return v.(*tallyMetricsHandler)
	}
	child := &tallyMetricsHandler{
		scope:          tmh.scope.Tagged(tagsToMap(tags, tmh.excludeTags)),
		perUnitBuckets: tmh.perUnitBuckets,
		excludeTags:    tmh.excludeTags,
	}
	actual, _ := tmh.childCache.LoadOrStore(key, child)
	return actual.(*tallyMetricsHandler)
}

// normalizeTagsForCaching applies excludeTags substitution to produce
// canonical tag values for cache key computation. Returns the original slice
// unchanged if no tags need normalization (zero-alloc fast path).
func normalizeTagsForCaching(tags []Tag, excl excludeTags) []Tag {
	if len(excl) == 0 {
		return tags
	}
	var normalized []Tag
	for i, t := range tags {
		if vals, ok := excl[t.Key]; ok {
			if _, ok := vals[t.Value]; !ok {
				if normalized == nil {
					normalized = make([]Tag, len(tags))
					copy(normalized, tags[:i])
				}
				normalized[i] = Tag{Key: t.Key, Value: tagExcludedValue}
				continue
			}
		}
		if normalized != nil {
			normalized[i] = t
		}
	}
	if normalized != nil {
		return normalized
	}
	return tags
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
