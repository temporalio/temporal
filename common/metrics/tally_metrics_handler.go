package metrics

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
)

// scopeCacheMaxSize is the approximate upper bound on cached scope entries.
// The bound may be slightly exceeded under high concurrency due to
// check-then-store races, which is acceptable.
const scopeCacheMaxSize = 1024

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
		childCacheSize atomic.Int64
		scopeCache     sync.Map // tagsCacheKey(normalized tags) -> tally.Scope
		scopeCacheSize atomic.Int64
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
	size := 0
	for i := range tags {
		size += len(tags[i].Key) + 1 + len(tags[i].Value) + 1
	}
	var sb strings.Builder
	sb.Grow(size)
	for _, t := range tags {
		sb.WriteString(t.Key)
		sb.WriteByte(0)
		sb.WriteString(t.Value)
		sb.WriteByte(0)
	}
	return sb.String()
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
	if tmh.childCacheSize.Load() >= scopeCacheMaxSize {
		return child
	}
	actual, loaded := tmh.childCache.LoadOrStore(key, child)
	if !loaded {
		tmh.childCacheSize.Add(1)
	}
	return actual.(*tallyMetricsHandler)
}

// cachedTaggedScope returns a tally.Scope tagged with the given tags, caching
// the result so that repeated calls with the same tag combination avoid
// allocating a new map and tally scope lookup. Tags are normalized through
// excludeTags before cache key computation so that different raw values which
// map to the same excluded placeholder share a single cache entry. The cache
// is bounded to scopeCacheMaxSize entries; beyond that, scopes are created
// but not cached.
func (tmh *tallyMetricsHandler) cachedTaggedScope(tags []Tag) tally.Scope {
	if len(tags) == 0 {
		return tmh.scope
	}
	key := tagsCacheKey(normalizeTagsForCaching(tags, tmh.excludeTags))
	if v, ok := tmh.scopeCache.Load(key); ok {
		return v.(tally.Scope)
	}
	scope := tmh.scope.Tagged(tagsToMap(tags, tmh.excludeTags))
	if tmh.scopeCacheSize.Load() >= scopeCacheMaxSize {
		return scope
	}
	actual, loaded := tmh.scopeCache.LoadOrStore(key, scope)
	if !loaded {
		tmh.scopeCacheSize.Add(1)
	}
	return actual.(tally.Scope)
}

// normalizeTag applies excludeTags substitution to a single tag.
// Returns the (possibly modified) tag and whether it was normalized.
func normalizeTag(t Tag, excl excludeTags) (Tag, bool) {
	if vals, ok := excl[t.Key]; ok {
		if _, ok := vals[t.Value]; !ok {
			return Tag{Key: t.Key, Value: tagExcludedValue}, true
		}
	}
	return t, false
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
		nt, changed := normalizeTag(t, excl)
		if changed && normalized == nil {
			normalized = slices.Clone(tags)
		}
		if normalized != nil {
			normalized[i] = nt
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
		tmh.cachedTaggedScope(t).Counter(counter).Inc(i)
	})
}

// Gauge obtains a gauge for the given name.
func (tmh *tallyMetricsHandler) Gauge(gauge string) GaugeIface {
	return GaugeFunc(func(f float64, t ...Tag) {
		tmh.cachedTaggedScope(t).Gauge(gauge).Update(f)
	})
}

// Timer obtains a timer for the given name.
func (tmh *tallyMetricsHandler) Timer(timer string) TimerIface {
	return TimerFunc(func(d time.Duration, t ...Tag) {
		tmh.cachedTaggedScope(t).Timer(timer).Record(d)
	})
}

// Histogram obtains a histogram for the given name.
func (tmh *tallyMetricsHandler) Histogram(histogram string, unit MetricUnit) HistogramIface {
	return HistogramFunc(func(i int64, t ...Tag) {
		tmh.cachedTaggedScope(t).Histogram(histogram, tmh.perUnitBuckets[unit]).RecordValue(float64(i))
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
	for i := range t1 {
		nt, _ := normalizeTag(t1[i], e)
		m[nt.Key] = nt.Value
	}
	return m
}
