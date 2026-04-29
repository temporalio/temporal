package metrics

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

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
		childCache     sync.Map // tagsCacheKey(normalized tags) -> *tallyMetricsHandler
		childCacheSize atomic.Int64
		scopeCache     sync.Map // tagsCacheKey(normalized tags) -> tally.Scope
		scopeCacheSize atomic.Int64
		counterCache   sync.Map // metric name -> CounterIface
		gaugeCache     sync.Map // metric name -> GaugeIface
		timerCache     sync.Map // metric name -> TimerIface
		histogramCache sync.Map // metric name + unit -> HistogramIface
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

// appendCacheKey appends a compact cache key for the given tags to buf and
// returns the extended buffer.
func appendCacheKey(buf []byte, tags []Tag) []byte {
	for i, t := range tags {
		if i > 0 {
			buf = append(buf, 0)
		}
		buf = append(buf, t.Key...)
		buf = append(buf, 0)
		buf = append(buf, t.Value...)
	}
	return buf
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
	b = appendCacheKey(b, tags)
	return string(b)
}

// tempString creates a temporary string from a byte slice without allocation.
// The returned string is only valid as long as the byte slice is not modified.
// This is safe for use as a sync.Map lookup key because Load does not retain
// the key after returning.
func tempString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// WithTags creates a new MetricProvider with provided []Tag
// Tags are merged with registered Tags from the source MetricsHandler.
// Handlers are cached by normalized tag combination so that excluded tags
// with different raw values share a single cache entry. The cache is bounded
// to scopeCacheMaxSize entries.
func (tmh *tallyMetricsHandler) WithTags(tags ...Tag) Handler {
	if len(tags) == 0 {
		return tmh
	}
	normalized := normalizeTagsForCaching(tags, tmh.excludeTags)
	var scratch [128]byte
	buf := appendCacheKey(scratch[:0], normalized)
	if v, ok := tmh.childCache.Load(tempString(buf)); ok {
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
	key := string(buf)
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
	normalized := normalizeTagsForCaching(tags, tmh.excludeTags)
	var scratch [128]byte
	buf := appendCacheKey(scratch[:0], normalized)
	if v, ok := tmh.scopeCache.Load(tempString(buf)); ok {
		return v.(tally.Scope)
	}
	scope := tmh.scope.Tagged(tagsToMap(tags, tmh.excludeTags))
	if tmh.scopeCacheSize.Load() >= scopeCacheMaxSize {
		return scope
	}
	key := string(buf)
	actual, loaded := tmh.scopeCache.LoadOrStore(key, scope)
	if !loaded {
		tmh.scopeCacheSize.Add(1)
	}
	return actual.(tally.Scope)
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
	if v, ok := tmh.counterCache.Load(counter); ok {
		return v.(CounterIface)
	}
	fn := CounterFunc(func(i int64, t ...Tag) {
		tmh.cachedTaggedScope(t).Counter(counter).Inc(i)
	})
	actual, _ := tmh.counterCache.LoadOrStore(counter, fn)
	return actual.(CounterIface)
}

// Gauge obtains a gauge for the given name.
func (tmh *tallyMetricsHandler) Gauge(gauge string) GaugeIface {
	if v, ok := tmh.gaugeCache.Load(gauge); ok {
		return v.(GaugeIface)
	}
	fn := GaugeFunc(func(f float64, t ...Tag) {
		tmh.cachedTaggedScope(t).Gauge(gauge).Update(f)
	})
	actual, _ := tmh.gaugeCache.LoadOrStore(gauge, fn)
	return actual.(GaugeIface)
}

// Timer obtains a timer for the given name.
func (tmh *tallyMetricsHandler) Timer(timer string) TimerIface {
	if v, ok := tmh.timerCache.Load(timer); ok {
		return v.(TimerIface)
	}
	fn := TimerFunc(func(d time.Duration, t ...Tag) {
		tmh.cachedTaggedScope(t).Timer(timer).Record(d)
	})
	actual, _ := tmh.timerCache.LoadOrStore(timer, fn)
	return actual.(TimerIface)
}

// Histogram obtains a histogram for the given name.
func (tmh *tallyMetricsHandler) Histogram(histogram string, unit MetricUnit) HistogramIface {
	key := histogram + "\x00" + string(unit)
	if v, ok := tmh.histogramCache.Load(key); ok {
		return v.(HistogramIface)
	}
	fn := HistogramFunc(func(i int64, t ...Tag) {
		tmh.cachedTaggedScope(t).Histogram(histogram, tmh.perUnitBuckets[unit]).RecordValue(float64(i))
	})
	actual, _ := tmh.histogramCache.LoadOrStore(key, fn)
	return actual.(HistogramIface)
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
