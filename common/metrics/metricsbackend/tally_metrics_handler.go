package metricsbackend

import (
	"encoding/binary"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

// defaultTagsCacheMaxSize is the default upper bound on cached scope/handler entries.
const defaultTagsCacheMaxSize = 10000

type histogramCacheKey struct {
	name string
	unit metrics.MetricUnit
}

var sanitizer = tally.NewSanitizer(tally.SanitizeOptions{
	NameCharacters:       tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	KeyCharacters:        tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	ValueCharacters:      tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	ReplacementCharacter: '_',
})

// sharedScopeCache is a bounded cache shared across all tallyMetricsHandler
// instances in a handler tree. When the cache reaches its size limit, all
// entries are cleared (clear-on-overflow) to bound memory usage.
type sharedScopeCache struct {
	scopes   map[string]tally.Scope
	handlers map[string]*tallyMetricsHandler
	mu       sync.RWMutex
	maxSize  int
}

func newSharedScopeCache(maxSize int) *sharedScopeCache {
	return &sharedScopeCache{
		maxSize:  maxSize,
		scopes:   make(map[string]tally.Scope),
		handlers: make(map[string]*tallyMetricsHandler),
	}
}

func (c *sharedScopeCache) loadOrStoreScope(key string, create func() tally.Scope) tally.Scope {
	c.mu.RLock()
	if s, ok := c.scopes[key]; ok {
		c.mu.RUnlock()
		return s
	}
	c.mu.RUnlock()

	s := create()

	c.mu.Lock()
	defer c.mu.Unlock()
	// Double-check: another goroutine may have inserted while we were creating.
	if existing, ok := c.scopes[key]; ok {
		return existing
	}
	if len(c.scopes) >= c.maxSize {
		clear(c.scopes)
	}
	c.scopes[key] = s
	return s
}

func (c *sharedScopeCache) loadOrStoreHandler(key string, create func() *tallyMetricsHandler) *tallyMetricsHandler {
	c.mu.RLock()
	if h, ok := c.handlers[key]; ok {
		c.mu.RUnlock()
		return h
	}
	c.mu.RUnlock()

	h := create()

	c.mu.Lock()
	defer c.mu.Unlock()
	// Double-check: another goroutine may have inserted while we were creating.
	if existing, ok := c.handlers[key]; ok {
		return existing
	}
	if len(c.handlers) >= c.maxSize {
		clear(c.handlers)
	}
	c.handlers[key] = h
	return h
}

type (
	excludeTags map[string]map[string]struct{}

	tallyMetricsHandler struct {
		scope          tally.Scope
		perUnitBuckets map[metrics.MetricUnit]tally.Buckets
		excludeTags    excludeTags
		cache          *sharedScopeCache
		scopeKey       string   // unique prefix for this handler in the shared cache
		counters       sync.Map // metric name -> metrics.CounterIface
		gauges         sync.Map // metric name -> metrics.GaugeIface
		timers         sync.Map // metric name -> metrics.TimerIface
		histograms     sync.Map // metric name + unit -> metrics.HistogramIface
	}
)

var _ metrics.Handler = (*tallyMetricsHandler)(nil)

func NewTallyMetricsHandler(cfg metrics.ClientConfig, scope tally.Scope) *tallyMetricsHandler {
	perUnitBuckets := make(map[metrics.MetricUnit]tally.Buckets)

	for unit, boundariesList := range cfg.PerUnitHistogramBoundaries {
		perUnitBuckets[metrics.MetricUnit(unit)] = tally.ValueBuckets(boundariesList)
	}

	maxSize := cfg.TagsCacheMaxSize
	if maxSize <= 0 {
		maxSize = defaultTagsCacheMaxSize
	}

	return &tallyMetricsHandler{
		scope:          scope,
		perUnitBuckets: perUnitBuckets,
		excludeTags:    configExcludeTags(cfg),
		cache:          newSharedScopeCache(maxSize),
		scopeKey:       "",
	}
}

// tagsCacheKey builds a compact string key from a tag slice for use as a
// map lookup key.
func tagsCacheKey(tags []metrics.Tag) string {
	size := 0
	for i := range tags {
		size += len(tags[i].Key) + len(tags[i].Value) + 2*binary.MaxVarintLen64
	}
	var sb strings.Builder
	sb.Grow(size)
	for _, t := range tags {
		appendCacheKeyPart(&sb, t.Key)
		appendCacheKeyPart(&sb, t.Value)
	}
	return sb.String()
}

func appendCacheKeyPart(sb *strings.Builder, value string) {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(value)))
	_, _ = sb.Write(lenBuf[:n])
	sb.WriteString(value)
}

// WithTags creates a new MetricProvider with provided []Tag
// Tags are merged with registered Tags from the source MetricsHandler.
// Handlers are cached by tag combination so repeated calls avoid allocations.
func (tmh *tallyMetricsHandler) WithTags(tags ...metrics.Tag) metrics.Handler {
	if len(tags) == 0 {
		return tmh
	}
	normalizedKey := tagsCacheKey(normalizeTagsForCaching(tags, tmh.excludeTags))
	key := tmh.scopeKey + normalizedKey
	return tmh.cache.loadOrStoreHandler(key, func() *tallyMetricsHandler {
		return &tallyMetricsHandler{
			scope:          tmh.scope.Tagged(tagsToMap(tags, tmh.excludeTags)),
			perUnitBuckets: tmh.perUnitBuckets,
			excludeTags:    tmh.excludeTags,
			cache:          tmh.cache,
			scopeKey:       key,
		}
	})
}

// cachedTaggedScope returns a tally.Scope tagged with the given tags, caching
// the result so that repeated calls with the same tag combination avoid
// allocating a new map and tally scope lookup. Tags are normalized through
// excludeTags before cache key computation so that different raw values which
// map to the same excluded placeholder share a single cache entry.
func (tmh *tallyMetricsHandler) cachedTaggedScope(tags []metrics.Tag) tally.Scope {
	if len(tags) == 0 {
		return tmh.scope
	}
	key := tmh.scopeKey + tagsCacheKey(normalizeTagsForCaching(tags, tmh.excludeTags))
	return tmh.cache.loadOrStoreScope(key, func() tally.Scope {
		return tmh.scope.Tagged(tagsToMap(tags, tmh.excludeTags))
	})
}

// normalizeTag applies excludeTags substitution to a single tag.
// Returns the (possibly modified) tag and whether it was normalized.
func normalizeTag(t metrics.Tag, excl excludeTags) (metrics.Tag, bool) {
	if vals, ok := excl[t.Key]; ok {
		if _, ok := vals[t.Value]; !ok {
			return metrics.Tag{Key: t.Key, Value: metrics.TagExcludedValue}, true
		}
	}
	return t, false
}

// normalizeTagsForCaching applies excludeTags substitution to produce
// canonical tag values for cache key computation. Returns the original slice
// unchanged if no tags need normalization (zero-alloc fast path).
func normalizeTagsForCaching(tags []metrics.Tag, excl excludeTags) []metrics.Tag {
	if len(excl) == 0 {
		return tags
	}
	var normalized []metrics.Tag
	for i, t := range tags {
		nt, changed := normalizeTag(t, excl)
		if changed {
			if normalized == nil {
				normalized = slices.Clone(tags)
			}
			normalized[i] = nt
		}
	}
	if normalized != nil {
		return normalized
	}
	return tags
}

// Counter obtains a counter for the given name.
func (tmh *tallyMetricsHandler) Counter(counter string) metrics.CounterIface {
	if v, ok := tmh.counters.Load(counter); ok {
		return v.(metrics.CounterIface) //nolint:revive // type-safe: only metrics.CounterIface is stored
	}
	c := metrics.CounterFunc(func(i int64, t ...metrics.Tag) {
		tmh.cachedTaggedScope(t).Counter(counter).Inc(i)
	})
	actual, _ := tmh.counters.LoadOrStore(counter, c)
	return actual.(metrics.CounterIface) //nolint:revive // type-safe: only metrics.CounterIface is stored
}

// Gauge obtains a gauge for the given name.
func (tmh *tallyMetricsHandler) Gauge(gauge string) metrics.GaugeIface {
	if v, ok := tmh.gauges.Load(gauge); ok {
		return v.(metrics.GaugeIface) //nolint:revive // type-safe: only metrics.GaugeIface is stored
	}
	g := metrics.GaugeFunc(func(f float64, t ...metrics.Tag) {
		tmh.cachedTaggedScope(t).Gauge(gauge).Update(f)
	})
	actual, _ := tmh.gauges.LoadOrStore(gauge, g)
	return actual.(metrics.GaugeIface) //nolint:revive // type-safe: only metrics.GaugeIface is stored
}

// Timer obtains a timer for the given name.
func (tmh *tallyMetricsHandler) Timer(timer string) metrics.TimerIface {
	if v, ok := tmh.timers.Load(timer); ok {
		return v.(metrics.TimerIface) //nolint:revive // type-safe: only metrics.TimerIface is stored
	}
	ti := metrics.TimerFunc(func(d time.Duration, t ...metrics.Tag) {
		tmh.cachedTaggedScope(t).Timer(timer).Record(d)
	})
	actual, _ := tmh.timers.LoadOrStore(timer, ti)
	return actual.(metrics.TimerIface) //nolint:revive // type-safe: only metrics.TimerIface is stored
}

// Histogram obtains a histogram for the given name.
func (tmh *tallyMetricsHandler) Histogram(histogram string, unit metrics.MetricUnit) metrics.HistogramIface {
	key := histogramCacheKey{name: histogram, unit: unit}
	if v, ok := tmh.histograms.Load(key); ok {
		return v.(metrics.HistogramIface) //nolint:revive // type-safe: only metrics.HistogramIface is stored
	}
	h := metrics.HistogramFunc(func(i int64, t ...metrics.Tag) {
		tmh.cachedTaggedScope(t).Histogram(histogram, tmh.perUnitBuckets[unit]).RecordValue(float64(i))
	})
	actual, _ := tmh.histograms.LoadOrStore(key, h)
	return actual.(metrics.HistogramIface) //nolint:revive // type-safe: only metrics.HistogramIface is stored
}

func (*tallyMetricsHandler) Stop(log.Logger) {}

func (*tallyMetricsHandler) Close() error {
	return nil
}

func (tmh *tallyMetricsHandler) StartBatch(_ string) metrics.BatchHandler {
	return tmh
}

func tagsToMap(t1 []metrics.Tag, e excludeTags) map[string]string {
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
