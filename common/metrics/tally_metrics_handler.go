package metrics

import (
	"encoding/binary"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
)

// defaultTagsCacheMaxSize is the default upper bound on cached scope/handler entries.
const defaultTagsCacheMaxSize = 10000

type histogramCacheKey struct {
	name string
	unit MetricUnit
}

var sanitizer = tally.NewSanitizer(tally.SanitizeOptions{
	NameCharacters:       tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	KeyCharacters:        tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	ValueCharacters:      tally.ValidCharacters{Ranges: tally.AlphanumericRange, Characters: tally.UnderscoreCharacters},
	ReplacementCharacter: '_',
})

// sharedScopeCache is a bounded cache shared across all tallyMetricsHandler
// instances in a handler tree. Uses sync.Map internally for lock-free reads
// and zero-alloc key lookups via tempString. When the cache reaches its size
// limit, new entries are not cached (graceful degradation).
type sharedScopeCache struct {
	scopes      sync.Map // tagsCacheKey -> tally.Scope
	handlers    sync.Map // tagsCacheKey -> *tallyMetricsHandler
	scopeSize   atomic.Int64
	handlerSize atomic.Int64
	maxSize     int
}

func newSharedScopeCache(maxSize int) *sharedScopeCache {
	return &sharedScopeCache{
		maxSize: maxSize,
	}
}

func (c *sharedScopeCache) loadOrStoreScope(buf []byte, create func() tally.Scope) tally.Scope {
	if v, ok := c.scopes.Load(tempString(buf)); ok {
		return v.(tally.Scope) //nolint:revive // type-safe: only tally.Scope is stored
	}
	s := create()
	if c.scopeSize.Load() >= int64(c.maxSize) {
		return s
	}
	key := string(buf)
	actual, loaded := c.scopes.LoadOrStore(key, s)
	if !loaded {
		c.scopeSize.Add(1)
	}
	return actual.(tally.Scope) //nolint:revive // type-safe: only tally.Scope is stored
}

func (c *sharedScopeCache) loadOrStoreHandler(buf []byte, create func() *tallyMetricsHandler) *tallyMetricsHandler {
	if v, ok := c.handlers.Load(tempString(buf)); ok {
		return v.(*tallyMetricsHandler) //nolint:revive // type-safe: only *tallyMetricsHandler is stored
	}
	h := create()
	if c.handlerSize.Load() >= int64(c.maxSize) {
		return h
	}
	key := string(buf)
	actual, loaded := c.handlers.LoadOrStore(key, h)
	if !loaded {
		c.handlerSize.Add(1)
	}
	return actual.(*tallyMetricsHandler) //nolint:revive // type-safe: only *tallyMetricsHandler is stored
}

type (
	excludeTags map[string]map[string]struct{}

	tallyMetricsHandler struct {
		scope          tally.Scope
		perUnitBuckets map[MetricUnit]tally.Buckets
		excludeTags    excludeTags
		cache          *sharedScopeCache
		scopeKey       string                   // cache key prefix from parent handler
		counters       atomic.Pointer[sync.Map] // metric name -> CounterIface
		gauges         atomic.Pointer[sync.Map] // metric name -> GaugeIface
		timers         atomic.Pointer[sync.Map] // metric name -> TimerIface
		histograms     atomic.Pointer[sync.Map] // metric name + unit -> HistogramIface
	}
)

var _ Handler = (*tallyMetricsHandler)(nil)

func NewTallyMetricsHandler(cfg ClientConfig, scope tally.Scope) *tallyMetricsHandler {
	perUnitBuckets := make(map[MetricUnit]tally.Buckets)

	for unit, boundariesList := range cfg.PerUnitHistogramBoundaries {
		perUnitBuckets[MetricUnit(unit)] = tally.ValueBuckets(boundariesList)
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

// tempString creates a temporary string from buf without allocation.
// The returned string is only valid as long as buf is not modified or
// retained. Safe for use as a sync.Map Load key because Load does not
// retain the key after returning.
func tempString(buf []byte) string {
	if len(buf) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(buf), len(buf))
}

// appendCacheKey appends a compact cache key for the given tags to buf
// and returns the extended buffer. Uses varint length-prefixed encoding
// to avoid collisions from embedded null bytes in tag key/value strings.
func appendCacheKey(buf []byte, tags []Tag) []byte {
	var lenBuf [binary.MaxVarintLen64]byte
	for _, t := range tags {
		n := binary.PutUvarint(lenBuf[:], uint64(len(t.Key)))
		buf = append(buf, lenBuf[:n]...)
		buf = append(buf, t.Key...)
		n = binary.PutUvarint(lenBuf[:], uint64(len(t.Value)))
		buf = append(buf, lenBuf[:n]...)
		buf = append(buf, t.Value...)
	}
	return buf
}

// tagsCacheKey builds a compact string key from a tag slice for use as a
// cache key. Allocates only the returned string (key is built on stack).
func tagsCacheKey(tags []Tag) string {
	var scratch [128]byte
	buf := appendCacheKey(scratch[:0], tags)
	return string(buf)
}

// WithTags creates a new MetricProvider with provided []Tag
// Tags are merged with registered Tags from the source MetricsHandler.
// Handlers are cached by normalized tag combination so that excluded tags
// with different raw values share a single cache entry. The cache is shared
// across the handler tree and bounded to TagsCacheMaxSize entries.
func (tmh *tallyMetricsHandler) WithTags(tags ...Tag) Handler {
	if len(tags) == 0 {
		return tmh
	}
	normalized := normalizeTagsForCaching(tags, tmh.excludeTags)
	var scratch [128]byte
	buf := append(scratch[:0], tmh.scopeKey...)
	buf = appendCacheKey(buf, normalized)
	return tmh.cache.loadOrStoreHandler(buf, func() *tallyMetricsHandler {
		key := string(buf)
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
// map to the same excluded placeholder share a single cache entry. The cache
// is bounded to TagsCacheMaxSize entries.
func (tmh *tallyMetricsHandler) cachedTaggedScope(tags []Tag) tally.Scope {
	if len(tags) == 0 {
		return tmh.scope
	}
	normalized := normalizeTagsForCaching(tags, tmh.excludeTags)
	var scratch [128]byte
	buf := append(scratch[:0], tmh.scopeKey...)
	buf = appendCacheKey(buf, normalized)
	return tmh.cache.loadOrStoreScope(buf, func() tally.Scope {
		return tmh.scope.Tagged(tagsToMap(tags, tmh.excludeTags))
	})
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

// countersMap lazily initializes and returns the counters sync.Map.
func (tmh *tallyMetricsHandler) countersMap() *sync.Map {
	if m := tmh.counters.Load(); m != nil {
		return m
	}
	m := new(sync.Map)
	if tmh.counters.CompareAndSwap(nil, m) {
		return m
	}
	return tmh.counters.Load()
}

// gaugesMap lazily initializes and returns the gauges sync.Map.
func (tmh *tallyMetricsHandler) gaugesMap() *sync.Map {
	if m := tmh.gauges.Load(); m != nil {
		return m
	}
	m := new(sync.Map)
	if tmh.gauges.CompareAndSwap(nil, m) {
		return m
	}
	return tmh.gauges.Load()
}

// timersMap lazily initializes and returns the timers sync.Map.
func (tmh *tallyMetricsHandler) timersMap() *sync.Map {
	if m := tmh.timers.Load(); m != nil {
		return m
	}
	m := new(sync.Map)
	if tmh.timers.CompareAndSwap(nil, m) {
		return m
	}
	return tmh.timers.Load()
}

// histogramsMap lazily initializes and returns the histograms sync.Map.
func (tmh *tallyMetricsHandler) histogramsMap() *sync.Map {
	if m := tmh.histograms.Load(); m != nil {
		return m
	}
	m := new(sync.Map)
	if tmh.histograms.CompareAndSwap(nil, m) {
		return m
	}
	return tmh.histograms.Load()
}

// Counter obtains a counter for the given name.
func (tmh *tallyMetricsHandler) Counter(counter string) CounterIface {
	m := tmh.countersMap()
	if v, ok := m.Load(counter); ok {
		return v.(CounterIface) //nolint:revive // type-safe: only CounterIface is stored
	}
	c := CounterFunc(func(i int64, t ...Tag) {
		tmh.cachedTaggedScope(t).Counter(counter).Inc(i)
	})
	actual, _ := m.LoadOrStore(counter, c)
	return actual.(CounterIface) //nolint:revive // type-safe: only CounterIface is stored
}

// Gauge obtains a gauge for the given name.
func (tmh *tallyMetricsHandler) Gauge(gauge string) GaugeIface {
	m := tmh.gaugesMap()
	if v, ok := m.Load(gauge); ok {
		return v.(GaugeIface) //nolint:revive // type-safe: only GaugeIface is stored
	}
	g := GaugeFunc(func(f float64, t ...Tag) {
		tmh.cachedTaggedScope(t).Gauge(gauge).Update(f)
	})
	actual, _ := m.LoadOrStore(gauge, g)
	return actual.(GaugeIface) //nolint:revive // type-safe: only GaugeIface is stored
}

// Timer obtains a timer for the given name.
func (tmh *tallyMetricsHandler) Timer(timer string) TimerIface {
	m := tmh.timersMap()
	if v, ok := m.Load(timer); ok {
		return v.(TimerIface) //nolint:revive // type-safe: only TimerIface is stored
	}
	ti := TimerFunc(func(d time.Duration, t ...Tag) {
		tmh.cachedTaggedScope(t).Timer(timer).Record(d)
	})
	actual, _ := m.LoadOrStore(timer, ti)
	return actual.(TimerIface) //nolint:revive // type-safe: only TimerIface is stored
}

// Histogram obtains a histogram for the given name.
func (tmh *tallyMetricsHandler) Histogram(histogram string, unit MetricUnit) HistogramIface {
	key := histogramCacheKey{name: histogram, unit: unit}
	m := tmh.histogramsMap()
	if v, ok := m.Load(key); ok {
		return v.(HistogramIface) //nolint:revive // type-safe: only HistogramIface is stored
	}
	h := HistogramFunc(func(i int64, t ...Tag) {
		tmh.cachedTaggedScope(t).Histogram(histogram, tmh.perUnitBuckets[unit]).RecordValue(float64(i))
	})
	actual, _ := m.LoadOrStore(key, h)
	return actual.(HistogramIface) //nolint:revive // type-safe: only HistogramIface is stored
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
