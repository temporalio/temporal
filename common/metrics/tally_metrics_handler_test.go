package metrics

import (
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
)

var defaultConfig = ClientConfig{
	Tags: nil,
	ExcludeTags: map[string][]string{
		"taskqueue":    {"__sticky__"},
		"activityType": {},
		"workflowType": {},
	},
	Prefix: "",
	PerUnitHistogramBoundaries: map[string][]float64{
		Dimensionless: {0, 10, 100},
		Bytes:         {1024, 2048},
		Milliseconds:  {10, 500, 1000, 5000, 10000},
		Seconds:       {0.01, 0.5, 1, 5, 10},
	},
}

func TestTallyScope(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	mp := NewTallyMetricsHandler(defaultConfig, scope)
	recordTallyMetrics(mp)

	snap := scope.Snapshot()
	counters, gauges, timers, histograms := snap.Counters(), snap.Gauges(), snap.Timers(), snap.Histograms()

	require.EqualValues(t, 8, counters["test.hits+"].Value())
	require.Equal(t, map[string]string{}, counters["test.hits+"].Tags())

	require.EqualValues(t, 11, counters["test.hits-tagged+taskqueue=__sticky__"].Value())
	require.Equal(t, map[string]string{"taskqueue": "__sticky__"}, counters["test.hits-tagged+taskqueue=__sticky__"].Tags())

	require.EqualValues(t, 14, counters["test.hits-tagged-excluded+taskqueue="+tagExcludedValue].Value())
	require.Equal(t, map[string]string{"taskqueue": tagExcludedValue}, counters["test.hits-tagged-excluded+taskqueue="+tagExcludedValue].Tags())

	require.InDelta(t, float64(-100), gauges["test.temp+location=Mare Imbrium"].Value(), 0.01)
	require.Equal(t, map[string]string{
		"location": "Mare Imbrium",
	}, gauges["test.temp+location=Mare Imbrium"].Tags())

	require.Equal(t, []time.Duration{
		1248 * time.Millisecond,
		5255 * time.Millisecond,
	}, timers["test.latency+"].Values())
	require.Equal(t, map[string]string{}, timers["test.latency+"].Tags())

	require.Equal(t, map[float64]int64{
		1024:            0,
		2048:            0,
		math.MaxFloat64: 1,
	}, histograms["test.transmission+"].Values())
	require.Equal(t, map[time.Duration]int64(nil), histograms["test.transmission+"].Durations())
	require.Equal(t, map[string]string{}, histograms["test.transmission+"].Tags())

	newTaggedHandler := mp.WithTags(NamespaceTag(uuid.NewString()))
	recordTallyMetrics(newTaggedHandler)
	snap = scope.Snapshot()
	counters = snap.Counters()

	require.EqualValues(t, 11, counters["test.hits-tagged+taskqueue=__sticky__"].Value())
	require.Equal(t, map[string]string{"taskqueue": "__sticky__"}, counters["test.hits-tagged+taskqueue=__sticky__"].Tags())
}

func recordTallyMetrics(h Handler) {
	hitsCounter := h.Counter("hits")
	gauge := h.Gauge("temp")
	timer := h.Timer("latency")
	histogram := h.Histogram("transmission", Bytes)
	hitsTaggedCounter := h.Counter("hits-tagged")
	hitsTaggedExcludedCounter := h.Counter("hits-tagged-excluded")

	hitsCounter.Record(8)
	gauge.Record(-100, StringTag("location", "Mare Imbrium"))
	timer.Record(1248 * time.Millisecond)
	timer.Record(5255 * time.Millisecond)
	histogram.Record(1234567)
	hitsTaggedCounter.Record(11, UnsafeTaskQueueTag("__sticky__"))
	hitsTaggedExcludedCounter.Record(14, UnsafeTaskQueueTag("filtered"))
}

func TestWithTags_EmptyTagsReturnsSelf(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	got := h.WithTags()
	require.Same(t, h, got, "WithTags() with no args should return the same handler")
}

func TestWithTags_CacheHitReturnsSamePointer(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	h1 := h.WithTags(OperationTag("op1"))
	h2 := h.WithTags(OperationTag("op1"))
	require.Same(t, h1, h2, "repeated WithTags with identical args should return the same handler")
}

func TestWithTags_DifferentTagsReturnDifferentHandlers(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	h1 := h.WithTags(OperationTag("op1"))
	h2 := h.WithTags(OperationTag("op2"))
	require.NotSame(t, h1, h2, "WithTags with different values must produce different handlers")

	// Different keys with same value.
	h3 := h.WithTags(StringTag("key_a", "val"))
	h4 := h.WithTags(StringTag("key_b", "val"))
	require.NotSame(t, h3, h4, "WithTags with different keys must produce different handlers")
}

func TestWithTags_CachedHandlerRecordsMetricsCorrectly(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	tagged := h.WithTags(StringTag("env", "prod"))

	// Record via first call.
	tagged.Counter("requests").Record(5)

	// Record via second (cached) call.
	cached := h.WithTags(StringTag("env", "prod"))
	cached.Counter("requests").Record(3)

	snap := scope.Snapshot()
	c := snap.Counters()["test.requests+env=prod"]
	require.NotNil(t, c)
	require.EqualValues(t, 8, c.Value())
	require.Equal(t, map[string]string{"env": "prod"}, c.Tags())
}

func TestWithTags_MultipleTagsCacheCorrectly(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	tags := []Tag{OperationTag("op1"), StringTag("env", "staging")}
	h1 := h.WithTags(tags...)
	h2 := h.WithTags(tags...)
	require.Same(t, h1, h2, "multi-tag WithTags should be cached")

	h1.Counter("hits").Record(1)
	h2.Counter("hits").Record(2)
	snap := scope.Snapshot()
	c := snap.Counters()["test.hits+env=staging,operation=op1"]
	require.NotNil(t, c)
	require.EqualValues(t, 3, c.Value())
}

func TestWithTags_TagOrderMatters(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	// Different ordering of the same two tags should be separate cache
	// entries (the tally scope will merge them, but the cache keys differ).
	h1 := h.WithTags(StringTag("a", "1"), StringTag("b", "2"))
	h2 := h.WithTags(StringTag("b", "2"), StringTag("a", "1"))

	// They must both work — record via each.
	h1.Counter("c").Record(1)
	h2.Counter("c").Record(1)

	snap := scope.Snapshot()
	c := snap.Counters()["test.c+a=1,b=2"]
	require.NotNil(t, c)
	require.EqualValues(t, 2, c.Value())
}

func TestWithTags_ChildCachesAreIndependent(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	child := h.WithTags(OperationTag("parent_op"))
	grandchild := child.WithTags(StringTag("env", "dev"))

	// Grandchild should be cached on child, not on root.
	grandchild2 := child.WithTags(StringTag("env", "dev"))
	require.Same(t, grandchild, grandchild2)

	// Root should not have the grandchild cached.
	fromRoot := h.WithTags(StringTag("env", "dev"))
	require.NotSame(t, grandchild, fromRoot, "child and root caches should be independent")
}

func TestWithTags_ExcludeTagsStillApply(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	// "activityType" is in excludeTags with empty allow-list, so any value
	// should be replaced with tagExcludedValue.
	tagged := h.WithTags(ActivityTypeTag("MyActivity"))
	tagged.Counter("hits").Record(1)

	snap := scope.Snapshot()
	c := snap.Counters()["test.hits+activityType="+tagExcludedValue]
	require.NotNil(t, c, "excluded tag value should be sanitized")
	require.EqualValues(t, 1, c.Value())
}

func TestWithTags_ExcludedTagsShareChildHandler(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	// Different excluded-tag values should produce the same cached child handler,
	// preventing unbounded childCache growth from high-cardinality excluded tags.
	h1 := h.WithTags(ActivityTypeTag("TypeA"))
	h2 := h.WithTags(ActivityTypeTag("TypeB"))
	h3 := h.WithTags(ActivityTypeTag("TypeC"))
	require.Same(t, h1, h2, "excluded tag variants should share the same child handler")
	require.Same(t, h2, h3, "excluded tag variants should share the same child handler")

	// Verify the child handler still records metrics correctly.
	h1.Counter("hits").Record(1)
	h2.Counter("hits").Record(2)
	h3.Counter("hits").Record(4)

	snap := scope.Snapshot()
	c := snap.Counters()["test.hits+activityType="+tagExcludedValue]
	require.NotNil(t, c)
	require.EqualValues(t, 7, c.Value())
}

func TestWithTags_ConcurrentAccess(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	const goroutines = 32
	const iterations = 100
	var wg sync.WaitGroup
	handlers := make([]Handler, goroutines)

	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			var last Handler
			for range iterations {
				last = h.WithTags(OperationTag("concurrent_op"))
				last.Counter("concurrent_count").Record(1)
			}
			handlers[idx] = last
		}(i)
	}
	wg.Wait()

	// All goroutines should have received the same cached handler.
	for i := 1; i < goroutines; i++ {
		require.Same(t, handlers[0], handlers[i],
			"all goroutines should get the same cached handler")
	}

	snap := scope.Snapshot()
	c := snap.Counters()["test.concurrent_count+operation=concurrent_op"]
	require.NotNil(t, c)
	require.Equal(t, int64(goroutines*iterations), c.Value())
}

func TestTagsCacheKey(t *testing.T) {
	tests := []struct {
		name string
		a, b []Tag
		same bool
	}{
		{
			name: "identical single tags",
			a:    []Tag{{Key: "op", Value: "foo"}},
			b:    []Tag{{Key: "op", Value: "foo"}},
			same: true,
		},
		{
			name: "different values",
			a:    []Tag{{Key: "op", Value: "foo"}},
			b:    []Tag{{Key: "op", Value: "bar"}},
			same: false,
		},
		{
			name: "different keys",
			a:    []Tag{{Key: "op", Value: "x"}},
			b:    []Tag{{Key: "ns", Value: "x"}},
			same: false,
		},
		{
			name: "identical multi tags",
			a:    []Tag{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}},
			b:    []Tag{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}},
			same: true,
		},
		{
			name: "different ordering",
			a:    []Tag{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}},
			b:    []Tag{{Key: "b", Value: "2"}, {Key: "a", Value: "1"}},
			same: false,
		},
		{
			name: "single vs multi",
			a:    []Tag{{Key: "a", Value: "1"}},
			b:    []Tag{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}},
			same: false,
		},
		{
			name: "key boundary ambiguity",
			a:    []Tag{{Key: "ab", Value: "c"}},
			b:    []Tag{{Key: "a", Value: "bc"}},
			same: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ka := tagsCacheKey(tt.a)
			kb := tagsCacheKey(tt.b)
			if tt.same {
				require.Equal(t, ka, kb)
			} else {
				require.NotEqual(t, ka, kb)
			}
		})
	}
}

func TestScopeCache_CounterWithInlineTags(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	c := h.Counter("requests")
	c.Record(1, StringTag("status", "ok"))
	c.Record(2, StringTag("status", "ok"))
	c.Record(5, StringTag("status", "err"))

	snap := scope.Snapshot()
	ok := snap.Counters()["test.requests+status=ok"]
	require.NotNil(t, ok)
	require.EqualValues(t, 3, ok.Value())

	errC := snap.Counters()["test.requests+status=err"]
	require.NotNil(t, errC)
	require.EqualValues(t, 5, errC.Value())
}

func TestScopeCache_GaugeWithInlineTags(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	g := h.Gauge("temp")
	g.Record(42.0, StringTag("location", "cpu"))
	g.Record(99.0, StringTag("location", "cpu"))

	snap := scope.Snapshot()
	gauge := snap.Gauges()["test.temp+location=cpu"]
	require.NotNil(t, gauge)
	require.InDelta(t, 99.0, gauge.Value(), 0.01)
}

func TestScopeCache_TimerWithInlineTags(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	ti := h.Timer("latency")
	ti.Record(100*time.Millisecond, StringTag("op", "read"))
	ti.Record(200*time.Millisecond, StringTag("op", "read"))

	snap := scope.Snapshot()
	timer := snap.Timers()["test.latency+op=read"]
	require.NotNil(t, timer)
	require.Equal(t, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}, timer.Values())
}

func TestScopeCache_HistogramWithInlineTags(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	hist := h.Histogram("size", Bytes)
	hist.Record(512, StringTag("type", "payload"))
	hist.Record(4096, StringTag("type", "payload"))

	snap := scope.Snapshot()
	histo := snap.Histograms()["test.size+type=payload"]
	require.NotNil(t, histo)
	require.Equal(t, map[float64]int64{
		1024:            1,
		2048:            0,
		math.MaxFloat64: 1,
	}, histo.Values())
}

func TestScopeCache_NoTagsUsesBaseScope(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	h.Counter("hits").Record(7)

	snap := scope.Snapshot()
	c := snap.Counters()["test.hits+"]
	require.NotNil(t, c)
	require.EqualValues(t, 7, c.Value())
}

func TestScopeCache_ExcludeTagsApply(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	h.Counter("hits").Record(1, ActivityTypeTag("MyActivity"))

	snap := scope.Snapshot()
	c := snap.Counters()["test.hits+activityType="+tagExcludedValue]
	require.NotNil(t, c, "excluded tag value should be sanitized via scope cache")
	require.EqualValues(t, 1, c.Value())
}

func TestScopeCache_IndependentPerHandler(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	child1 := h.WithTags(OperationTag("op1"))
	child2 := h.WithTags(OperationTag("op2"))

	child1.Counter("hits").Record(1, StringTag("env", "prod"))
	child2.Counter("hits").Record(2, StringTag("env", "prod"))

	snap := scope.Snapshot()
	c1 := snap.Counters()["test.hits+env=prod,operation=op1"]
	require.NotNil(t, c1)
	require.EqualValues(t, 1, c1.Value())

	c2 := snap.Counters()["test.hits+env=prod,operation=op2"]
	require.NotNil(t, c2)
	require.EqualValues(t, 2, c2.Value())
}

func TestScopeCache_ConcurrentRecordWithTags(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	const goroutines = 32
	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			c := h.Counter("concurrent")
			for range iterations {
				c.Record(1, StringTag("shard", "0"))
			}
		}()
	}
	wg.Wait()

	snap := scope.Snapshot()
	c := snap.Counters()["test.concurrent+shard=0"]
	require.NotNil(t, c)
	require.Equal(t, int64(goroutines*iterations), c.Value())
}

func TestScopeCache_ExcludedTagsMergeInCache(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	// "activityType" has empty allow-list, so all values are excluded.
	// Different raw values should normalize to the same cache entry.
	h.Counter("hits").Record(1, ActivityTypeTag("TypeA"))
	h.Counter("hits").Record(2, ActivityTypeTag("TypeB"))
	h.Counter("hits").Record(4, ActivityTypeTag("TypeC"))

	snap := scope.Snapshot()
	c := snap.Counters()["test.hits+activityType="+tagExcludedValue]
	require.NotNil(t, c)
	require.EqualValues(t, 7, c.Value(), "all excluded tag values should map to the same counter")

	// Verify only one cache entry was created, not three.
	var scopeCount int
	h.cache.scopes.Range(func(_, _ any) bool {
		scopeCount++
		return true
	})
	require.Equal(t, 1, scopeCount,
		"excluded tags with different raw values should share a single cache entry")
}

func TestScopeCache_BoundedSize(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	cfg := defaultConfig
	cfg.TagsCacheMaxSize = 100
	h := NewTallyMetricsHandler(cfg, scope)

	// Fill the cache to the limit.
	for i := range 100 {
		h.Counter("c").Record(1, StringTag("id", strconv.Itoa(i)))
	}
	var scopeCount int
	h.cache.scopes.Range(func(_, _ any) bool {
		scopeCount++
		return true
	})
	require.LessOrEqual(t, scopeCount, 100,
		"scope cache should not exceed maxSize")

	// Beyond the limit, caching stops but metrics still work.
	for range 10 {
		h.Counter("c").Record(1, StringTag("id", "overflow"))
	}

	snap := scope.Snapshot()
	c := snap.Counters()["test.c+id=overflow"]
	require.NotNil(t, c, "metrics should work even after cache is full")
	require.EqualValues(t, 10, c.Value())

	// Cache should still be at (approximately) the same size.
	var afterCount int
	h.cache.scopes.Range(func(_, _ any) bool {
		afterCount++
		return true
	})
	require.LessOrEqual(t, afterCount, 110,
		"scope cache should not grow significantly beyond maxSize")
}

func TestWithTags_BoundedChildCacheSize(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	cfg := defaultConfig
	cfg.TagsCacheMaxSize = 100
	h := NewTallyMetricsHandler(cfg, scope)

	// Fill the handler cache to the limit.
	for i := range 100 {
		h.WithTags(StringTag("id", strconv.Itoa(i)))
	}
	var handlerCount int
	h.cache.handlers.Range(func(_, _ any) bool {
		handlerCount++
		return true
	})
	require.LessOrEqual(t, handlerCount, 100,
		"handler cache should not exceed maxSize")

	// Beyond the limit, WithTags still works but results are not cached.
	overflow := h.WithTags(StringTag("id", "overflow"))
	overflow.Counter("hits").Record(1)
	snap := scope.Snapshot()
	c := snap.Counters()["test.hits+id=overflow"]
	require.NotNil(t, c)
	require.EqualValues(t, 1, c.Value())

	// Cache should still be at (approximately) the same size.
	var afterCount int
	h.cache.handlers.Range(func(_, _ any) bool {
		afterCount++
		return true
	})
	require.LessOrEqual(t, afterCount, 110,
		"handler cache should not grow significantly beyond maxSize")
}

func TestNormalizeTagsForCaching(t *testing.T) {
	excl := excludeTags{
		"activityType": {},                         // empty allow-list: exclude all
		"taskqueue":    {"__sticky__": struct{}{}}, // allow only __sticky__
	}

	t.Run("no excluded tags returns original slice", func(t *testing.T) {
		tags := []Tag{{Key: "env", Value: "prod"}}
		result := normalizeTagsForCaching(tags, excl)
		require.Same(t, &tags[0], &result[0], "should return the same slice when no normalization needed")
	})

	t.Run("excluded tag gets normalized", func(t *testing.T) {
		tags := []Tag{{Key: "activityType", Value: "MyActivity"}}
		result := normalizeTagsForCaching(tags, excl)
		require.Equal(t, tagExcludedValue, result[0].Value)
	})

	t.Run("allowed tag value is not normalized", func(t *testing.T) {
		tags := []Tag{{Key: "taskqueue", Value: "__sticky__"}}
		result := normalizeTagsForCaching(tags, excl)
		require.Same(t, &tags[0], &result[0], "allowed value should not trigger normalization")
	})

	t.Run("mixed tags normalize only excluded ones", func(t *testing.T) {
		tags := []Tag{
			{Key: "env", Value: "prod"},
			{Key: "activityType", Value: "DoSomething"},
			{Key: "taskqueue", Value: "non-sticky"},
		}
		result := normalizeTagsForCaching(tags, excl)
		require.Equal(t, "prod", result[0].Value)
		require.Equal(t, tagExcludedValue, result[1].Value)
		require.Equal(t, tagExcludedValue, result[2].Value)
	})

	t.Run("empty excludeTags returns original", func(t *testing.T) {
		tags := []Tag{{Key: "anything", Value: "val"}}
		result := normalizeTagsForCaching(tags, nil)
		require.Same(t, &tags[0], &result[0])
	})
}

func TestAppendCacheKey_DeterministicAndCollisionFree(t *testing.T) {
	var buf [128]byte

	// Deterministic: same tags produce same bytes.
	a := []Tag{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}}
	b := []Tag{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}}
	ka := string(appendCacheKey(buf[:0], a))
	kb := string(appendCacheKey(buf[:0], b))
	require.Equal(t, ka, kb)

	// Different tags produce different keys.
	c := []Tag{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v3"}}
	require.NotEqual(t, ka, string(appendCacheKey(buf[:0], c)))

	// Empty tags produce empty key.
	require.Empty(t, string(appendCacheKey(buf[:0], nil)))
	require.Empty(t, string(appendCacheKey(buf[:0], []Tag{})))

	// Long values (> 128 bytes) work (overflow from scratch buffer).
	longKey := strings.Repeat("x", 100)
	longVal := strings.Repeat("y", 100)
	long := []Tag{{Key: longKey, Value: longVal}}
	_ = tagsCacheKey(long) // must not panic
}

func TestTagsCacheKey_NoCollisionsForEmbeddedNulls(t *testing.T) {
	tagsA := []Tag{{Key: "a", Value: "\x00b"}}
	tagsB := []Tag{{Key: "a\x00", Value: "b"}}

	require.NotEqual(t, tagsCacheKey(tagsA), tagsCacheKey(tagsB))
}

func TestWithTags_DistinguishesEmbeddedNullTags(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	h1 := h.WithTags(StringTag("a", "\x00b"))
	h2 := h.WithTags(StringTag("a\x00", "b"))

	require.NotSame(t, h1, h2)
}

func TestHistogram_CacheKeyDistinguishesNameAndUnit(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	h.Histogram("ab", MetricUnit("c"))
	h.Histogram("ab\x00c", MetricUnit(""))

	count := 0
	h.histogramsMap().Range(func(_, _ any) bool {
		count++
		return true
	})
	require.Equal(t, 2, count)
}
