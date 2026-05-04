package metrics

import (
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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

	assert.EqualValues(t, 8, counters["test.hits+"].Value())
	assert.EqualValues(t, map[string]string{}, counters["test.hits+"].Tags())

	assert.EqualValues(t, 11, counters["test.hits-tagged+taskqueue=__sticky__"].Value())
	assert.EqualValues(t, map[string]string{"taskqueue": "__sticky__"}, counters["test.hits-tagged+taskqueue=__sticky__"].Tags())

	assert.EqualValues(t, 14, counters["test.hits-tagged-excluded+taskqueue="+tagExcludedValue].Value())
	assert.EqualValues(t, map[string]string{"taskqueue": tagExcludedValue}, counters["test.hits-tagged-excluded+taskqueue="+tagExcludedValue].Tags())

	assert.EqualValues(t, float64(-100), gauges["test.temp+location=Mare Imbrium"].Value())
	assert.EqualValues(t, map[string]string{
		"location": "Mare Imbrium",
	}, gauges["test.temp+location=Mare Imbrium"].Tags())

	assert.EqualValues(t, []time.Duration{
		1248 * time.Millisecond,
		5255 * time.Millisecond,
	}, timers["test.latency+"].Values())
	assert.EqualValues(t, map[string]string{}, timers["test.latency+"].Tags())

	assert.EqualValues(t, map[float64]int64{
		1024:            0,
		2048:            0,
		math.MaxFloat64: 1,
	}, histograms["test.transmission+"].Values())
	assert.EqualValues(t, map[time.Duration]int64(nil), histograms["test.transmission+"].Durations())
	assert.EqualValues(t, map[string]string{}, histograms["test.transmission+"].Tags())

	newTaggedHandler := mp.WithTags(NamespaceTag(uuid.NewString()))
	recordTallyMetrics(newTaggedHandler)
	snap = scope.Snapshot()
	counters = snap.Counters()

	assert.EqualValues(t, 11, counters["test.hits-tagged+taskqueue=__sticky__"].Value())
	assert.EqualValues(t, map[string]string{"taskqueue": "__sticky__"}, counters["test.hits-tagged+taskqueue=__sticky__"].Tags())
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
	assert.EqualValues(t, 8, c.Value())
	assert.EqualValues(t, map[string]string{"env": "prod"}, c.Tags())
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
	assert.EqualValues(t, 3, c.Value())
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
	assert.EqualValues(t, 2, c.Value())
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
	assert.EqualValues(t, 1, c.Value())
}

func TestWithTags_ConcurrentAccess(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	const goroutines = 32
	const iterations = 100
	var wg sync.WaitGroup
	handlers := make([]Handler, goroutines)

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			var last Handler
			for j := 0; j < iterations; j++ {
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
	assert.EqualValues(t, int64(goroutines*iterations), c.Value())
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
	assert.EqualValues(t, 3, ok.Value())

	errC := snap.Counters()["test.requests+status=err"]
	require.NotNil(t, errC)
	assert.EqualValues(t, 5, errC.Value())
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
	assert.EqualValues(t, 99.0, gauge.Value())
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
	assert.Equal(t, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}, timer.Values())
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
	assert.EqualValues(t, map[float64]int64{
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
	assert.EqualValues(t, 7, c.Value())
}

func TestScopeCache_ExcludeTagsApply(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	h.Counter("hits").Record(1, ActivityTypeTag("MyActivity"))

	snap := scope.Snapshot()
	c := snap.Counters()["test.hits+activityType="+tagExcludedValue]
	require.NotNil(t, c, "excluded tag value should be sanitized via scope cache")
	assert.EqualValues(t, 1, c.Value())
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
	assert.EqualValues(t, 1, c1.Value())

	c2 := snap.Counters()["test.hits+env=prod,operation=op2"]
	require.NotNil(t, c2)
	assert.EqualValues(t, 2, c2.Value())
}

func TestScopeCache_ConcurrentRecordWithTags(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	const goroutines = 32
	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			c := h.Counter("concurrent")
			for j := 0; j < iterations; j++ {
				c.Record(1, StringTag("shard", "0"))
			}
		}()
	}
	wg.Wait()

	snap := scope.Snapshot()
	c := snap.Counters()["test.concurrent+shard=0"]
	require.NotNil(t, c)
	assert.EqualValues(t, int64(goroutines*iterations), c.Value())
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
	assert.EqualValues(t, 7, c.Value(), "all excluded tag values should map to the same counter")

	// Verify only one cache entry was created, not three.
	require.Equal(t, int64(1), h.scopeCacheSize.Load(),
		"excluded tags with different raw values should share a single cache entry")
}

func TestScopeCache_BoundedSize(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	// Fill the cache to the limit.
	for i := 0; i < scopeCacheMaxSize; i++ {
		h.Counter("c").Record(1, StringTag("id", strconv.Itoa(i)))
	}
	require.Equal(t, int64(scopeCacheMaxSize), h.scopeCacheSize.Load())

	// Beyond the limit, metrics still work but new entries aren't cached.
	h.Counter("c").Record(1, StringTag("id", "overflow"))
	require.Equal(t, int64(scopeCacheMaxSize), h.scopeCacheSize.Load(),
		"scope cache should not grow beyond the limit")

	snap := scope.Snapshot()
	c := snap.Counters()["test.c+id=overflow"]
	require.NotNil(t, c, "metrics should work even when scope cache is full")
	assert.EqualValues(t, 1, c.Value())

	// Existing cached entries should still be served from cache.
	h.Counter("c").Record(1, StringTag("id", "0"))
	snap = scope.Snapshot()
	c0 := snap.Counters()["test.c+id=0"]
	require.NotNil(t, c0)
	assert.EqualValues(t, 2, c0.Value())
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

func TestCounter_CacheReturnsSameInstance(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	c1 := h.Counter("requests")
	c2 := h.Counter("requests")

	// Verify cache stores an entry.
	_, ok := h.counterCache.Load("requests")
	require.True(t, ok)

	// Functional correctness: both references record to the same counter.
	c1.Record(3)
	c2.Record(7)
	snap := scope.Snapshot()
	require.EqualValues(t, 10, snap.Counters()["test.requests+"].Value())
}

func TestTimer_CacheReturnsSameInstance(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	h.Timer("latency")
	h.Timer("latency")

	_, ok := h.timerCache.Load("latency")
	require.True(t, ok)
}

func TestHistogram_CacheReturnsSameInstance(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	h.Histogram("size", Bytes)
	h.Histogram("size", Bytes)

	_, ok := h.histogramCache.Load("size\x00" + string(Bytes))
	require.True(t, ok)
}

func TestWithTags_ExcludedTagsDedup(t *testing.T) {
	// WithTags should normalize excluded tags before keying, so different raw
	// values that map to the same excluded placeholder share a cache entry.
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	// "activityType" has empty allow-list → all values excluded.
	h1 := h.WithTags(ActivityTypeTag("ActivityA"))
	h2 := h.WithTags(ActivityTypeTag("ActivityB"))
	require.Same(t, h1, h2, "excluded tag values should deduplicate to same cached handler")
}

func TestWithTags_ChildCacheBounded(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	// Fill child cache to the limit.
	for i := 0; i < scopeCacheMaxSize; i++ {
		h.WithTags(OperationTag("op_" + strconv.Itoa(i)))
	}
	require.Equal(t, int64(scopeCacheMaxSize), h.childCacheSize.Load())

	// One more should not increase the cache size.
	h.WithTags(OperationTag("op_overflow"))
	require.Equal(t, int64(scopeCacheMaxSize), h.childCacheSize.Load(),
		"childCache should not grow beyond scopeCacheMaxSize")
}

func TestGauge_CacheReturnsSameInstance(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)

	g1 := h.Gauge("connections")
	g2 := h.Gauge("connections")

	_, ok := h.gaugeCache.Load("connections")
	require.True(t, ok)

	g1.Record(5)
	g2.Record(3)
	snap := scope.Snapshot()
	require.EqualValues(t, 3, snap.Gauges()["test.connections+"].Value())
}

func BenchmarkWithTags_CacheHit(b *testing.B) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)
	// Warm the cache.
	h.WithTags(OperationTag("op1"))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.WithTags(OperationTag("op1"))
	}
}

func BenchmarkWithTags_ExcludedDedup(b *testing.B) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)
	// Warm the cache with one excluded value.
	h.WithTags(ActivityTypeTag("ActivityA"))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Different raw value but normalizes to same key.
		h.WithTags(ActivityTypeTag("ActivityB"))
	}
}

func BenchmarkCounter_CacheHit(b *testing.B) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)
	c := h.Counter("hits")
	_ = c

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.Counter("hits").Record(1, OperationTag("op1"))
	}
}

func TestWithTags_CacheHitAllocs(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)
	h.WithTags(OperationTag("op1"))

	allocs := testing.AllocsPerRun(1000, func() {
		h.WithTags(OperationTag("op1"))
	})
	require.Equal(t, float64(0), allocs,
		"WithTags cache hit should be allocation-free")
}

func TestWithTags_ExcludedDedupAllocs(t *testing.T) {
	scope := tally.NewTestScope("test", map[string]string{})
	h := NewTallyMetricsHandler(defaultConfig, scope)
	h.WithTags(ActivityTypeTag("ActivityA"))

	allocs := testing.AllocsPerRun(1000, func() {
		h.WithTags(ActivityTypeTag("ActivityB"))
	})
	// 1 alloc for the normalizeTagsForCaching []Tag slice (substitution needed).
	// The key string itself is allocation-free on cache hit via unsafe.String.
	require.LessOrEqual(t, allocs, float64(1),
		"excluded tag dedup cache hit should only allocate for normalization")
}
