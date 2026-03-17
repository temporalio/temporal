package metrics

import (
	"math"
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
