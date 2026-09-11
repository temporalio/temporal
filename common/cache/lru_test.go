package cache

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/await"
)

type (
	keyType struct {
		dummyString string
		dummyInt    int
	}
	testEntryWithCacheSize struct {
		cacheSize int
	}
)

func (c *testEntryWithCacheSize) CacheSize() int {
	return c.cacheSize
}

func TestLRU(t *testing.T) {
	t.Parallel()
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()

	cache := NewWithMetrics(4, nil, metricsHandler)

	cache.Put("A", "Foo")
	require.Equal(t, "Foo", cache.Get("A"))
	require.Nil(t, cache.Get("B"))
	require.Equal(t, 1, cache.Size())
	snapshot := capture.Snapshot()
	require.InDelta(t, float64(4), snapshot[metrics.CacheSize.Name()][0].Value, 0)
	require.InDelta(t, float64(1), snapshot[metrics.CacheUsage.Name()][0].Value, 0)

	capture = metricsHandler.StartCapture()
	cache.Put("B", "Bar")
	cache.Put("C", "Cid")
	cache.Put("D", "Delt")
	require.Equal(t, 4, cache.Size())
	snapshot = capture.Snapshot()
	require.InDelta(t, float64(4), snapshot[metrics.CacheUsage.Name()][2].Value, 0)

	require.Equal(t, "Bar", cache.Get("B"))
	require.Equal(t, "Cid", cache.Get("C"))
	require.Equal(t, "Delt", cache.Get("D"))

	cache.Put("A", "Foo2")
	require.Equal(t, "Foo2", cache.Get("A"))
	require.Equal(t, 4, cache.Size())

	capture = metricsHandler.StartCapture()
	cache.Put("E", "Epsi")
	require.Equal(t, "Epsi", cache.Get("E"))
	require.Equal(t, "Foo2", cache.Get("A"))
	require.Nil(t, cache.Get("B")) // Oldest, should be evicted
	require.Equal(t, 4, cache.Size())
	snapshot = capture.Snapshot()
	require.Len(t, snapshot[metrics.CacheUsage.Name()], 2)
	require.InDelta(t, float64(4), snapshot[metrics.CacheUsage.Name()][1].Value, 0)

	// Access C, D is now LRU
	cache.Get("C")
	cache.Put("F", "Felp")
	require.Nil(t, cache.Get("D"))
	require.Equal(t, 4, cache.Size())

	capture = metricsHandler.StartCapture()
	cache.Delete("A")
	require.Nil(t, cache.Get("A"))
	require.Equal(t, 3, cache.Size())
	snapshot = capture.Snapshot()
	require.Len(t, snapshot[metrics.CacheUsage.Name()], 1)
	require.InDelta(t, float64(3), snapshot[metrics.CacheUsage.Name()][0].Value, 0)
}

func TestGenerics(t *testing.T) {
	t.Parallel()

	key := keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}
	value := "some random value"

	cache := NewLRU(5, metrics.NoopMetricsHandler)
	cache.Put(key, value)

	require.Equal(t, value, cache.Get(key))
	require.Equal(t, value, cache.Get(keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}))
	require.Nil(t, cache.Get(keyType{
		dummyString: "some other random key",
		dummyInt:    56,
	}))
	require.Equal(t, 1, cache.Size())

	cache.Put(key, "some other random value")
	require.Equal(t, "some other random value", cache.Get(key))
	require.Equal(t, 1, cache.Size())
}

func TestLRUWithTTL(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	cache := NewWithMetrics(5,
		&Options{
			TTL:        time.Millisecond * 100,
			TimeSource: timeSource,
		},
		metricsHandler,
	)
	cache.Put("A", "foo")
	require.Equal(t, "foo", cache.Get("A"))
	snapshot := capture.Snapshot()
	require.InDelta(t, float64(5), snapshot[metrics.CacheSize.Name()][0].Value, 0)
	require.InDelta(t, float64(1), snapshot[metrics.CacheUsage.Name()][0].Value, 0)
	require.Equal(t, time.Millisecond*100, snapshot[metrics.CacheTtl.Name()][0].Value)
	require.Equal(t, time.Duration(0), snapshot[metrics.CacheEntryAgeOnGet.Name()][0].Value)
	timeSource.Advance(time.Millisecond * 300)
	require.Nil(t, cache.Get("A"))
	snapshot = capture.Snapshot()
	require.Len(t, snapshot[metrics.CacheUsage.Name()], 2)
	require.InDelta(t, float64(0), snapshot[metrics.CacheUsage.Name()][1].Value, 0)
	require.Equal(t, 0, cache.Size())
	require.Len(t, snapshot[metrics.CacheEntryAgeOnGet.Name()], 1)
	require.Equal(t, time.Millisecond*300, snapshot[metrics.CacheEntryAgeOnEviction.Name()][0].Value)
}

func TestLRUCacheConcurrentAccess(t *testing.T) {
	t.Parallel()

	cache := NewLRU(5, metrics.NoopMetricsHandler)
	values := map[string]string{
		"A": "foo",
		"B": "bar",
		"C": "zed",
		"D": "dank",
		"E": "ezpz",
	}

	for k, v := range values {
		cache.Put(k, v)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 20 {
		// concurrent get and put
		wg.Go(func() {
			<-start

			for range 1000 {
				cache.Get("A")
				cache.Put("A", "fooo")
			}
		})

		// concurrent iteration
		wg.Go(func() {
			<-start

			for range 50 {
				it := cache.Iterator()
				for it.HasNext() {
					_ = it.Next()
				}
				it.Close()
			}
		})
	}

	close(start)
	wg.Wait()
}

func TestTTL(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	cache := New(5,
		&Options{
			TTL:        time.Millisecond * 50,
			TimeSource: timeSource,
		},
	)

	cache.Put("A", t)
	require.Equal(t, t, cache.Get("A"))
	timeSource.Advance(time.Millisecond * 100)
	require.Nil(t, cache.Get("A"))
}

func TestTTLWithPin(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	metricsHandler := metricstest.NewCaptureHandler()
	cache := NewWithMetrics(5,
		&Options{
			TTL:        time.Millisecond * 50,
			Pin:        true,
			TimeSource: timeSource,
		},
		metricsHandler,
	)

	capture := metricsHandler.StartCapture()
	_, err := cache.PutIfNotExist("A", t)
	require.NoError(t, err)
	require.Equal(t, t, cache.Get("A"))
	require.Equal(t, 1, cache.Size())
	snapshot := capture.Snapshot()
	require.InDelta(t, float64(1), snapshot[metrics.CacheUsage.Name()][0].Value, 0)
	require.InDelta(t, float64(1), snapshot[metrics.CachePinnedUsage.Name()][0].Value, 0)
	capture = metricsHandler.StartCapture()
	timeSource.Advance(time.Millisecond * 100)
	require.Equal(t, t, cache.Get("A"))
	require.Equal(t, 1, cache.Size())
	// release 3 time since put if not exist also increase the counter
	cache.Release("A")
	cache.Release("A")
	cache.Release("A")
	snapshot = capture.Snapshot()
	require.InDelta(t, float64(0), snapshot[metrics.CachePinnedUsage.Name()][0].Value, 0)
	require.Nil(t, cache.Get("A"))
	require.Equal(t, 0, cache.Size())
	snapshot = capture.Snapshot()
	// cache.Release() will emit cacheUsage 3 times. cache.Get() will emit cacheUsage once.
	require.InDelta(t, float64(0), snapshot[metrics.CacheUsage.Name()][3].Value, 0)
}

func TestMaxSizeWithPin_MidItem(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	cache := New(2,
		&Options{
			TTL:        time.Millisecond * 50,
			Pin:        true,
			TimeSource: timeSource,
		},
	)

	_, err := cache.PutIfNotExist("A", t)
	require.NoError(t, err)
	require.Equal(t, 1, cache.Size())

	_, err = cache.PutIfNotExist("B", t)
	require.NoError(t, err)
	require.Equal(t, 2, cache.Size())

	_, err = cache.PutIfNotExist("C", t)
	require.Error(t, err)
	require.Equal(t, 2, cache.Size())

	require.Equal(t, t, cache.Get("A"))
	cache.Release("A") // get will also increase the ref count
	require.Equal(t, t, cache.Get("B"))
	cache.Release("B") // get will also increase the ref count
	require.Equal(t, 2, cache.Size())

	cache.Release("B") // B's ref count is 0
	_, err = cache.PutIfNotExist("C", t)
	require.NoError(t, err)
	require.Equal(t, t, cache.Get("C"))
	cache.Release("C") // get will also increase the ref count
	require.Equal(t, 2, cache.Size())

	cache.Release("A") // A's ref count is 0
	cache.Release("C") // C's ref count is 0
	require.Equal(t, 2, cache.Size())

	timeSource.Advance(time.Millisecond * 100)
	require.Nil(t, cache.Get("A"))
	require.Nil(t, cache.Get("B"))
	require.Nil(t, cache.Get("C"))
	require.Equal(t, 0, cache.Size())
}

func TestMaxSizeWithPin_LastItem(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	cache := New(2,
		&Options{
			TTL:        time.Millisecond * 50,
			Pin:        true,
			TimeSource: timeSource,
		},
	)

	_, err := cache.PutIfNotExist("A", t)
	require.NoError(t, err)
	require.Equal(t, 1, cache.Size())

	_, err = cache.PutIfNotExist("B", t)
	require.NoError(t, err)
	require.Equal(t, 2, cache.Size())

	_, err = cache.PutIfNotExist("C", t)
	require.Error(t, err)
	require.Equal(t, 2, cache.Size())

	require.Equal(t, t, cache.Get("A"))
	cache.Release("A") // get will also increase the ref count
	require.Equal(t, t, cache.Get("B"))
	cache.Release("B") // get will also increase the ref count
	require.Equal(t, 2, cache.Size())

	cache.Release("A") // A's ref count is 0
	_, err = cache.PutIfNotExist("C", t)
	require.NoError(t, err)
	require.Equal(t, t, cache.Get("C"))
	cache.Release("C") // get will also increase the ref count
	require.Equal(t, 2, cache.Size())

	cache.Release("B") // B's ref count is 0
	cache.Release("C") // C's ref count is 0
	require.Equal(t, 2, cache.Size())

	timeSource.Advance(time.Millisecond * 100)
	require.Nil(t, cache.Get("A"))
	require.Nil(t, cache.Get("B"))
	require.Nil(t, cache.Get("C"))
	require.Equal(t, 0, cache.Size())
}

func TestIterator(t *testing.T) {
	t.Parallel()

	expected := map[string]string{
		"A": "Alpha",
		"B": "Beta",
		"G": "Gamma",
		"D": "Delta",
	}

	cache := NewLRU(5, metrics.NoopMetricsHandler)

	for k, v := range expected {
		cache.Put(k, v)
	}

	actual := map[string]string{}

	it := cache.Iterator()
	for it.HasNext() {
		entry := it.Next()
		actual[entry.Key().(string)] = entry.Value().(string)
	}
	it.Close()
	require.Equal(t, expected, actual)

	it = cache.Iterator()
	for i := 0; i < len(expected); i++ {
		entry := it.Next()
		actual[entry.Key().(string)] = entry.Value().(string)
	}
	it.Close()
	require.Equal(t, expected, actual)
}

func TestZeroSizeCache(t *testing.T) {
	t.Parallel()

	cache := NewLRU(0, metrics.NoopMetricsHandler)
	_, err := cache.PutIfNotExist("A", t)
	require.NoError(t, err)
	require.Nil(t, cache.Get("A"))
	require.Equal(t, 0, cache.Size())
	it := cache.Iterator()
	require.False(t, it.HasNext())
	it.Close()
	cache.Release("A")
	cache.Delete("A")
	v, err := cache.PutIfNotExist("A", t)
	require.Equal(t, t, v)
	require.NoError(t, err)
	require.Equal(t, 0, cache.Size())
}

func TestCache_ItemSizeTooLarge(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	res := cache.Put(uuid.New(), &testEntryWithCacheSize{maxTotalBytes})
	require.Nil(t, res)
	require.Equal(t, 10, cache.Size())

	res, err := cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{maxTotalBytes + 1})
	require.Equal(t, ErrCacheItemTooLarge, err)
	require.Nil(t, res)
	require.Equal(t, 10, cache.Size())

}

func TestCache_ItemHasCacheSizeDefined(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	numPuts := rand.Intn(1024)

	startWG := sync.WaitGroup{}
	endWG := sync.WaitGroup{}

	startWG.Add(numPuts)
	endWG.Add(numPuts)

	sizeDuringPuts := make(chan int)
	go func() {
		startWG.Wait()
		await.Snd(t, sizeDuringPuts, cache.Size())
	}()
	for range numPuts {
		go func() {
			defer endWG.Done()

			startWG.Wait()
			key := uuid.New()
			cache.Put(key, &testEntryWithCacheSize{rand.Int()})
		}()
		startWG.Done()
	}

	size := await.Rcv(t, sizeDuringPuts)
	endWG.Wait()
	require.Less(t, size, maxTotalBytes)
}

func TestCache_ItemHasCacheSizeDefined_PutWithNewKeys(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	// Put with new key and value size greater than cache size, should not be added to cache
	cache.Put(uuid.New(), &testEntryWithCacheSize{15})
	require.Equal(t, 0, cache.Size())

	// Put with new key and value size less than cache size, should be added to cache
	cache.Put(uuid.New(), &testEntryWithCacheSize{5})
	require.Equal(t, 5, cache.Size())

	// Put with new key and value size less than cache size, should evict 0 ref items and added to cache
	cache.Put(uuid.New(), &testEntryWithCacheSize{10})
	require.Equal(t, 10, cache.Size())

	// Put with new key and value size less than cache size, should evict 0 ref items until enough spaces and added to cache
	cache.Put(uuid.New(), &testEntryWithCacheSize{3})
	require.Equal(t, 3, cache.Size())
	cache.Put(uuid.New(), &testEntryWithCacheSize{7})
	require.Equal(t, 10, cache.Size())
}

func TestCache_ItemHasCacheSizeDefined_PutWithSameKeyAndDifferentSizes(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	key1 := "A"
	cache.Put(key1, &testEntryWithCacheSize{4})
	require.Equal(t, 4, cache.Size())

	key2 := "B"
	cache.Put(key2, &testEntryWithCacheSize{4})
	// 4 + 4 = 8 < 10 should not evict any items
	require.Equal(t, 8, cache.Size())
	// put same key with smaller size, should not evict any items
	cache.Put(key2, &testEntryWithCacheSize{3})
	require.Equal(t, &testEntryWithCacheSize{4}, cache.Get(key1))
	// 8 - 4 + 3 = 7 < 10, should not evict any items
	require.Equal(t, 7, cache.Size())

	// put same key with larger size, but below cache size, should not evict any items
	cache.Put(key2, &testEntryWithCacheSize{6})
	// 7 - 3 + 6 = 10 =< 10, should not evict any items
	require.Equal(t, 10, cache.Size())
	// get key1 after to make it the most recently used
	require.Equal(t, &testEntryWithCacheSize{6}, cache.Get(key2))
	require.Equal(t, &testEntryWithCacheSize{4}, cache.Get(key1))

	// put same key with larger size, but take all cache size, should evict all items
	cache.Put(key2, &testEntryWithCacheSize{10})
	// 10 - 4 - 6 + 10 = 10 =< 10, should evict all items
	require.Equal(t, 10, cache.Size())
	require.Nil(t, cache.Get(key1))
	require.Equal(t, &testEntryWithCacheSize{10}, cache.Get(key2))
}

func TestCache_ItemHasCacheSizeDefined_PutWithSameKey(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	key := uuid.New()

	// Put with same key and value size greater than cache size, should not be added to cache
	cache.Put(key, &testEntryWithCacheSize{15})
	require.Equal(t, 0, cache.Size())

	// Put with same key and value size less than cache size, should be added to cache
	cache.Put(key, &testEntryWithCacheSize{5})
	require.Equal(t, 5, cache.Size())

	// Put with same key and value size less than cache size, should be evicted until enough space and added to cache
	cache.Put(key, &testEntryWithCacheSize{10})
	require.Equal(t, 10, cache.Size())

	// Put with same key and value size less than cache size, should be evicted until enough space and added to cache
	cache.Put(key, &testEntryWithCacheSize{3})
	require.Equal(t, 3, cache.Size())
	cache.Put(key, &testEntryWithCacheSize{7})
	require.Equal(t, 7, cache.Size())
}

func TestCache_ItemHasCacheSizeDefined_PutIfNotExistWithNewKeys(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	// PutIfNotExist with new keys with size greater than cache size, should return error and not add to cache
	val, err := cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{15})
	require.Equal(t, ErrCacheItemTooLarge, err)
	require.Nil(t, val)
	require.Equal(t, 0, cache.Size())

	// PutIfNotExist with new keys with size less than cache size, should add to cache
	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{5})
	require.NoError(t, err)
	require.Equal(t, &testEntryWithCacheSize{5}, val)
	require.Equal(t, 5, cache.Size())

	// PutIfNotExist with new keys with size less than cache size, should evict item and add to cache
	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{10})
	require.NoError(t, err)
	require.Equal(t, &testEntryWithCacheSize{10}, val)
	require.Equal(t, 10, cache.Size())

	// PutIfNotExist with new keys with size less than cache size, should evict item and add to cache
	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{5})
	require.NoError(t, err)
	require.Equal(t, &testEntryWithCacheSize{5}, val)
	require.Equal(t, 5, cache.Size())
}

func TestCache_ItemHasCacheSizeDefined_PutIfNotExistWithSameKey(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)
	key := uuid.New().String()

	// PutIfNotExist with new keys with size greater than cache size, should return error and not add to cache
	val, err := cache.PutIfNotExist(key, &testEntryWithCacheSize{15})
	require.Equal(t, ErrCacheItemTooLarge, err)
	require.Nil(t, val)
	require.Equal(t, 0, cache.Size())

	// PutIfNotExist with new keys with size less than cache size, should add to cache
	val, err = cache.PutIfNotExist(key, &testEntryWithCacheSize{5})
	require.NoError(t, err)
	require.Equal(t, &testEntryWithCacheSize{5}, val)
	require.Equal(t, 5, cache.Size())

	// PutIfNotExist with same keys with size less than cache size, should not be added to cache
	val, err = cache.PutIfNotExist(key, &testEntryWithCacheSize{10})
	require.NoError(t, err)
	require.Equal(t, &testEntryWithCacheSize{5}, val)
	require.Equal(t, 5, cache.Size())
}

func TestCache_PutIfNotExistWithNewKeys_Pin(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := New(maxTotalBytes, &Options{Pin: true})

	val, err := cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{15})
	require.Equal(t, ErrCacheItemTooLarge, err)
	require.Nil(t, val)
	require.Equal(t, 0, cache.Size())

	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{3})
	require.NoError(t, err)
	require.Equal(t, &testEntryWithCacheSize{3}, val)
	require.Equal(t, 3, cache.Size())

	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{7})
	require.NoError(t, err)
	require.Equal(t, &testEntryWithCacheSize{7}, val)
	require.Equal(t, 10, cache.Size())

	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{8})
	require.Equal(t, ErrCacheFull, err)
	require.Nil(t, val)
	require.Equal(t, 10, cache.Size())
}

func TestCache_PutIfNotExistWithSameKeys_Pin(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := New(maxTotalBytes, &Options{Pin: true})

	key := uuid.New()
	val, err := cache.PutIfNotExist(key, &testEntryWithCacheSize{15})
	require.Equal(t, ErrCacheItemTooLarge, err)
	require.Nil(t, val)
	require.Equal(t, 0, cache.Size())

	val, err = cache.PutIfNotExist(key, &testEntryWithCacheSize{3})
	require.NoError(t, err)
	require.Equal(t, &testEntryWithCacheSize{3}, val)
	require.Equal(t, 3, cache.Size())

	val, err = cache.PutIfNotExist(key, &testEntryWithCacheSize{7})
	require.NoError(t, err)
	require.Equal(t, &testEntryWithCacheSize{3}, val)
	require.Equal(t, 3, cache.Size())
}

func TestCache_ItemSizeChangeBeforeRelease(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := New(maxTotalBytes,
		&Options{
			TTL:        time.Millisecond * 50,
			Pin:        true,
			TimeSource: nil,
		},
	)

	entry1 := &testEntryWithCacheSize{
		cacheSize: 1,
	}
	key1 := uuid.New()
	_, err := cache.PutIfNotExist(key1, entry1)
	require.NoError(t, err)
	require.Equal(t, 1, cache.Size())

	entry1.cacheSize = 5
	cache.Release(key1)
	require.Equal(t, 5, cache.Size())

	_, err = cache.PutIfNotExist(key1, entry1)
	require.NoError(t, err)
	require.Equal(t, 5, cache.Size())
	entry1.cacheSize = 10
	cache.Release(key1)
	require.Equal(t, 10, cache.Size())

	// Inserting another entry when cache is full. entry1 should be evicted from cache.
	entry2 := &testEntryWithCacheSize{
		cacheSize: 2,
	}
	key2 := uuid.New()
	_, err = cache.PutIfNotExist(key2, entry2)
	require.NoError(t, err)
	require.Equal(t, 2, cache.Size())

	// Inserting entry1 again to make cache full again.
	entry1.cacheSize = 8
	_, err = cache.PutIfNotExist(key1, entry1)
	require.NoError(t, err)
	require.Equal(t, 10, cache.Size())
	// Increasing the size of entry1 before releasing. This will make the cache size > max limit.
	entry1.cacheSize = 10
	cache.Release(key1)
	// Cache should have evicted entry1 to bring cache size under max limit.
	require.Equal(t, 2, cache.Size())
}

func TestCache_InvokeLifecycleCallbacks(t *testing.T) {
	t.Parallel()

	var onPut, onEvict int
	ttl := time.Millisecond * 50
	timeSource := clock.NewEventTimeSource()
	cache := New(5,
		&Options{
			TTL:        ttl,
			TimeSource: timeSource,
			OnPut: func(val any) {
				require.Equal(t, "value", val)
				onPut++
			},
			OnEvict: func(val any) {
				require.Equal(t, "value", val)
				onEvict++
			},
		},
	)

	cache.Put("key", "value")
	cache.Put("key", "value")
	require.Equal(t, 2, onPut, "expected OnPut callback to be invoked twice")

	_, _ = cache.PutIfNotExist("key", "value")
	require.Equal(t, 2, onPut, "expected OnPut callback to *not* be invoked again")
	require.Equal(t, 0, onEvict, "expected OnEvict callback to be *not* be invoked")

	cache.Delete("key")
	require.Equal(t, 1, onEvict, "expected OnEvict callback to be invoked")

	cache.Put("key", "value")
	timeSource.Advance(2 * ttl)
	require.Nil(t, cache.Get("key"))
	require.Equal(t, 2, onEvict, "expected OnEvict callback to be invoked")
}

func TestCache_UnusedExpiry(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	ttl := 10 * time.Minute
	loopInterval := 1 * time.Minute
	timeSource := clock.NewEventTimeSource()

	cache := New(5,
		&Options{
			TTL:        ttl,
			TimeSource: timeSource,
			BackgroundEvict: func() dynamicconfig.CacheBackgroundEvictSettings {
				return dynamicconfig.CacheBackgroundEvictSettings{
					Enabled:         true,
					LoopInterval:    loopInterval,
					MaxEntryPerCall: 1,
				}
			},
		},
	)

	cache.Put(1, 1)
	r.Equal(1, cache.Size())

	await.Require(t.Context(), t, func(t *await.T) {
		timeSource.Advance(loopInterval)
		require.Equal(t, 0, cache.Size())
	}, 2*time.Second, 100*time.Millisecond)

	cache.Put(2, 2)
	timeSource.Advance(ttl / 2)
	cache.Put(3, 3)
	r.Equal(2, cache.Size())

	await.Require(t.Context(), t, func(t *await.T) {
		timeSource.Advance(loopInterval)
		require.Equal(t, 1, cache.Size())
		require.Nil(t, cache.Get(2))
		require.Equal(t, 3, cache.Get(3))
	}, 2*time.Second, 100*time.Millisecond)

	await.Require(t.Context(), t, func(t *await.T) {
		timeSource.Advance(loopInterval)
		require.Equal(t, 0, cache.Size())
		require.Nil(t, cache.Get(2))
		require.Nil(t, cache.Get(3))
	}, 2*time.Second, 100*time.Millisecond)

	// Stop the background goroutine, confirm no active expiration.
	cache.Put(4, 4)
	cache.Stop()
	l, ok := cache.(*lru)
	r.True(ok)
	c := make(chan struct{})
	go func() {
		l.loops.Wait()
		close(c)
	}()
	await.Rcv(t, c)
	timeSource.Advance(ttl + 1*time.Second)
	// The cache should still have entry 4,
	r.Equal(1, cache.Size())
	// but this Get call will check the (hard) ttl & expire it.
	r.Nil(cache.Get(4))
}

func TestCache_UnusedExpiryPin(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	ttl := 10 * time.Minute
	loopInterval := 1 * time.Minute
	timeSource := clock.NewEventTimeSource()

	cache := New(5,
		&Options{
			TTL:        ttl,
			Pin:        true,
			TimeSource: timeSource,
			BackgroundEvict: func() dynamicconfig.CacheBackgroundEvictSettings {
				return dynamicconfig.CacheBackgroundEvictSettings{
					Enabled:         true,
					LoopInterval:    loopInterval,
					MaxEntryPerCall: 1,
				}
			},
		},
	)

	_, err := cache.PutIfNotExist(1, 1)
	r.NoError(err)
	timeSource.Advance(ttl / 2)
	cache.Release(1)
	_, err = cache.PutIfNotExist(2, 2)
	r.NoError(err)
	r.Equal(2, cache.Size())

	await.Require(t.Context(), t, func(t *await.T) {
		timeSource.Advance(loopInterval)
		require.Equal(t, 1, cache.Size())
		require.Nil(t, cache.Get(1))
	}, 1*time.Second, 100*time.Millisecond)

	cache.Release(2)

	await.Require(t.Context(), t, func(t *await.T) {
		timeSource.Advance(loopInterval)
		require.Equal(t, 0, cache.Size())
	}, 1*time.Second, 100*time.Millisecond)
}
