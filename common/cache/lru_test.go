// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cache

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
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

	cache := NewLRU(4, metricsHandler)

	cache.Put("A", "Foo")
	assert.Equal(t, "Foo", cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Equal(t, 1, cache.Size())
	snapshot := capture.Snapshot()
	assert.Equal(t, float64(4), snapshot[metrics.CacheSize.Name()][0].Value)
	assert.Equal(t, float64(1), snapshot[metrics.CacheUsage.Name()][0].Value)

	capture = metricsHandler.StartCapture()
	cache.Put("B", "Bar")
	cache.Put("C", "Cid")
	cache.Put("D", "Delt")
	assert.Equal(t, 4, cache.Size())
	snapshot = capture.Snapshot()
	assert.Equal(t, float64(4), snapshot[metrics.CacheUsage.Name()][2].Value)

	assert.Equal(t, "Bar", cache.Get("B"))
	assert.Equal(t, "Cid", cache.Get("C"))
	assert.Equal(t, "Delt", cache.Get("D"))

	cache.Put("A", "Foo2")
	assert.Equal(t, "Foo2", cache.Get("A"))
	assert.Equal(t, 4, cache.Size())

	capture = metricsHandler.StartCapture()
	cache.Put("E", "Epsi")
	assert.Equal(t, "Epsi", cache.Get("E"))
	assert.Equal(t, "Foo2", cache.Get("A"))
	assert.Nil(t, cache.Get("B")) // Oldest, should be evicted
	assert.Equal(t, 4, cache.Size())
	snapshot = capture.Snapshot()
	assert.Equal(t, 2, len(snapshot[metrics.CacheUsage.Name()]))
	assert.Equal(t, float64(4), snapshot[metrics.CacheUsage.Name()][1].Value)

	// Access C, D is now LRU
	cache.Get("C")
	cache.Put("F", "Felp")
	assert.Nil(t, cache.Get("D"))
	assert.Equal(t, 4, cache.Size())

	capture = metricsHandler.StartCapture()
	cache.Delete("A")
	assert.Nil(t, cache.Get("A"))
	assert.Equal(t, 3, cache.Size())
	snapshot = capture.Snapshot()
	assert.Equal(t, 1, len(snapshot[metrics.CacheUsage.Name()]))
	assert.Equal(t, float64(3), snapshot[metrics.CacheUsage.Name()][0].Value)
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

	assert.Equal(t, value, cache.Get(key))
	assert.Equal(t, value, cache.Get(keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}))
	assert.Nil(t, cache.Get(keyType{
		dummyString: "some other random key",
		dummyInt:    56,
	}))
	assert.Equal(t, 1, cache.Size())

	cache.Put(key, "some other random value")
	assert.Equal(t, "some other random value", cache.Get(key))
	assert.Equal(t, 1, cache.Size())
}

func TestLRUWithTTL(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	cache := New(5,
		&Options{
			TTL:        time.Millisecond * 100,
			TimeSource: timeSource,
		},
		metricsHandler,
	)
	cache.Put("A", "foo")
	assert.Equal(t, "foo", cache.Get("A"))
	snapshot := capture.Snapshot()
	assert.Equal(t, float64(5), snapshot[metrics.CacheSize.Name()][0].Value)
	assert.Equal(t, float64(1), snapshot[metrics.CacheUsage.Name()][0].Value)
	assert.Equal(t, time.Millisecond*100, snapshot[metrics.CacheTtl.Name()][0].Value)
	assert.Equal(t, time.Duration(0), snapshot[metrics.CacheEntryAgeOnGet.Name()][0].Value)
	timeSource.Advance(time.Millisecond * 300)
	assert.Nil(t, cache.Get("A"))
	snapshot = capture.Snapshot()
	assert.Equal(t, 2, len(snapshot[metrics.CacheUsage.Name()]))
	assert.Equal(t, float64(0), snapshot[metrics.CacheUsage.Name()][1].Value)
	assert.Equal(t, 0, cache.Size())
	assert.Equal(t, 2, len(snapshot[metrics.CacheEntryAgeOnGet.Name()]))
	assert.Equal(t, time.Millisecond*300, snapshot[metrics.CacheEntryAgeOnGet.Name()][1].Value)
	assert.Equal(t, time.Millisecond*300, snapshot[metrics.CacheEntryAgeOnEviction.Name()][0].Value)
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
	for i := 0; i < 20; i++ {
		wg.Add(2)

		// concurrent get and put
		go func() {
			defer wg.Done()

			<-start

			for j := 0; j < 1000; j++ {
				cache.Get("A")
				cache.Put("A", "fooo")
			}
		}()

		// concurrent iteration
		go func() {
			defer wg.Done()

			<-start

			for j := 0; j < 50; j++ {
				it := cache.Iterator()
				for it.HasNext() {
					_ = it.Next()
				}
				it.Close()
			}
		}()
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
		metrics.NoopMetricsHandler,
	)

	cache.Put("A", t)
	assert.Equal(t, t, cache.Get("A"))
	timeSource.Advance(time.Millisecond * 100)
	assert.Nil(t, cache.Get("A"))
}

func TestTTLWithPin(t *testing.T) {
	t.Parallel()

	timeSource := clock.NewEventTimeSource()
	metricsHandler := metricstest.NewCaptureHandler()
	cache := New(5,
		&Options{
			TTL:        time.Millisecond * 50,
			Pin:        true,
			TimeSource: timeSource,
		},
		metricsHandler,
	)

	capture := metricsHandler.StartCapture()
	_, err := cache.PutIfNotExist("A", t)
	assert.NoError(t, err)
	assert.Equal(t, t, cache.Get("A"))
	assert.Equal(t, 1, cache.Size())
	snapshot := capture.Snapshot()
	assert.Equal(t, float64(1), snapshot[metrics.CacheUsage.Name()][0].Value)
	assert.Equal(t, float64(1), snapshot[metrics.CachePinnedUsage.Name()][0].Value)
	capture = metricsHandler.StartCapture()
	timeSource.Advance(time.Millisecond * 100)
	assert.Equal(t, t, cache.Get("A"))
	assert.Equal(t, 1, cache.Size())
	// release 3 time since put if not exist also increase the counter
	cache.Release("A")
	cache.Release("A")
	cache.Release("A")
	snapshot = capture.Snapshot()
	assert.Equal(t, float64(0), snapshot[metrics.CachePinnedUsage.Name()][0].Value)
	assert.Nil(t, cache.Get("A"))
	assert.Equal(t, 0, cache.Size())
	snapshot = capture.Snapshot()
	// cache.Release() will emit cacheUsage 3 times. cache.Get() will emit cacheUsage once.
	assert.Equal(t, float64(0), snapshot[metrics.CacheUsage.Name()][3].Value)
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
		metrics.NoopMetricsHandler,
	)

	_, err := cache.PutIfNotExist("A", t)
	assert.NoError(t, err)
	assert.Equal(t, 1, cache.Size())

	_, err = cache.PutIfNotExist("B", t)
	assert.NoError(t, err)
	assert.Equal(t, 2, cache.Size())

	_, err = cache.PutIfNotExist("C", t)
	assert.Error(t, err)
	assert.Equal(t, 2, cache.Size())

	assert.Equal(t, t, cache.Get("A"))
	cache.Release("A") // get will also increase the ref count
	assert.Equal(t, t, cache.Get("B"))
	cache.Release("B") // get will also increase the ref count
	assert.Equal(t, 2, cache.Size())

	cache.Release("B") // B's ref count is 0
	_, err = cache.PutIfNotExist("C", t)
	assert.NoError(t, err)
	assert.Equal(t, t, cache.Get("C"))
	cache.Release("C") // get will also increase the ref count
	assert.Equal(t, 2, cache.Size())

	cache.Release("A") // A's ref count is 0
	cache.Release("C") // C's ref count is 0
	assert.Equal(t, 2, cache.Size())

	timeSource.Advance(time.Millisecond * 100)
	assert.Nil(t, cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Nil(t, cache.Get("C"))
	assert.Equal(t, 0, cache.Size())
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
		metrics.NoopMetricsHandler,
	)

	_, err := cache.PutIfNotExist("A", t)
	assert.NoError(t, err)
	assert.Equal(t, 1, cache.Size())

	_, err = cache.PutIfNotExist("B", t)
	assert.NoError(t, err)
	assert.Equal(t, 2, cache.Size())

	_, err = cache.PutIfNotExist("C", t)
	assert.Error(t, err)
	assert.Equal(t, 2, cache.Size())

	assert.Equal(t, t, cache.Get("A"))
	cache.Release("A") // get will also increase the ref count
	assert.Equal(t, t, cache.Get("B"))
	cache.Release("B") // get will also increase the ref count
	assert.Equal(t, 2, cache.Size())

	cache.Release("A") // A's ref count is 0
	_, err = cache.PutIfNotExist("C", t)
	assert.NoError(t, err)
	assert.Equal(t, t, cache.Get("C"))
	cache.Release("C") // get will also increase the ref count
	assert.Equal(t, 2, cache.Size())

	cache.Release("B") // B's ref count is 0
	cache.Release("C") // C's ref count is 0
	assert.Equal(t, 2, cache.Size())

	timeSource.Advance(time.Millisecond * 100)
	assert.Nil(t, cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Nil(t, cache.Get("C"))
	assert.Equal(t, 0, cache.Size())
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
	assert.Equal(t, expected, actual)

	it = cache.Iterator()
	for i := 0; i < len(expected); i++ {
		entry := it.Next()
		actual[entry.Key().(string)] = entry.Value().(string)
	}
	it.Close()
	assert.Equal(t, expected, actual)
}

func TestZeroSizeCache(t *testing.T) {
	t.Parallel()

	cache := NewLRU(0, metrics.NoopMetricsHandler)
	_, err := cache.PutIfNotExist("A", t)
	assert.NoError(t, err)
	assert.Equal(t, nil, cache.Get("A"))
	assert.Equal(t, 0, cache.Size())
	it := cache.Iterator()
	assert.False(t, it.HasNext())
	it.Close()
	cache.Release("A")
	cache.Delete("A")
	v, err := cache.PutIfNotExist("A", t)
	assert.Equal(t, v, t)
	assert.Nil(t, err)
	assert.Equal(t, 0, cache.Size())
}

func TestCache_ItemSizeTooLarge(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	res := cache.Put(uuid.New(), &testEntryWithCacheSize{maxTotalBytes})
	assert.Equal(t, res, nil)
	assert.Equal(t, 10, cache.Size())

	res, err := cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{maxTotalBytes + 1})
	assert.Equal(t, err, ErrCacheItemTooLarge)
	assert.Equal(t, res, nil)
	assert.Equal(t, 10, cache.Size())

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

	go func() {
		startWG.Wait()
		assert.True(t, cache.Size() < maxTotalBytes)
	}()
	for i := 0; i < numPuts; i++ {
		go func() {
			defer endWG.Done()

			startWG.Wait()
			key := uuid.New()
			cache.Put(key, &testEntryWithCacheSize{rand.Int()})
		}()
		startWG.Done()
	}

	endWG.Wait()
}

func TestCache_ItemHasCacheSizeDefined_PutWithNewKeys(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	// Put with new key and value size greater than cache size, should not be added to cache
	cache.Put(uuid.New(), &testEntryWithCacheSize{15})
	assert.Equal(t, 0, cache.Size())

	// Put with new key and value size less than cache size, should be added to cache
	cache.Put(uuid.New(), &testEntryWithCacheSize{5})
	assert.Equal(t, 5, cache.Size())

	// Put with new key and value size less than cache size, should evict 0 ref items and added to cache
	cache.Put(uuid.New(), &testEntryWithCacheSize{10})
	assert.Equal(t, 10, cache.Size())

	// Put with new key and value size less than cache size, should evict 0 ref items until enough spaces and added to cache
	cache.Put(uuid.New(), &testEntryWithCacheSize{3})
	assert.Equal(t, 3, cache.Size())
	cache.Put(uuid.New(), &testEntryWithCacheSize{7})
	assert.Equal(t, 10, cache.Size())
}

func TestCache_ItemHasCacheSizeDefined_PutWithSameKeyAndDifferentSizes(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	key1 := "A"
	cache.Put(key1, &testEntryWithCacheSize{4})
	assert.Equal(t, 4, cache.Size())

	key2 := "B"
	cache.Put(key2, &testEntryWithCacheSize{4})
	// 4 + 4 = 8 < 10 should not evict any items
	assert.Equal(t, 8, cache.Size())
	// put same key with smaller size, should not evict any items
	cache.Put(key2, &testEntryWithCacheSize{3})
	assert.Equal(t, cache.Get(key1), &testEntryWithCacheSize{4})
	// 8 - 4 + 3 = 7 < 10, should not evict any items
	assert.Equal(t, 7, cache.Size())

	// put same key with larger size, but below cache size, should not evict any items
	cache.Put(key2, &testEntryWithCacheSize{6})
	// 7 - 3 + 6 = 10 =< 10, should not evict any items
	assert.Equal(t, 10, cache.Size())
	// get key1 after to make it the most recently used
	assert.Equal(t, cache.Get(key2), &testEntryWithCacheSize{6})
	assert.Equal(t, cache.Get(key1), &testEntryWithCacheSize{4})

	// put same key with larger size, but take all cache size, should evict all items
	cache.Put(key2, &testEntryWithCacheSize{10})
	// 10 - 4 - 6 + 10 = 10 =< 10, should evict all items
	assert.Equal(t, 10, cache.Size())
	assert.Equal(t, cache.Get(key1), nil)
	assert.Equal(t, cache.Get(key2), &testEntryWithCacheSize{10})
}

func TestCache_ItemHasCacheSizeDefined_PutWithSameKey(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	key := uuid.New()

	// Put with same key and value size greater than cache size, should not be added to cache
	cache.Put(key, &testEntryWithCacheSize{15})
	assert.Equal(t, 0, cache.Size())

	// Put with same key and value size less than cache size, should be added to cache
	cache.Put(key, &testEntryWithCacheSize{5})
	assert.Equal(t, 5, cache.Size())

	// Put with same key and value size less than cache size, should be evicted until enough space and added to cache
	cache.Put(key, &testEntryWithCacheSize{10})
	assert.Equal(t, 10, cache.Size())

	// Put with same key and value size less than cache size, should be evicted until enough space and added to cache
	cache.Put(key, &testEntryWithCacheSize{3})
	assert.Equal(t, 3, cache.Size())
	cache.Put(key, &testEntryWithCacheSize{7})
	assert.Equal(t, 7, cache.Size())
}

func TestCache_ItemHasCacheSizeDefined_PutIfNotExistWithNewKeys(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)

	// PutIfNotExist with new keys with size greater than cache size, should return error and not add to cache
	val, err := cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{15})
	assert.Equal(t, ErrCacheItemTooLarge, err)
	assert.Nil(t, val)
	assert.Equal(t, 0, cache.Size())

	// PutIfNotExist with new keys with size less than cache size, should add to cache
	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{5})
	assert.NoError(t, err)
	assert.Equal(t, &testEntryWithCacheSize{5}, val)
	assert.Equal(t, 5, cache.Size())

	// PutIfNotExist with new keys with size less than cache size, should evict item and add to cache
	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{10})
	assert.NoError(t, err)
	assert.Equal(t, &testEntryWithCacheSize{10}, val)
	assert.Equal(t, 10, cache.Size())

	// PutIfNotExist with new keys with size less than cache size, should evict item and add to cache
	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{5})
	assert.NoError(t, err)
	assert.Equal(t, &testEntryWithCacheSize{5}, val)
	assert.Equal(t, 5, cache.Size())
}

func TestCache_ItemHasCacheSizeDefined_PutIfNotExistWithSameKey(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := NewLRU(maxTotalBytes, metrics.NoopMetricsHandler)
	key := uuid.New().String()

	// PutIfNotExist with new keys with size greater than cache size, should return error and not add to cache
	val, err := cache.PutIfNotExist(key, &testEntryWithCacheSize{15})
	assert.Equal(t, ErrCacheItemTooLarge, err)
	assert.Nil(t, val)
	assert.Equal(t, 0, cache.Size())

	// PutIfNotExist with new keys with size less than cache size, should add to cache
	val, err = cache.PutIfNotExist(key, &testEntryWithCacheSize{5})
	assert.NoError(t, err)
	assert.Equal(t, &testEntryWithCacheSize{5}, val)
	assert.Equal(t, 5, cache.Size())

	// PutIfNotExist with same keys with size less than cache size, should not be added to cache
	val, err = cache.PutIfNotExist(key, &testEntryWithCacheSize{10})
	assert.NoError(t, err)
	assert.Equal(t, &testEntryWithCacheSize{5}, val)
	assert.Equal(t, 5, cache.Size())
}

func TestCache_PutIfNotExistWithNewKeys_Pin(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := New(maxTotalBytes, &Options{Pin: true}, metrics.NoopMetricsHandler)

	val, err := cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{15})
	assert.Equal(t, ErrCacheItemTooLarge, err)
	assert.Nil(t, val)
	assert.Equal(t, 0, cache.Size())

	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{3})
	assert.NoError(t, err)
	assert.Equal(t, &testEntryWithCacheSize{3}, val)
	assert.Equal(t, 3, cache.Size())

	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{7})
	assert.NoError(t, err)
	assert.Equal(t, &testEntryWithCacheSize{7}, val)
	assert.Equal(t, 10, cache.Size())

	val, err = cache.PutIfNotExist(uuid.New(), &testEntryWithCacheSize{8})
	assert.Equal(t, ErrCacheFull, err)
	assert.Nil(t, val)
	assert.Equal(t, 10, cache.Size())
}

func TestCache_PutIfNotExistWithSameKeys_Pin(t *testing.T) {
	t.Parallel()

	maxTotalBytes := 10
	cache := New(maxTotalBytes, &Options{Pin: true}, metrics.NoopMetricsHandler)

	key := uuid.New()
	val, err := cache.PutIfNotExist(key, &testEntryWithCacheSize{15})
	assert.Equal(t, ErrCacheItemTooLarge, err)
	assert.Nil(t, val)
	assert.Equal(t, 0, cache.Size())

	val, err = cache.PutIfNotExist(key, &testEntryWithCacheSize{3})
	assert.NoError(t, err)
	assert.Equal(t, &testEntryWithCacheSize{3}, val)
	assert.Equal(t, 3, cache.Size())

	val, err = cache.PutIfNotExist(key, &testEntryWithCacheSize{7})
	assert.NoError(t, err)
	assert.Equal(t, &testEntryWithCacheSize{3}, val)
	assert.Equal(t, 3, cache.Size())
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
		metrics.NoopMetricsHandler,
	)

	entry1 := &testEntryWithCacheSize{
		cacheSize: 1,
	}
	key1 := uuid.New()
	_, err := cache.PutIfNotExist(key1, entry1)
	assert.NoError(t, err)
	assert.Equal(t, 1, cache.Size())

	entry1.cacheSize = 5
	cache.Release(key1)
	assert.Equal(t, 5, cache.Size())

	_, err = cache.PutIfNotExist(key1, entry1)
	assert.NoError(t, err)
	assert.Equal(t, 5, cache.Size())
	entry1.cacheSize = 10
	cache.Release(key1)
	assert.Equal(t, 10, cache.Size())

	// Inserting another entry when cache is full. entry1 should be evicted from cache.
	entry2 := &testEntryWithCacheSize{
		cacheSize: 2,
	}
	key2 := uuid.New()
	_, err = cache.PutIfNotExist(key2, entry2)
	assert.NoError(t, err)
	assert.Equal(t, 2, cache.Size())

	// Inserting entry1 again to make cache full again.
	entry1.cacheSize = 8
	_, err = cache.PutIfNotExist(key1, entry1)
	assert.NoError(t, err)
	assert.Equal(t, 10, cache.Size())
	// Increasing the size of entry1 before releasing. This will make the cache size > max limit.
	entry1.cacheSize = 10
	cache.Release(key1)
	// Cache should have evicted entry1 to bring cache size under max limit.
	assert.Equal(t, 2, cache.Size())
}
