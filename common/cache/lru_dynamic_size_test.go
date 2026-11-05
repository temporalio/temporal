package cache

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestLRUDynamicSizePreservesEntriesAndTTL(t *testing.T) {
	t.Parallel()
	size := 1
	timeSource := clock.NewEventTimeSource()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	c := NewWithMetrics(100, &Options{
		MaxSize:    func() int { return size },
		TTL:        time.Hour,
		TimeSource: timeSource,
	}, handler)
	t.Cleanup(c.Stop)
	c.Put("a", "a")
	timeSource.Advance(30 * time.Minute)
	size = 2
	c.Put("b", "b")
	require.Equal(t, "a", c.Get("a"))
	require.Equal(t, "b", c.Get("b"))
	require.Equal(t, 2, c.Size())
	recordings := capture.SnapshotMetric(metrics.CacheSize.Name())
	require.Len(t, recordings, 2)
	require.InDelta(t, float64(1), recordings[0].Value, 0)
	require.InDelta(t, float64(2), recordings[1].Value, 0)
	timeSource.Advance(31 * time.Minute)
	require.Nil(t, c.Get("a"))
	require.Equal(t, "b", c.Get("b"))
}

func TestLRUDynamicSizeShrink(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"get", "put"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			size := 6
			c := New(100, &Options{MaxSize: func() int { return size }})
			t.Cleanup(c.Stop)
			for _, key := range []string{"a", "b", "c"} {
				c.Put(key, &testEntryWithCacheSize{cacheSize: 2})
			}
			c.Get("a") // b is now the least recently used entry.
			size = 4
			if operation == "get" {
				c.Get("a")
			} else {
				c.Put("a", &testEntryWithCacheSize{cacheSize: 2})
			}
			require.Equal(t, 4, c.Size())
			require.Nil(t, c.Get("b"))
			require.NotNil(t, c.Get("a"))
			require.NotNil(t, c.Get("c"))
		})
	}
}

func TestLRUDynamicSizeDisableAndReenable(t *testing.T) {
	t.Parallel()
	for _, disabledSize := range []int{0, -1} {
		t.Run(strconv.Itoa(disabledSize), func(t *testing.T) {
			t.Parallel()
			size := 2
			c := New(100, &Options{MaxSize: func() int { return size }})
			t.Cleanup(c.Stop)
			c.Put("a", "a")
			size = disabledSize
			require.Nil(t, c.Get("a"))
			require.Zero(t, c.Size())
			c.Put("b", "b")
			require.Zero(t, c.Size())
			size = 2
			c.Put("c", "c")
			require.Equal(t, "c", c.Get("c"))
			require.Nil(t, c.Get("a"))
			require.Nil(t, c.Get("b"))
		})
	}
}

func TestLRUDynamicSizePreservesPinnedEntries(t *testing.T) {
	t.Parallel()
	size := 2
	c := New(100, &Options{MaxSize: func() int { return size }, Pin: true})
	t.Cleanup(c.Stop)
	_, err := c.PutIfNotExist("a", "a")
	require.NoError(t, err)
	_, err = c.PutIfNotExist("b", "b")
	require.NoError(t, err)
	c.Release("b")
	size = 0
	require.Nil(t, c.Get("a"))
	require.Equal(t, 1, c.Size(), "the pinned entry must survive shrinking")
	c.Release("a")
	require.Zero(t, c.Size(), "release must evict the final entry even when capacity is zero")
}

func TestLRUDynamicSizeConcurrentAccess(t *testing.T) {
	t.Parallel()
	var size atomic.Int64
	size.Store(2)
	c := New(100, &Options{MaxSize: func() int { return int(size.Load()) }})
	t.Cleanup(c.Stop)
	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Go(func() {
			for i := range 100 {
				size.Store(int64(i % 4))
				c.Put(worker, i)
				c.Get(worker)
				c.Delete(worker)
			}
		})
	}
	wg.Wait()
	size.Store(2)
	c.Put("a", "a")
	c.Put("b", "b")
	require.Equal(t, "a", c.Get("a"))
	require.Equal(t, "b", c.Get("b"))
	require.Equal(t, 2, c.Size())
}
