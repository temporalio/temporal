package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestEvictOnFinalUnpin(t *testing.T) {
	t.Parallel()
	var evicted int
	handler := metricstest.NewCaptureHandler()
	c := NewWithMetrics(100, &Options{
		Pin:          true,
		EvictOnUnpin: func(value any) bool { return value.(*testEntryWithCacheSize).cacheSize == 0 },
		OnEvict:      func(any) { evicted++ },
	}, handler)
	t.Cleanup(c.Stop)
	item := &testEntryWithCacheSize{cacheSize: 10}
	_, err := c.PutIfNotExist("workflow", item)
	require.NoError(t, err)
	require.Same(t, item, c.Get("workflow"))
	item.cacheSize = 0
	c.Release("workflow")
	require.Zero(t, evicted, "another caller still owns the context")
	capture := handler.StartCapture()
	c.Release("workflow")
	require.Equal(t, 1, evicted)
	require.Nil(t, c.Get("workflow"))
	require.Zero(t, c.Size())
	snapshot := capture.Snapshot()
	require.Len(t, snapshot[metrics.CacheUsage.Name()], 1)
	require.Zero(t, snapshot[metrics.CacheUsage.Name()][0].Value)
}

func TestReusableStateSurvivesUnpin(t *testing.T) {
	t.Parallel()
	c := New(100, &Options{Pin: true, EvictOnUnpin: func(value any) bool {
		return value.(*testEntryWithCacheSize).cacheSize == 0
	}})
	t.Cleanup(c.Stop)
	item := &testEntryWithCacheSize{cacheSize: 10}
	_, err := c.PutIfNotExist("workflow", item)
	require.NoError(t, err)
	c.Release("workflow")
	require.Same(t, item, c.Get("workflow"))
	c.Release("workflow")
}

func TestEmptyContextChurnRetainsNoEntries(t *testing.T) {
	t.Parallel()
	c := New(100, &Options{Pin: true, EvictOnUnpin: func(any) bool { return true }})
	t.Cleanup(c.Stop)
	for i := range 10000 {
		_, err := c.PutIfNotExist(i, &testEntryWithCacheSize{cacheSize: 1})
		require.NoError(t, err)
		c.Release(i)
	}
	iterator := c.Iterator()
	hasEntries := iterator.HasNext()
	iterator.Close()
	require.False(t, hasEntries)
	require.Zero(t, c.Size())
}
