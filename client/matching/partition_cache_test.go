package matching

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/metrics"
)

// Using a fixed UUID so tests are deterministic. Must be valid UUID format for makeKey.
const testNsID = "f47ac10b-58cc-4372-a567-0e02b2c3d479"

func TestPartitionCache_BasicPutLookup(t *testing.T) {
	t.Parallel()
	c := newPartitionCache(metrics.NoopMetricsHandler)
	c.Start()
	defer c.Stop()

	key := c.makeKey(testNsID, "my-tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	keyb := c.makeKey(testNsID, "my-tq-b", enumspb.TASK_QUEUE_TYPE_WORKFLOW)

	pc4 := PartitionCounts{Read: 4, Write: 4}
	pc8 := PartitionCounts{Read: 8, Write: 8}

	c.put(key, pc4)
	c.put(keyb, pc8)
	require.Equal(t, pc4, c.lookup(key))
	require.Equal(t, pc8, c.lookup(keyb))

	c.put(key, pc8)
	require.Equal(t, pc8, c.lookup(key))

	// missing key
	keyc := c.makeKey(testNsID, "nonexistent", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	require.Equal(t, PartitionCounts{}, c.lookup(keyc))

	// removes
	c.put(key, PartitionCounts{Read: -3, Write: -5})
	require.Equal(t, PartitionCounts{}, c.lookup(key))
}

func TestPartitionCache_Rotate(t *testing.T) {
	t.Parallel()
	c := newPartitionCache(metrics.NoopMetricsHandler)
	c.Start()
	defer c.Stop()

	key := c.makeKey(testNsID, "my-tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	pc := PartitionCounts{Read: 8, Write: 8}
	c.put(key, pc)

	// Rotate the shard — entry moves to prev
	c.shards[c.shardFromKey(key)].rotate()

	// Lookup should still find it (promotes from prev to active)
	require.Equal(t, pc, c.lookup(key))

	// After promotion, rotate again — it should still be in active
	c.shards[c.shardFromKey(key)].rotate()
	require.Equal(t, pc, c.lookup(key))

	// Rotate twice without any lookup — entry should be gone
	c.shards[c.shardFromKey(key)].rotate()
	c.shards[c.shardFromKey(key)].rotate()

	require.Equal(t, PartitionCounts{}, c.lookup(key))
}

func TestPartitionCache_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	c := newPartitionCache(metrics.NoopMetricsHandler)
	c.Start()
	defer c.Stop()

	var wg sync.WaitGroup
	for g := range 20 {
		wg.Go(func() {
			for range 1000 {
				key := c.makeKey(testNsID, "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
				pc := PartitionCounts{Read: int32(g + 1), Write: int32(g + 1)}
				c.put(key, pc)
				c.lookup(key)
			}
		})
	}
	wg.Wait()
	// no panic or data races detected
}
