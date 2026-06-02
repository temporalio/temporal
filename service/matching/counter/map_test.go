package counter

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapCounter_Basic(t *testing.T) {
	m := NewMapCounter(100)

	assert.Equal(t, int64(1), m.GetPass("a", 0, 1))
	assert.Equal(t, int64(3), m.GetPass("a", 0, 2))
	assert.Equal(t, int64(10), m.GetPass("b", 0, 10))
	assert.Equal(t, 2, m.EstimateDistinctKeys())
}

func TestMapCounter_TopK(t *testing.T) {
	m := NewMapCounter(3)

	m.GetPass("low1", 0, 1)
	m.GetPass("low2", 0, 2)
	m.GetPass("mid", 0, 5)
	m.GetPass("high1", 0, 10)
	m.GetPass("high2", 0, 8)

	assert.ElementsMatch(t, []TopKEntry{
		TopKEntry{Key: "high1", Count: 10},
		TopKEntry{Key: "high2", Count: 8},
		TopKEntry{Key: "mid", Count: 5},
	}, m.TopK())
}

func TestMapCounter_TopK_Update(t *testing.T) {
	m := NewMapCounter(2)

	// Start with two entries
	m.GetPass("a", 0, 1)
	m.GetPass("b", 0, 2)

	topK := m.TopK()
	assert.Len(t, topK, 2)

	// Update "a" to have the highest count
	m.GetPass("a", 0, 100)

	assert.ElementsMatch(t, []TopKEntry{
		TopKEntry{Key: "a", Count: 101},
		TopKEntry{Key: "b", Count: 2},
	}, m.TopK())
}

func TestMapCounter_TopK_Eviction(t *testing.T) {
	m := NewMapCounter(2)

	m.GetPass("a", 0, 10)
	m.GetPass("b", 0, 20)

	// "c" with count 5 should not evict anything
	m.GetPass("c", 0, 5)
	assert.ElementsMatch(t, []TopKEntry{
		TopKEntry{Key: "a", Count: 10},
		TopKEntry{Key: "b", Count: 20},
	}, m.TopK())

	// "d" with count 15 should evict "a"
	m.GetPass("d", 0, 15)
	assert.ElementsMatch(t, []TopKEntry{
		TopKEntry{Key: "d", Count: 15},
		TopKEntry{Key: "b", Count: 20},
	}, m.TopK())
}

func TestMapCounter_TopK_ManyEntries(t *testing.T) {
	m := NewMapCounter(5)

	for i := range 100 {
		m.GetPass(fmt.Sprintf("key%d", i), 0, int64(i))
	}

	assert.ElementsMatch(t, []TopKEntry{
		TopKEntry{Key: "key99", Count: 99},
		TopKEntry{Key: "key98", Count: 98},
		TopKEntry{Key: "key97", Count: 97},
		TopKEntry{Key: "key96", Count: 96},
		TopKEntry{Key: "key95", Count: 95},
	}, m.TopK())
}

func TestMapCounter_HeapCorrectness(t *testing.T) {
	m := NewMapCounter(10)

	// add three keys
	m.GetPass("key1", 0, 100)
	m.GetPass("key2", 0, 200)
	m.GetPass("key3", 0, 50)

	// key3 (50) should be at root (index 0)
	assert.Equal(t, "key3", m.heap[0].Key)

	// update key3 to be the largest
	m.GetPass("key3", 0, 300)

	// key3 count is now 350. it should have moved down.
	// key1 (100) should now be the new root.
	assert.Equal(t, "key1", m.heap[0].Key)
	assert.Equal(t, int64(100), m.heap[0].Count)

	// verify map and heap are in sync
	for key, idx := range m.m {
		assert.Equal(t, key, m.heap[idx].Key)
	}
}

func TestMapCounter_HeapIndexTracking(t *testing.T) {
	m := NewMapCounter(5)

	for i := range 1000 {
		// update 10 keys with random increments
		key := fmt.Sprintf("k%d", i%10)
		m.GetPass(key, 0, int64(rand.Intn(100)))

		// check after each operation
		for k, idx := range m.m {
			assert.Equal(t, k, m.heap[idx].Key)
		}
	}
}
