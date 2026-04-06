package counter

import (
	"container/heap"
	"slices"
)

// mapCounter is a Counter that stores counts in a map.
// It also maintains a min-heap to efficiently track the top-K entries.
// mapCounter is not safe for concurrent use.
type mapCounter struct {
	m     map[string]int
	heap  []TopKEntry
	limit int
}

var _ Counter = (*mapCounter)(nil)

// NewMapCounter creates a mapCounter that also tracks the top K entries.
func NewMapCounter(limit int) *mapCounter {
	return &mapCounter{
		m:     make(map[string]int),
		limit: limit,
	}
}

func (m *mapCounter) GetPass(key string, base, inc int64) int64 {
	c, _ := m.getPassWithOverflow(key, base, inc)
	return c
}

func (m *mapCounter) getPassWithOverflow(key string, base, inc int64) (int64, bool) {
	if idx, ok := m.m[key]; ok {
		prev := m.heap[idx].Count
		count := max(base, prev+inc)
		// inline simple case of updateHeap
		m.heap[idx].Count = count
		heap.Fix(m, idx)
		return count, false
	}
	// not present, fall back to full updateHeap
	count := max(base, inc)
	return count, m.updateHeap(key, count)
}

func (m *mapCounter) EstimateDistinctKeys() int {
	return len(m.heap)
}

// TopK returns the top-K entries by count.
func (m *mapCounter) TopK() []TopKEntry {
	return slices.Clone(m.heap)
}

func (m *mapCounter) updateHeap(key string, count int64) bool {
	if idx, ok := m.m[key]; ok {
		// already in heap - update count and fix
		m.heap[idx].Count = count
		heap.Fix(m, idx)
		return false
	}

	if len(m.heap) < m.limit {
		// heap not full - add
		m.m[key] = len(m.heap)
		heap.Push(m, TopKEntry{Key: key, Count: count})
		return false
	}

	// heap is full - only add if count > min
	if count > m.heap[0].Count {
		// evict min
		evicted := heap.Pop(m).(TopKEntry) // nolint:revive // unchecked-type-assertion
		delete(m.m, evicted.Key)
		// add new
		m.m[key] = len(m.heap)
		heap.Push(m, TopKEntry{Key: key, Count: count})
	}
	return true
}

// implements heap.Interface using m.heap
func (m *mapCounter) Len() int           { return len(m.heap) }
func (m *mapCounter) Less(i, j int) bool { return m.heap[i].Count < m.heap[j].Count }
func (m *mapCounter) Swap(i, j int) {
	m.heap[i], m.heap[j] = m.heap[j], m.heap[i]
	// don't forget to fix the map:
	m.m[m.heap[i].Key] = i
	m.m[m.heap[j].Key] = j
}

func (m *mapCounter) Push(x any) {
	m.heap = append(m.heap, x.(TopKEntry))
}

func (m *mapCounter) Pop() any {
	n := len(m.heap)
	entry := m.heap[n-1]
	m.heap[n-1] = TopKEntry{}
	m.heap = m.heap[0 : n-1]
	return entry
}
