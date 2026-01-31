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
	heap  topKHeap
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
		heap.Fix(&m.heap, idx)
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
		heap.Fix(&m.heap, idx)
		return false
	}

	if len(m.heap) < m.limit {
		// heap not full - add
		m.m[key] = len(m.heap)
		heap.Push(&m.heap, TopKEntry{Key: key, Count: count})
		return false
	}

	// heap is full - only add if count > min
	if count > m.heap[0].Count {
		// evict min
		evicted := heap.Pop(&m.heap).(TopKEntry)
		delete(m.m, evicted.Key)
		// add new
		m.m[key] = len(m.heap)
		heap.Push(&m.heap, TopKEntry{Key: key, Count: count})
	}
	return true
}

// topKHeap implements heap.Interface as a min-heap (smallest count at root)
type topKHeap []TopKEntry

func (h topKHeap) Len() int           { return len(h) }
func (h topKHeap) Less(i, j int) bool { return h[i].Count < h[j].Count }
func (h topKHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *topKHeap) Push(x any) {
	*h = append(*h, x.(TopKEntry))
}

func (h *topKHeap) Pop() any {
	n := len(*h)
	entry := (*h)[n-1]
	(*h)[n-1] = TopKEntry{}
	*h = (*h)[0 : n-1]
	return entry
}
