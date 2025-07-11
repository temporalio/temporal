package counter

// mapCounter is a Counter that stores counts in a map.
// mapCounter is not safe for concurrent use.
type mapCounter struct {
	m map[string]int64
}

var _ Counter = (*mapCounter)(nil)

func NewMapCounter() *mapCounter {
	return &mapCounter{m: make(map[string]int64)}
}

func (m *mapCounter) GetPass(key string, base, inc int64) int64 {
	c := max(base, m.m[key]+inc)
	m.m[key] = c
	return c
}

func (m *mapCounter) EstimateDistinctKeys() int {
	return len(m.m)
}
