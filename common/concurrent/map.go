package concurrent

import "sync"

// Map implements a simple mutex-wrapped map. We've had bugs where we took the wrong lock
// when reimplementing this pattern, so it's worth having a single canonical implementation.
type Map[K comparable, V any] struct {
	*sync.RWMutex
	contents map[K]V
}

func NewMap[K comparable, V any]() Map[K, V] {
	return Map[K, V]{
		RWMutex:  &sync.RWMutex{},
		contents: make(map[K]V),
	}
}

func (m *Map[K, V]) Get(key K) (value V, ok bool) {
	m.RLock()
	defer m.RUnlock()
	value, ok = m.contents[key]
	return
}

func (m *Map[K, V]) Set(key K, value V) {
	m.Lock()
	defer m.Unlock()
	m.contents[key] = value
}

func (m *Map[K, V]) Delete(key K) {
	m.Lock()
	defer m.Unlock()
	delete(m.contents, key)
}

func (m *Map[K, V]) Pop(key K) (value V, ok bool) {
	m.Lock()
	defer m.Unlock()
	value, ok = m.contents[key]
	if ok {
		delete(m.contents, key)
	}
	return value, ok
}
