//go:build test_dep

package failpoint

import (
	"maps"
	"slices"
	"sync"
)

type registryImpl struct {
	mu          sync.RWMutex
	failpoints  map[Key]any
	nextSubID   int
	subscribers map[int]func(Event)
}

// NewRegistry creates a new failpoint registry for testing.
func NewRegistry() Registry {
	return &registryImpl{
		failpoints:  make(map[Key]any),
		subscribers: make(map[int]func(Event)),
	}
}

func (r *registryImpl) Get(key Key) any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.failpoints[key]
}

func (r *registryImpl) Set(key Key, fp any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failpoints[key] = fp
}

func (r *registryImpl) Delete(key Key) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failpoints, key)
}

func (r *registryImpl) Keys() []Key {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Collect(maps.Keys(r.failpoints))
}

func (r *registryImpl) Subscribe(fn func(Event)) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextSubID
	r.nextSubID++
	r.subscribers[id] = fn
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.subscribers, id)
	}
}

func (r *registryImpl) emit(event Event) {
	r.mu.RLock()
	subscribers := slices.Collect(maps.Values(r.subscribers))
	r.mu.RUnlock()
	for _, fn := range subscribers {
		fn(event)
	}
}
