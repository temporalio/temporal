//go:build test_dep

package failpoint

import (
	"maps"
	"slices"
	"sync"
)

type registryImpl struct {
	mu         sync.RWMutex
	failpoints map[Key]any
}

// NewRegistry creates a new failpoint registry for testing.
func NewRegistry() Registry {
	return &registryImpl{
		failpoints: make(map[Key]any),
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
