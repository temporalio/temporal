//go:build test_dep

package failpoint

import (
	"crypto/rand"
	"maps"
	"reflect"
	"slices"
	"sync"
)

type registryImpl struct {
	id string
	// configMu serializes Configure calls. It is separate from mu, so configuring a failpoint
	// that is busy, such as one that pauses, doesn't block the lookups of executions.
	configMu    sync.Mutex
	mu          sync.RWMutex
	failpoints  map[Key]any
	nextSubID   int
	subscribers map[int]func(Event)
}

// NewRegistry creates a new failpoint registry for testing.
func NewRegistry() Registry {
	return &registryImpl{
		id:          rand.Text(),
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

func (r *registryImpl) GetOrSet(key Key, fp any) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.failpoints[key]; ok {
		return existing
	}
	r.failpoints[key] = fp
	return fp
}

func (r *registryImpl) CompareAndDelete(key Key, fp any) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.failpoints[key]; !ok || !sameEntry(existing, fp) {
		return false
	}
	delete(r.failpoints, key)
	return true
}

// sameEntry reports whether a and b are equal without panicking on values that cannot be
// compared, such as slices or structs that contain them.
func sameEntry(a, b any) bool {
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if !va.IsValid() || !vb.IsValid() {
		return !va.IsValid() && !vb.IsValid()
	}
	if va.Type() != vb.Type() || !va.Comparable() || !vb.Comparable() {
		return false
	}
	return a == b
}

func (r *registryImpl) DeleteAll() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	removed := slices.Collect(maps.Values(r.failpoints))
	clear(r.failpoints)
	return removed
}

func (r *registryImpl) Configure(fn func()) {
	r.configMu.Lock()
	defer r.configMu.Unlock()
	fn()
}

func (r *registryImpl) ID() string {
	return r.id
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
