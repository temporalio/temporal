package failpoint

import (
	"sync"
	"sync/atomic"
)

// entry is a failpoint that a Registry holds. *Failpoint[T] implements it for every T. The
// registry keys an entry by the key of the failpoint, so an entry is always registered under the
// key whose type it has.
type entry interface {
	Disable()
	id() keyID
}

func validateEntry(fp entry) {
	if fp == nil {
		//nolint:forbidigo // registering a nil failpoint is a programming error
		panic("failpoint: nil failpoint")
	}
	if fp.id().name == "" {
		//nolint:forbidigo // registering a failpoint with an empty key name is a programming error
		panic("failpoint: empty key name")
	}
}

// Registry manages failpoint instances.
// It provides thread-safe operations to register and query failpoints.
type Registry struct {
	mu         sync.Mutex
	failpoints map[keyID]entry
	// count is the number of failpoints. It is updated under mu, and read without it by Get and
	// lookup, so a lookup in an empty registry doesn't take the lock.
	count atomic.Int64
}

// Source provides the registry that Execute uses. Each component that provides its own Source,
// such as each process under test, can have its own registry.
type Source interface {
	Failpoints() *Registry
}

// Failpoints returns r, so a registry is a Source for code that has no source of its own. It is
// nil for a nil registry.
func (r *Registry) Failpoints() *Registry {
	return r
}

// NewRegistry creates a new failpoint registry for testing.
func NewRegistry() *Registry {
	return &Registry{
		failpoints: make(map[keyID]entry),
	}
}

// Get retrieves a failpoint by key. Returns nil if not found.
func (r *Registry) Get(key AnyKey) entry {
	if key == nil || r.count.Load() == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failpoints[key.id()]
}

// Len returns the number of registered failpoints. It doesn't take the registry's lock, so call
// sites that do work before they execute a failpoint, such as building its key, can skip it
// cheaply while no failpoint is registered.
func (r *Registry) Len() int {
	return int(r.count.Load())
}

// setCountLocked updates count after a change of the failpoints. The caller must hold r.mu.
func (r *Registry) setCountLocked() {
	r.count.Store(int64(len(r.failpoints)))
}

// lookup returns the failpoint registered for key or, for a scoped key, for the unscoped key,
// from one snapshot of the registry.
func (r *Registry) lookup(key keyID) (entry, bool) {
	if r.count.Load() == 0 {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lookupLocked(key)
}

// lookupLocked is lookup for a caller that holds r.mu.
func (r *Registry) lookupLocked(key keyID) (entry, bool) {
	if fp := r.failpoints[key]; fp != nil {
		return fp, true
	}
	if key.scope != "" {
		if fp := r.failpoints[keyID{name: key.name}]; fp != nil {
			return fp, true
		}
	}
	return nil, false
}

// Set registers fp for its key, replacing the failpoint registered for the key, if any. It panics
// if fp is nil or its key has an empty name.
func (r *Registry) Set(fp entry) {
	validateEntry(fp)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failpoints[fp.id()] = fp
	r.setCountLocked()
}

// Delete removes a failpoint from the registry.
func (r *Registry) Delete(key AnyKey) {
	if key == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failpoints, key.id())
	r.setCountLocked()
}
