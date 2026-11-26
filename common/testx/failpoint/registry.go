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

	// policy is the active Policy, guarded by mu. mayFire caches MayFire and is updated under mu.
	policy  Policy
	mayFire atomic.Bool
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

// MayFire reports whether any failpoint is registered or a Policy is set (see SetPolicy). It
// doesn't take the registry's lock, so call sites that do work before they execute a failpoint can
// skip it cheaply while it is false.
func (r *Registry) MayFire() bool {
	return r.mayFire.Load()
}

// SetPolicy installs p as the Policy of the registry, replacing any previous policy. Pass nil to
// clear it. A nil policy is not consulted and doesn't make the registry MayFire.
func (r *Registry) SetPolicy(p Policy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.policy = p
	r.updateMayFireLocked()
}

// Policy returns the installed Policy, or nil.
func (r *Registry) Policy() Policy {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.policy
}

// setCountLocked updates count and mayFire after a change of the failpoints. The caller must hold
// r.mu.
func (r *Registry) setCountLocked() {
	r.count.Store(int64(len(r.failpoints)))
	r.updateMayFireLocked()
}

// updateMayFireLocked publishes mayFire after a change of the failpoints or of the policy. The
// caller must hold r.mu.
func (r *Registry) updateMayFireLocked() {
	r.mayFire.Store(len(r.failpoints) > 0 || r.policy != nil)
}

// lookupOrPolicy returns the failpoint registered for key (or, for a scoped key, for the unscoped
// key), or the installed Policy when nothing is registered, from one snapshot of the registry.
func (r *Registry) lookupOrPolicy(key keyID) (entry, Policy, bool) {
	if !r.mayFire.Load() {
		return nil, nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if fp, ok := r.lookupLocked(key); ok {
		return fp, nil, true
	}
	return nil, r.policy, false
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
// if fp is nil or its key has an empty name. Callers replacing a failpoint that may hold blocked
// executions should use Swap and call Disable on the displaced entry so those executions are not
// stranded.
func (r *Registry) Set(fp entry) {
	r.Swap(fp)
}

// Swap registers fp for its key and returns the displaced failpoint, or nil if none was
// registered. It panics if fp is nil or its key has an empty name. If the displaced failpoint may
// hold blocked or paused executions, the caller must call Disable on it to release them.
func (r *Registry) Swap(fp entry) entry {
	validateEntry(fp)
	r.mu.Lock()
	defer r.mu.Unlock()
	prev := r.failpoints[fp.id()]
	r.failpoints[fp.id()] = fp
	r.setCountLocked()
	return prev
}

// Delete removes a failpoint from the registry. It only detaches the failpoint: it doesn't disable
// it, so call Disable on the failpoint to release the executions it holds.
func (r *Registry) Delete(key AnyKey) {
	if key == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failpoints, key.id())
	r.setCountLocked()
}
