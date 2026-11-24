package failpoint

import (
	"context"
	"crypto/rand"
	"maps"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
)

// Registry manages failpoint instances.
// It provides thread-safe operations to register and query failpoints.
type Registry interface {
	// Get retrieves a failpoint by key. Returns nil if not found.
	Get(key Key) any

	// Len returns the number of registered failpoints. It doesn't take the registry's lock, so call
	// sites that do work before they execute a failpoint, such as building its key, can skip it
	// cheaply while no failpoint is registered.
	Len() int

	// Armed reports whether any failpoint is registered or an active fallback is installed (see
	// Fallback). Like Len, it doesn't take the registry's lock, so call sites that do work before
	// they execute a failpoint can skip it cheaply while it is false.
	Armed() bool

	// SetFallback installs f as the fallback of the registry, replacing the previous one, or
	// removes the fallback if f is nil. A fallback starts inactive, see SetFallbackActive.
	SetFallback(f Fallback)

	// InstallFallback installs f as the fallback of the registry if it has none, and returns the
	// installed fallback, f or the one that was already installed. It doesn't take the lock of
	// Configure, so it can be called inside Configure.
	InstallFallback(f Fallback) Fallback

	// Fallback returns the installed fallback, active or not, or nil.
	Fallback() Fallback

	// ActiveFallback returns the installed fallback while it is active, else nil. Code that
	// decides like the fallback, such as a branch at a call site, checks it, so it agrees with the
	// executions that consult the fallback.
	ActiveFallback() Fallback

	// SetFallbackActive turns the fallback f on or off, and reports whether it did. It does
	// nothing if f is not the installed fallback, so a stale handle can't turn on a fallback that
	// replaced it. An inactive fallback is not consulted and doesn't arm the registry.
	SetFallbackActive(f Fallback, active bool) bool

	// Set stores a failpoint instance for the given key.
	Set(key Key, fp any)

	// GetOrSet returns the failpoint for key, storing fp for key first if there is none. Callers
	// that share a registry use it to create a failpoint without racing each other.
	GetOrSet(key Key, fp any) any

	// Delete removes a failpoint from the registry.
	Delete(key Key)

	// CompareAndDelete removes the failpoint for key if it is fp, and reports whether it did.
	// Values that cannot be compared, such as slices, are never equal.
	CompareAndDelete(key Key, fp any) bool

	// DeleteAll removes all failpoints at once and returns them.
	DeleteAll() []any

	// DeleteFunc removes the failpoints for which keep returns false at once and returns them. It
	// calls keep without holding the registry's lock, so keep can call the registry. An entry that
	// changed after keep saw it is kept.
	DeleteFunc(keep func(key Key, fp any) bool) []any

	// Configure runs fn while no other Configure call of the registry runs. Code that configures
	// or removes failpoints with several calls, such as a lookup followed by a change, runs them
	// in Configure, so they are atomic with respect to each other. It does not block executions.
	Configure(fn func())

	// ID returns a random identifier of the registry, so that processes that serve the same
	// registry under several addresses can be told apart from ones that don't.
	ID() string

	// Keys returns the keys of all registered failpoints.
	Keys() []Key

	// Subscribe calls fn for every execution of an enabled failpoint in this registry, and for
	// every event of its fallback, until the returned function is called. fn must not call into the
	// failpoint that is executing, and must not execute failpoints while it handles a
	// FallbackActivationEvent.
	Subscribe(fn func(Event)) (unsubscribe func())

	// Emit reports an event to the subscribers. Fallbacks use it to report their decisions.
	Emit(Event)

	// lookup returns the failpoint registered for key or, for a scoped key, for the unscoped key,
	// from one snapshot of the registry.
	lookup(key Key) (any, bool)

	// registeredAny reports whether a failpoint is registered for key or any of others, or for
	// their unscoped keys, from one snapshot of the registry.
	registeredAny(key Key, others []Key) bool

	// update runs fn with the failpoints of the registry, while no other change to the registry
	// runs, so fn can check and change several entries atomically. fn must not call the registry.
	update(fn func(failpoints map[Key]any))
}

// Source provides the registry that Execute uses. Each component that provides its own Source,
// such as each process under test, can have its own registry.
type Source interface {
	Failpoints() Registry
}

// ctxState is the failpoint state of a context: its registry and the failpoints injected into it
// directly. Both are stored under one context key, so that ExecuteCtx needs a single context
// lookup.
type ctxState struct {
	registry Registry
	injected *injectedFailpoint
}

// injectedFailpoint is a failpoint injected into a context. It links to the failpoints injected
// into the parent contexts.
type injectedFailpoint struct {
	key  Key
	fp   any
	next *injectedFailpoint
}

type ctxStateKey struct{}

func stateFromContext(ctx context.Context) *ctxState {
	state, _ := ctx.Value(ctxStateKey{}).(*ctxState)
	return state
}

// stateCopy returns a copy of the state of ctx, to change for a child context.
func stateCopy(ctx context.Context) ctxState {
	if state := stateFromContext(ctx); state != nil {
		return *state
	}
	return ctxState{}
}

// injectFailpoint adds a failpoint for key to the context.
func injectFailpoint(ctx context.Context, key Key, fp any) context.Context {
	state := stateCopy(ctx)
	state.injected = &injectedFailpoint{key: key, fp: fp, next: state.injected}
	return context.WithValue(ctx, ctxStateKey{}, &state)
}

// InjectRegistry adds a failpoint registry to the context.
func InjectRegistry(ctx context.Context, registry Registry) context.Context {
	state := stateCopy(ctx)
	state.registry = registry
	return context.WithValue(ctx, ctxStateKey{}, &state)
}

// GetRegistry retrieves a failpoint registry from the context.
func GetRegistry(ctx context.Context) (Registry, bool) {
	if state := stateFromContext(ctx); state != nil && state.registry != nil {
		return state.registry, true
	}
	return nil, false
}

type registryImpl struct {
	id string
	// configMu serializes Configure calls. It is separate from mu, so configuring a failpoint
	// that is busy, such as one that pauses, doesn't block the lookups of executions.
	configMu   sync.Mutex
	mu         sync.RWMutex
	failpoints map[Key]any
	// count is the number of failpoints. It is updated under mu, and read without it by Get, so a
	// lookup in an empty registry doesn't take the lock.
	count       atomic.Int64
	nextSubID   int
	subscribers map[int]func(Event)

	// fallback is the installed fallback and fallbackActive whether it is on. Both are guarded by
	// mu.
	fallback       Fallback
	fallbackActive bool
	// effective holds fallback while it is active, else nil, so executions read the fallback and
	// whether it is active in one snapshot. armed caches Armed. Both are updated under mu.
	effective atomic.Pointer[installedFallback]
	armed     atomic.Bool
}

// installedFallback boxes a Fallback for an atomic pointer.
type installedFallback struct {
	f Fallback
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
	if r.count.Load() == 0 {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.failpoints[key]
}

func (r *registryImpl) Len() int {
	return int(r.count.Load())
}

func (r *registryImpl) Armed() bool {
	return r.armed.Load()
}

// setCountLocked updates count and armed after a change of the failpoints. The caller must hold
// r.mu.
func (r *registryImpl) setCountLocked() {
	r.count.Store(int64(len(r.failpoints)))
	r.updateArmedLocked()
}

// updateArmedLocked publishes effective and armed after a change of the failpoints or of the
// fallback. The caller must hold r.mu.
func (r *registryImpl) updateArmedLocked() {
	var effective *installedFallback
	if r.fallback != nil && r.fallbackActive {
		effective = &installedFallback{f: r.fallback}
	}
	r.effective.Store(effective)
	r.armed.Store(len(r.failpoints) > 0 || effective != nil)
}

func (r *registryImpl) SetFallback(f Fallback) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fallback = f
	r.fallbackActive = false
	r.updateArmedLocked()
}

func (r *registryImpl) Fallback() Fallback {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.fallback
}

func (r *registryImpl) InstallFallback(f Fallback) Fallback {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fallback == nil {
		r.fallback = f
		r.fallbackActive = false
		r.updateArmedLocked()
	}
	return r.fallback
}

func (r *registryImpl) SetFallbackActive(f Fallback, active bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fallback == nil || !sameEntry(r.fallback, f) {
		return false
	}
	r.fallbackActive = active
	r.updateArmedLocked()
	return true
}

func (r *registryImpl) ActiveFallback() Fallback {
	if effective := r.effective.Load(); effective != nil {
		return effective.f
	}
	return nil
}

func (r *registryImpl) registeredAny(key Key, others []Key) bool {
	if r.count.Load() == 0 {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.lookupLocked(key); ok {
		return true
	}
	for _, other := range others {
		if _, ok := r.lookupLocked(other); ok {
			return true
		}
	}
	return false
}

func (r *registryImpl) lookup(key Key) (any, bool) {
	if r.count.Load() == 0 {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lookupLocked(key)
}

// lookupLocked is lookup for a caller that holds r.mu.
func (r *registryImpl) lookupLocked(key Key) (any, bool) {
	if fp := r.failpoints[key]; fp != nil {
		return fp, true
	}
	if key.scope != "" {
		if fp := r.failpoints[Key{name: key.name}]; fp != nil {
			return fp, true
		}
	}
	return nil, false
}

func (r *registryImpl) Set(key Key, fp any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failpoints[key] = fp
	r.setCountLocked()
}

func (r *registryImpl) GetOrSet(key Key, fp any) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.failpoints[key]; ok {
		return existing
	}
	r.failpoints[key] = fp
	r.setCountLocked()
	return fp
}

func (r *registryImpl) CompareAndDelete(key Key, fp any) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.failpoints[key]; !ok || !sameEntry(existing, fp) {
		return false
	}
	delete(r.failpoints, key)
	r.setCountLocked()
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
	r.setCountLocked()
	return removed
}

func (r *registryImpl) update(fn func(failpoints map[Key]any)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn(r.failpoints)
	r.setCountLocked()
}

func (r *registryImpl) DeleteFunc(keep func(key Key, fp any) bool) []any {
	r.mu.RLock()
	snapshot := maps.Clone(r.failpoints)
	r.mu.RUnlock()

	candidates := make(map[Key]any)
	for key, fp := range snapshot {
		if !keep(key, fp) {
			candidates[key] = fp
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	var removed []any
	for key, fp := range candidates {
		if current, ok := r.failpoints[key]; ok && identical(current, fp) {
			removed = append(removed, current)
			delete(r.failpoints, key)
		}
	}
	r.setCountLocked()
	return removed
}

// identical reports whether a and b are the same entry. Unlike sameEntry, it compares slices and
// maps by identity, so DeleteFunc can remove them.
func identical(a, b any) bool {
	if sameEntry(a, b) {
		return true
	}
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if !va.IsValid() || !vb.IsValid() || va.Type() != vb.Type() {
		return false
	}
	switch va.Kind() {
	case reflect.Slice:
		return va.Pointer() == vb.Pointer() && va.Len() == vb.Len()
	case reflect.Map:
		return va.Pointer() == vb.Pointer()
	default:
		return false
	}
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
	r.setCountLocked()
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

func (r *registryImpl) Emit(event Event) {
	r.mu.RLock()
	subscribers := slices.Collect(maps.Values(r.subscribers))
	r.mu.RUnlock()
	for _, fn := range subscribers {
		fn(event)
	}
}
