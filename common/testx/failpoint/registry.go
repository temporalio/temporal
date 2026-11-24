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

	// Configure runs fn while no other Configure call of the registry runs. Code that configures
	// or removes failpoints with several calls, such as a lookup followed by a change, runs them
	// in Configure, so they are atomic with respect to each other. It does not block executions.
	Configure(fn func())

	// ID returns a random identifier of the registry, so that processes that serve the same
	// registry under several addresses can be told apart from ones that don't.
	ID() string

	// Keys returns the keys of all registered failpoints.
	Keys() []Key

	// Subscribe calls fn for every execution of an enabled failpoint in this registry, until the
	// returned function is called. fn must not call into the failpoint that is executing.
	Subscribe(fn func(Event)) (unsubscribe func())

	// emit reports an execution to the subscribers.
	emit(Event)
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

func (r *registryImpl) Set(key Key, fp any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failpoints[key] = fp
	r.count.Store(int64(len(r.failpoints)))
}

func (r *registryImpl) GetOrSet(key Key, fp any) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.failpoints[key]; ok {
		return existing
	}
	r.failpoints[key] = fp
	r.count.Store(int64(len(r.failpoints)))
	return fp
}

func (r *registryImpl) CompareAndDelete(key Key, fp any) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.failpoints[key]; !ok || !sameEntry(existing, fp) {
		return false
	}
	delete(r.failpoints, key)
	r.count.Store(int64(len(r.failpoints)))
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
	r.count.Store(0)
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
	r.count.Store(int64(len(r.failpoints)))
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
