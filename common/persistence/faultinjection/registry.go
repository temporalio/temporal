package faultinjection

import (
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/config"
)

type (
	// Target identifies the persistence operation a fault callback may act on. It is an
	// alias for config.FaultInjectionTarget so a FaultRegistry plugs directly into the
	// existing config.FaultInjection.Injector seam.
	Target = config.FaultInjectionTarget

	// Callback decides whether to inject a fault for the given target. Returning a nil
	// error lets the operation proceed to the real store. Callbacks must be safe for
	// concurrent use.
	Callback func(Target) error

	// faultCallback is the internal callback shape. Unlike the public Callback it can
	// return a *fault, which additionally carries the "execute the real operation, then
	// fail" semantics used by the ExecuteAndTimeout config error.
	faultCallback func(Target) *fault

	callbackEntry struct {
		id       uint64
		callback faultCallback
	}

	// FaultRegistry is a thread-safe, ordered set of fault-injection callbacks. It is the
	// generic core of persistence fault injection: the store wrapper registers the
	// YAML-configured faults as one callback and the runtime config.Injector as another,
	// so config-based injection is just a special case. Its Inject method matches
	// config.FaultInjector, so a registry can be used directly as
	// config.FaultInjection.Injector to inject faults programmatically from tests.
	FaultRegistry struct {
		mu        sync.RWMutex
		callbacks []callbackEntry
		nextID    atomic.Uint64
	}
)

// NewFaultRegistry returns an empty FaultRegistry.
func NewFaultRegistry() *FaultRegistry {
	return &FaultRegistry{}
}

// RegisterCallback registers a fault-injection callback and returns a cleanup function
// that removes it when called.
func (r *FaultRegistry) RegisterCallback(cb Callback) func() {
	return r.register(func(t Target) *fault {
		err := cb(t)
		if err == nil {
			return nil
		}
		f := newFaultFromError(err, 1.0)
		return &f
	})
}

// register adds an internal faultCallback and returns a cleanup function.
func (r *FaultRegistry) register(cb faultCallback) func() {
	if r == nil {
		return func() {}
	}
	id := r.nextID.Add(1)

	r.mu.Lock()
	r.callbacks = append(r.callbacks, callbackEntry{id: id, callback: cb})
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		for i, e := range r.callbacks {
			if e.id == id {
				r.callbacks = append(r.callbacks[:i], r.callbacks[i+1:]...)
				return
			}
		}
	}
}

// HasCallbacks returns true if there is at least one registered callback.
func (r *FaultRegistry) HasCallbacks() bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.callbacks) > 0
}

// Generate runs the registered callbacks in registration order and returns the first
// fault produced, or nil if none of them inject a fault.
func (r *FaultRegistry) Generate(t Target) *fault {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	if len(r.callbacks) == 0 {
		r.mu.RUnlock()
		return nil
	}
	callbacks := make([]callbackEntry, len(r.callbacks))
	copy(callbacks, r.callbacks)
	r.mu.RUnlock()

	for _, e := range callbacks {
		if f := e.callback(t); f != nil {
			return f
		}
	}
	return nil
}

// Inject adapts the registry to config.FaultInjector so it can be assigned directly to
// config.FaultInjection.Injector.
func (r *FaultRegistry) Inject(t Target) error {
	f := r.Generate(t)
	if f == nil {
		return nil
	}
	return f.err
}
