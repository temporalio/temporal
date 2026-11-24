package failpoint

import (
	"context"
	"fmt"
)

// ExecuteCtx executes a failpoint if registered and enabled.
// It first checks for a registry in the context, then falls back to
// checking for a directly-injected failpoint.
// Returns the failpoint's return value and a boolean indicating whether the failpoint
// provided a return value.
func ExecuteCtx[T any](ctx context.Context, key Key) (T, bool) {
	var zero T
	state := stateFromContext(ctx)
	if state == nil {
		return zero, false
	}

	// Check registry first
	if state.registry != nil {
		if fpAny := state.registry.Get(key); fpAny != nil {
			v, set, _ := execute[T](state.registry, key, fpAny, nil)
			return v, set
		}
	}

	// Fall back to directly-injected failpoint
	for injected := state.injected; injected != nil; injected = injected.next {
		if injected.key == key {
			v, set, _ := execute[T](nil, key, injected.fp, nil)
			return v, set
		}
	}

	// Nothing is injected for the key, and nothing is registered for it or the unscoped key, so
	// BUGGIFY may inject an error.
	if state.registry != nil && state.registry.Armed() && !state.registry.registeredAny(key, nil) {
		v, set, _ := executeBuggify[T](state.registry, key)
		return v, set
	}
	return zero, false
}

// Execute executes the failpoint for key in the registry of src, if registered and enabled. For a
// key with a scope, a failpoint registered for the unscoped key applies when none is registered
// for the scope.
// Returns the failpoint's return value and a boolean indicating whether the failpoint
// provided a return value.
//
// A Source without a registry, such as one in a build that disables failpoints, has no
// failpoints.
func Execute[T any](src Source, key Key) (T, bool) {
	return ExecuteWith[T](src, key, nil)
}

// ExecuteWith is Execute that passes arg to the callback of a failpoint configured with CallWith,
// for example the request that the call site is about to handle, so the callback can decide
// whether to fail it.
func ExecuteWith[T any](src Source, key Key, arg any) (T, bool) {
	v, set, _ := ExecuteWithFired[T](src, key, arg)
	return v, set
}

// ExecuteWithFired is ExecuteWith that also reports whether a failpoint fired with any action,
// including one that paused, blocked and was released, called back or returned a value that is
// not a T. A failpoint that is disabled, suspended or skipped by its Trigger did not fire. Call
// sites that fall back to another failpoint when the first did not fire use it.
//
// If no failpoint is registered for key or the unscoped key, and T is error, BUGGIFY may fire
// with an error (see BuggifyError).
func ExecuteWithFired[T any](src Source, key Key, arg any) (value T, set, fired bool) {
	registry := src.Failpoints()
	if registry == nil || !registry.Armed() {
		return value, false, false
	}
	if fpAny, ok := lookupRegistered(registry, key); ok {
		return execute[T](registry, key, fpAny, arg)
	}
	return executeBuggify[T](registry, key)
}

// ExecuteRegistered is ExecuteWithFired without BUGGIFY: it only executes a failpoint registered
// for key or the unscoped key. Call sites that execute several keys for one operation use it, and
// BuggifyError after all of them, so that a failpoint registered for any of the keys takes
// precedence over BUGGIFY.
func ExecuteRegistered[T any](src Source, key Key, arg any) (value T, set, fired bool) {
	registry := src.Failpoints()
	if registry == nil || !registry.Armed() {
		return value, false, false
	}
	if fpAny, ok := lookupRegistered(registry, key); ok {
		return execute[T](registry, key, fpAny, arg)
	}
	return value, false, false
}

// executeBuggify returns the error that BUGGIFY injects for key, which has no failpoint
// registered, if T is error.
func executeBuggify[T any](registry Registry, key Key) (value T, set, fired bool) {
	if _, isError := any((*T)(nil)).(*error); !isError {
		return value, false, false
	}
	err := buggifyError(registry, key)
	if err == nil {
		return value, false, false
	}
	value, _ = any(err).(T)
	return value, true, true
}

// Checkpoint executes an error failpoint for key in the registry of src and returns its error.
// It returns nil if the failpoint is not registered, paused or blocked and then released, or
// returned or called back with a nil error, so the caller continues as normal.
func Checkpoint(src Source, key Key) error {
	err, _ := Execute[error](src, key)
	return err
}

// Active executes the failpoint for key in the registry of src, as Execute does, and reports
// whether it fired with a value. Switch-style call sites use it with failpoints configured with
// On; a failpoint that pauses, or blocks and is released, is not active.
func Active(src Source, key Key) bool {
	_, ok := Execute[any](src, key)
	return ok
}

// ActiveCtx is Active for a failpoint looked up as ExecuteCtx does.
func ActiveCtx(ctx context.Context, key Key) bool {
	_, ok := ExecuteCtx[any](ctx, key)
	return ok
}

// executor is implemented by Failpoint[T] for every T.
type executor interface {
	executeAny(emit func(Event), arg any) (value any, set, fired bool)
}

// execute executes fpAny for a call site that expects T with arg and reports the execution to
// registry, if not nil. It returns the value, whether it was set, and whether the failpoint fired.
// A failpoint of another type still blocks, pauses, panics and calls back; a return value that is
// not a T fires, but makes the call site continue as if the failpoint had not set a value.
func execute[T any](registry Registry, key Key, fpAny any, arg any) (value T, set, fired bool) {
	var zero T
	var emit func(Event)
	if registry != nil {
		emit = registry.emit
	}
	if fp, ok := fpAny.(*Failpoint[T]); ok {
		return fp.execute(emit, arg)
	}
	ex, ok := fpAny.(executor)
	if !ok {
		panic(fmt.Errorf("failpoint: invalid type for key %v: expected *Failpoint[%T], got %T", key, zero, fpAny))
	}
	v, set, fired := ex.executeAny(emit, arg)
	if !set {
		return zero, false, fired
	}
	if v == nil {
		return zero, true, true
	}
	if t, ok := v.(T); ok {
		return t, true, true
	}
	if emit != nil {
		emit(Event{Key: key, Action: Return, Fired: true, TypeMismatch: true})
	}
	return zero, false, true
}
