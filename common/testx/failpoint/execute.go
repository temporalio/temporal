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
			return execute[T](state.registry, key, fpAny)
		}
	}

	// Fall back to directly-injected failpoint
	for injected := state.injected; injected != nil; injected = injected.next {
		if injected.key == key {
			return execute[T](nil, key, injected.fp)
		}
	}
	return zero, false
}

// Execute executes the failpoint for key in the registry of src, if registered and enabled. For a
// key scoped to a namespace, a failpoint registered for the unscoped key applies when none is
// registered for the namespace.
// Returns the failpoint's return value and a boolean indicating whether the failpoint
// provided a return value.
//
// A Source without a registry, such as a zero-value test hooks struct in a unit test, has no
// failpoints.
func Execute[T any](src Source, key Key) (T, bool) {
	var zero T
	registry := src.Failpoints()
	if registry == nil {
		return zero, false
	}
	if fpAny := registry.Get(key); fpAny != nil {
		return execute[T](registry, key, fpAny)
	}
	if key.namespaceID != "" {
		if fpAny := registry.Get(Key{name: key.name}); fpAny != nil {
			return execute[T](registry, key, fpAny)
		}
	}
	return zero, false
}

// Checkpoint executes an error failpoint for key in the registry of src and returns its error.
// It returns nil if the failpoint is not registered, paused or blocked and then released, or
// returned or called back with a nil error, so the caller continues as normal.
func Checkpoint(src Source, key Key) error {
	err, _ := Execute[error](src, key)
	return err
}

// executor is implemented by Failpoint[T] for every T.
type executor interface {
	executeAny(emit func(Event)) (any, bool)
}

// execute executes fpAny for a call site that expects T and reports the execution to registry,
// if not nil. A failpoint of another type still blocks, pauses, panics and calls back; a return
// value that is not a T makes the call site continue as if the failpoint had not fired.
func execute[T any](registry Registry, key Key, fpAny any) (T, bool) {
	var zero T
	var emit func(Event)
	if registry != nil {
		emit = registry.emit
	}
	if fp, ok := fpAny.(*Failpoint[T]); ok {
		return fp.execute(emit)
	}
	ex, ok := fpAny.(executor)
	if !ok {
		panic(fmt.Errorf("failpoint: invalid type for key %v: expected *Failpoint[%T], got %T", key, zero, fpAny))
	}
	v, set := ex.executeAny(emit)
	if !set {
		return zero, false
	}
	if v == nil {
		return zero, true
	}
	if t, ok := v.(T); ok {
		return t, true
	}
	if emit != nil {
		emit(Event{Key: key, Action: Return, Fired: true, TypeMismatch: true})
	}
	return zero, false
}
