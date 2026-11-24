//go:build test_dep

package failpoint

import (
	"context"
	"fmt"
)

type ctxKey struct {
	key Key
}

// ExecuteCtx executes a failpoint if registered and enabled.
// It first checks for a registry in the context, then falls back to
// checking for a directly-injected failpoint.
// Returns the failpoint's return value and a boolean indicating whether the failpoint
// provided a return value.
func ExecuteCtx[T any](ctx context.Context, key Key) (T, bool) {
	var zero T

	// Check registry first
	if registry, ok := GetRegistry(ctx); ok {
		if fpAny := registry.Get(key); fpAny != nil {
			return execute[T](key, fpAny)
		}
	}

	// Fall back to directly-injected failpoint
	v := ctx.Value(ctxKey{key})
	if v == nil {
		return zero, false
	}
	return execute[T](key, v)
}

// Execute executes the failpoint for key in the registry of src, if registered and enabled. For a
// key scoped to a namespace, a failpoint registered for the unscoped key applies when none is
// registered for the namespace.
// Returns the failpoint's return value and a boolean indicating whether the failpoint
// provided a return value.
func Execute[T any](src Source, key Key) (T, bool) {
	registry := src.Failpoints()
	if fpAny := registry.Get(key); fpAny != nil {
		return execute[T](key, fpAny)
	}
	if key.namespaceID != "" {
		if fpAny := registry.Get(Key{name: key.name}); fpAny != nil {
			return execute[T](key, fpAny)
		}
	}
	var zero T
	return zero, false
}

// Checkpoint executes an error failpoint for key in the registry of src and returns its error.
// It returns nil if the failpoint is not registered, paused or blocked and then released, or
// returned or called back with a nil error, so the caller continues as normal.
func Checkpoint(src Source, key Key) error {
	err, _ := Execute[error](src, key)
	return err
}

func execute[T any](key Key, fpAny any) (T, bool) {
	fp, ok := fpAny.(*Failpoint[T])
	if !ok {
		var zero T
		panic(fmt.Errorf("failpoint: invalid type for key %v: expected *Failpoint[%T], got %T", key, zero, fpAny))
	}
	return fp.Execute()
}
