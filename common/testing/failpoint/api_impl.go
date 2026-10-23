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
			fp, ok := fpAny.(*Failpoint[T])
			if !ok {
				panic(fmt.Errorf("failpoint: invalid type for key %v: expected *Failpoint[%T], got %T", key, zero, fpAny))
			}
			return fp.Execute()
		}
	}

	// Fall back to directly-injected failpoint
	v := ctx.Value(ctxKey{key})
	if v == nil {
		return zero, false
	}
	fp, ok := v.(*Failpoint[T])
	if !ok {
		panic(fmt.Errorf("failpoint: invalid type for key %v %T", key, v))
	}
	return fp.Execute()
}

// ExecuteCtx2 executes a two-valued failpoint if registered and enabled.
// It first checks for a registry in the context, then falls back to
// checking for a directly-injected failpoint.
// Returns the failpoint's return values and a boolean indicating whether the failpoint
// provided return values.
func ExecuteCtx2[T1, T2 any](ctx context.Context, key Key) (T1, T2, bool) {
	var zero1 T1
	var zero2 T2

	// Check registry first
	if registry, ok := GetRegistry(ctx); ok {
		if fpAny := registry.Get(key); fpAny != nil {
			fp, ok := fpAny.(*Failpoint2[T1, T2])
			if !ok {
				panic(fmt.Errorf("failpoint: invalid type for key %v: expected *Failpoint2[%T, %T], got %T", key, zero1, zero2, fpAny))
			}
			return fp.Execute()
		}
	}

	// Fall back to directly-injected failpoint
	v := ctx.Value(ctxKey{key})
	if v == nil {
		return zero1, zero2, false
	}
	fp, ok := v.(*Failpoint2[T1, T2])
	if !ok {
		panic(fmt.Errorf("failpoint: invalid type for key %v %T", key, v))
	}
	return fp.Execute()
}
