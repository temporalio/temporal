//go:build test_dep

package testhooks

import "testing"

// Ensure that testhook functionality that operates on contexts functions as
// expected.
func TestTestHooks_Context(t *testing.T) {
	t.Run("Values can be set and get on the context directly", func(t *testing.T) {
		ctx, _ := NewInjectedTestHooksImpl(t.Context())
		cleanup := SetCtx(ctx, UpdateWithStartInBetweenLockAndStart, 33)
		defer cleanup()

		v, ok := GetCtx[int](ctx, UpdateWithStartInBetweenLockAndStart)
		if !ok {
			t.Fatal("Expected TestHooksImpl to contain value")
		}
		if v != 33 {
			t.Fatal("Expected value to be 33")
		}
	})

	t.Run("Values set directly on the registry are visible through the context", func(t *testing.T) {
		ctx, reg := NewInjectedTestHooksImpl(t.Context())
		cleanup := Set(reg, UpdateWithStartInBetweenLockAndStart, 44)
		defer cleanup()

		v, ok := GetCtx[int](ctx, UpdateWithStartInBetweenLockAndStart)
		if !ok {
			t.Fatal("Expected TestHooksImpl to contain value")
		}
		if v != 44 {
			t.Fatal("Expected value to be 44")
		}
	})

	t.Run("CallCtx uses the registry injected into the context", func(t *testing.T) {
		ctx, reg := NewInjectedTestHooksImpl(t.Context())
		var value int
		callback := func() {
			value = 55
		}
		cleanup := Set(reg, UpdateWithStartInBetweenLockAndStart, callback)
		defer cleanup()

		CallCtx(ctx, UpdateWithStartInBetweenLockAndStart)
		if value != 55 {
			t.Fatal("Expected value to be 55")
		}
	})
}
