//go:build test_dep

package testhooks

import (
	"context"
	"sync"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTestHooksImpl),
)

type (
	// TestHooks holds a registry of active test hooks. It should be obtained through fx and
	// used with Get and Set.
	//
	// TestHooks are an inherently unclean way of writing tests. They require mixing test-only
	// concerns into production code. In general you should prefer other ways of writing tests
	// wherever possible, and only use TestHooks sparingly, as a last resort.
	TestHooks interface {
		// private accessors; access must go through package-level Get/Set
		get(Key) (any, bool)
		set(Key, any)
		del(Key)
	}

	// testHooksImpl is an implementation of TestHooks.
	testHooksImpl struct {
		m sync.Map
	}

	contextKey struct{}
)

// Get gets the value of a test hook from the registry.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func Get[T any](th TestHooks, key Key) (T, bool) {
	var zero T
	if th == nil {
		return zero, false
	}
	if val, ok := th.get(key); ok {
		// this is only used in test so we want to panic on type mismatch:
		return val.(T), ok // nolint:revive
	}
	return zero, false
}

// GetCtx gets the value of a test hook from the registry embedded in the
// context chain.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func GetCtx[T any](ctx context.Context, key Key) (T, bool) {
	hooks := ctx.Value(contextKey{})
	if hooks, ok := hooks.(TestHooks); ok {
		return Get[T](hooks, key)
	}
	var zero T
	return zero, false
}

// Call calls a func() hook if present.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func Call(th TestHooks, key Key) {
	if hook, ok := Get[func()](th, key); ok {
		hook()
	}
}

// CallCtx calls a func() hook if present and a TestHooks implementation
// exists in the context chain.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func CallCtx(ctx context.Context, key Key) {
	hooks := ctx.Value(contextKey{})
	if hooks, ok := hooks.(TestHooks); ok {
		Call(hooks, key)
	}
}

// Set sets a test hook to a value and returns a cleanup function to unset it.
// Calls to Set and the cleanup function should form a stack.
func Set[T any](th TestHooks, key Key, val T) func() {
	th.set(key, val)
	return func() { th.del(key) }
}

// SetCtx sets a test hook to a value on the provided context and returns a
// cleanup function to unset it. Calls to SetCtx and the cleanup function
// should form a stack.
func SetCtx[T any](ctx context.Context, key Key, val T) func() {
	hooks := ctx.Value(contextKey{})
	if hooks, ok := hooks.(TestHooks); ok {
		return Set(hooks, key, val)
	}
	return func() {}
}

// NewTestHooksImpl returns a new instance of a test hook registry. This is provided and used
// in the main "resource" module as a default, but in functional tests, it's overridden by an
// explicitly constructed instance.
func NewTestHooksImpl() TestHooks {
	return &testHooksImpl{}
}

// NewInjectedTestHooksImpl returns a new instance of a test hook registry and a context with the
// registry injected.
func NewInjectedTestHooksImpl(parent context.Context) (context.Context, TestHooks) {
	hooks := NewTestHooksImpl()
	return context.WithValue(parent, contextKey{}, hooks), hooks
}

func (th *testHooksImpl) get(key Key) (any, bool) {
	return th.m.Load(key)
}

func (th *testHooksImpl) set(key Key, val any) {
	th.m.Store(key, val)
}

func (th *testHooksImpl) del(key Key) {
	th.m.Delete(key)
}
