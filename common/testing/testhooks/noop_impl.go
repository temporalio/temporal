//go:build !test_dep

package testhooks

import (
	"context"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(func() (_ TestHooks) { return }),
)

type (
	// TestHooks (in production mode) is an empty struct just so the build works.
	// See TestHooks in test_impl.go.
	//
	// TestHooks are an inherently unclean way of writing tests. They require mixing test-only
	// concerns into production code. In general you should prefer other ways of writing tests
	// wherever possible, and only use TestHooks sparingly, as a last resort.
	TestHooks struct{}
)

// Get gets the value of a test hook. In production mode it always returns the zero value and
// false, which hopefully the compiler will inline and remove the hook as dead code.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func Get[T any](_ TestHooks, key Key) (T, bool) {
	var zero T
	return zero, false
}

// GetCtx gets the value of a test hook from the registry embedded in the
// context chain.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func GetCtx[T any](ctx context.Context, key Key) (T, bool) {
	var zero T
	return zero, false
}

// Call calls a func() hook if present.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func Call(_ TestHooks, key Key) {
}

// CallCtx calls a func(context.Context) hook if present.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func CallCtx(_ context.Context, key Key) {
}
