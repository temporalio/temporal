//go:build !testhooks

package testhooks

import "go.uber.org/fx"

var Module = fx.Options(
	fx.Provide(func() (_ TestHooks) { return }),
)

type (
	TestHooks struct{}
)

func Get[T any](_ TestHooks, key string) (T, bool) {
	var zero T
	return zero, false
}
