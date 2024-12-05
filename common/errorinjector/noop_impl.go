//go:build !errorinjector

package errorinjector

import "go.uber.org/fx"

var Module = fx.Options(
	fx.Provide(func() ErrorInjector { return nil }),
)

type (
	ErrorInjector interface {
	}
)

func Get[T any](ei ErrorInjector, key string) (T, bool) {
	var zero T
	return zero, false
}

func Set[T any](ei ErrorInjector, key string, val T) {
}
