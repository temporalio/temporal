//go:build !test_dep

package testhooks

// Hook is an empty stub in production mode. NewHook and its methods are only available with -tags=test_dep.
type Hook struct{}

func NewHook[T any, S any](_ Key[T, S], _ T) Hook {
	panic("testhooks.NewHook called but TestHooks are not enabled: use -tags=test_dep when running `go test`")
}

func (h Hook) Scope() ScopeType {
	panic("testhooks.Hook used but TestHooks are not enabled: use -tags=test_dep when running `go test`")
}

func (h Hook) Apply(_ TestHooks, _ any) func() {
	panic("testhooks.Hook used but TestHooks are not enabled: use -tags=test_dep when running `go test`")
}

func newKey[T any, S any]() Key[T, S] {
	return Key[T, S]{}
}
