//go:build testhooks

package testhooks

import (
	"sync"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTestHooksImpl),
)

type (
	TestHooks interface {
		// private accessors; access must go through package-level Get/Set
		get(string) (any, bool)
		set(string, any)
		del(string)
	}

	testHooksImpl struct {
		m sync.Map
	}
)

func Get[T any](th TestHooks, key string) (T, bool) {
	if val, ok := th.get(key); ok {
		// this is only used in test so we want to panic on type mismatch:
		return val.(T), ok // nolint:revive
	}
	var zero T
	return zero, false
}

func Set[T any](th TestHooks, key string, val T) func() {
	th.set(key, val)
	return func() { th.del(key) }
}

func NewTestHooksImpl() TestHooks {
	return &testHooksImpl{}
}

func (th *testHooksImpl) get(key string) (any, bool) {
	val, ok := th.m.Load(key)
	return val, ok
}

func (th *testHooksImpl) set(key string, val any) {
	th.m.Store(key, val)
}

func (th *testHooksImpl) del(key string) {
	th.m.Delete(key)
}
