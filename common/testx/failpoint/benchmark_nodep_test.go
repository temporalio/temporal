//go:build !test_dep

package failpoint_test

import (
	"testing"

	"go.temporal.io/testx/testhooks"
)

// BenchmarkTestHooksGet measures the production stub, which only exists without test_dep. Under
// test_dep, testhooks.Get is a sync.Map lookup and would not be comparable. The stub returns
// constants and is inlined, so this row is expected to cost about as much as an empty loop.
func BenchmarkTestHooksGet(b *testing.B) {
	hooks := testhooks.NewTestHooks()
	key := testhooks.NewKey[struct{}, string]()
	scope := benchmarkScope
	for b.Loop() {
		_, _ = testhooks.Get(hooks, key, scope)
	}
}
