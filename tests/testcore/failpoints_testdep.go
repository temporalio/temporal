//go:build test_dep

package testcore

import (
	"go.temporal.io/server/common/testing/failpoint"
)

// InjectFailpoint registers a failpoint for key in the test cluster, scoped to the test's
// namespace, and returns it for the test to configure. The failpoint is disabled and removed when
// the test ends.
func InjectFailpoint[T any](env *TestEnv, key failpoint.Key) *failpoint.Failpoint[T] {
	scoped := key.ForNamespace(env.nsID)
	registry := env.cluster.host.testHooks.Failpoints()
	fp := failpoint.NewFailpoint[T](scoped)
	registry.Set(scoped, fp)
	env.t.Cleanup(func() {
		fp.Disable()
		registry.Delete(scoped)
	})
	return fp
}
