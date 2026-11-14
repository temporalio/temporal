//go:build test_dep

package testcore

import (
	"go.temporal.io/server/common/testing/failpoint"
)

// InjectFailpoint registers a failpoint for key in the test cluster, scoped to the test's
// namespace, and returns it for the test to configure. The failpoint is disabled and removed when
// the test ends.
func InjectFailpoint[T any](env *TestEnv, key failpoint.Key) *failpoint.Failpoint[T] {
	return injectFailpoint[T](env, key.ForNamespace(env.nsID))
}

// InjectGlobalFailpoint is InjectFailpoint for a failpoint that applies to all namespaces, such as
// a persistence failpoint whose requests carry no namespace. Like global test hooks, it requires a
// dedicated cluster.
func InjectGlobalFailpoint[T any](env *TestEnv, key failpoint.Key) *failpoint.Failpoint[T] {
	if env.isShared && !testClusterRouter.hasSuiteScoped(env.t) {
		env.t.Fatal("InjectGlobalFailpoint: global failpoints require a dedicated cluster; use testcore.WithDedicatedCluster()")
	}
	env.dedicatedGuard.record("global failpoint injected")
	return injectFailpoint[T](env, key)
}

func injectFailpoint[T any](env *TestEnv, key failpoint.Key) *failpoint.Failpoint[T] {
	registry := env.cluster.host.testHooks.Failpoints()
	fp := failpoint.NewFailpoint[T](key)
	registry.Set(key, fp)
	env.t.Cleanup(func() {
		fp.Disable()
		registry.Delete(key)
	})
	return fp
}
