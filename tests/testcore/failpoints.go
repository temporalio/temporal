package testcore

import (
	"go.temporal.io/testx/failpoint"
)

// InjectFailpoint registers a failpoint for key in the test cluster, scoped to the test's
// namespace, and returns it for the test to configure. The failpoint is disabled and removed when
// the test ends.
func InjectFailpoint[T any](env *TestEnv, key failpoint.Key) *failpoint.Failpoint[T] {
	return injectFailpoint[T](env, key.ForScope(env.nsID.String()))
}

// InjectGlobalFailpoint is InjectFailpoint for a failpoint that applies to all namespaces, such as
// a persistence failpoint of a store method whose requests carry no namespace. Like global test
// hooks, it requires a dedicated cluster.
func InjectGlobalFailpoint[T any](env *TestEnv, key failpoint.Key) *failpoint.Failpoint[T] {
	if env.isShared && !testClusterRouter.hasSuiteScoped(env.t) {
		env.t.Fatal("InjectGlobalFailpoint: global failpoints require a dedicated cluster; use testcore.WithDedicatedCluster()")
	}
	env.dedicatedGuard.record("global failpoint injected")
	return injectFailpoint[T](env, key)
}

func injectFailpoint[T any](env *TestEnv, key failpoint.Key) *failpoint.Failpoint[T] {
	registry := FailpointRegistry(env)
	if registry == nil {
		env.t.Fatal("InjectFailpoint: the test cluster has no failpoint registry")
	}
	if owned, ok := registry.Get(key).(interface{ Owner() string }); ok && owned.Owner() != "" {
		env.t.Fatalf("InjectFailpoint: failpoint %s is managed by %s", key, owned.Owner())
	}
	fp := failpoint.NewFailpoint[T](key)
	registry.Set(key, fp)
	env.t.Cleanup(func() {
		// Only remove this failpoint, not one that replaced it, such as a rule of dynamic config.
		// Remove it before disabling it, so a concurrent injection
		// either configures it before it is disabled, or configures a new one.
		registry.CompareAndDelete(key, fp)
		fp.Disable()
	})
	return fp
}

// FailpointRegistry returns the failpoint registry of the test cluster, for example to subscribe
// to its events. On a shared cluster it also reports the events of other tests.
func FailpointRegistry(env *TestEnv) failpoint.Registry {
	return env.cluster.host.failpointSource.Load().Failpoints()
}
