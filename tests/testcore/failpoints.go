package testcore

import (
	"testing"

	"go.temporal.io/testx/failpoint"
)

// FailpointRegistry returns the failpoint registry of the cluster, for example to look up a
// failpoint. On a shared cluster it holds the failpoints of other tests too.
func FailpointRegistry(cluster *TestCluster) *failpoint.Registry {
	return cluster.host.failpointRegistry
}

// InjectFailpoint registers a failpoint for key, scoped to the namespace of env, in the registry
// of the test cluster. It returns the failpoint to configure, for example with On. The test's
// cleanup removes and disables it.
func InjectFailpoint[T any](env *TestEnv, key failpoint.Key[T]) *failpoint.Failpoint[T] {
	return InjectFailpointIn(env.T(), env.GetTestCluster(), key.ForScope(env.NamespaceID().String()))
}

// InjectFailpointIn is like InjectFailpoint for a key that is already scoped, and a cluster that
// is not necessarily the one of a TestEnv. It fails the test for an unscoped key, which would apply
// to every namespace of a shared cluster.
func InjectFailpointIn[T any](t testing.TB, cluster *TestCluster, key failpoint.Key[T]) *failpoint.Failpoint[T] {
	if key.Scope() == "" {
		t.Fatalf("failpoint %s is unscoped and would apply to every namespace of a shared cluster", key)
	}
	registry := FailpointRegistry(cluster)
	fp := failpoint.New(key)
	registry.Set(fp)
	t.Cleanup(func() {
		// Remove only this registration; the test may have replaced it.
		if registry.Get(key) == fp {
			registry.Delete(key)
		}
		fp.Disable()
	})
	return fp
}
