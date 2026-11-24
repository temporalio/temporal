package testcore

import (
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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
	registry := env.cluster.host.testHooks.Failpoints()
	if registry == nil {
		env.t.Fatal("InjectFailpoint: failpoints require test hooks; use -tags test_dep")
	}
	if owned, ok := registry.Get(key).(interface{ Owner() string }); ok && owned.Owner() != "" {
		env.t.Fatalf("InjectFailpoint: failpoint %s is managed by %s", key, owned.Owner())
	}
	fp := failpoint.NewFailpoint[T](key)
	registry.Set(key, fp)
	env.t.Cleanup(func() {
		// Only remove this failpoint, not one that replaced it, such as through FailpointClient.
		// Remove it before disabling it, as the FailpointService does, so a concurrent injection
		// either configures it before it is disabled, or configures a new one.
		registry.CompareAndDelete(key, fp)
		fp.Disable()
	})
	return fp
}

// FailpointClient returns a FailpointService client for the frontend of the test cluster, the way
// a test or tool outside the cluster's process configures failpoints. The frontend fans each
// request out to every host of the cluster. The connection is closed when the test ends.
func FailpointClient(env *TestEnv) failpointpb.FailpointServiceClient {
	creds := insecure.NewCredentials()
	if provider := env.cluster.host.tlsConfigProvider; provider != nil && provider.FrontendClientConfig != nil {
		creds = credentials.NewTLS(provider.FrontendClientConfig)
	}
	conn, err := grpc.NewClient(env.FrontendGRPCAddress(), grpc.WithTransportCredentials(creds))
	if err != nil {
		env.t.Fatalf("FailpointClient: %v", err)
	}
	env.t.Cleanup(func() { _ = conn.Close() })
	return failpointpb.NewFailpointServiceClient(conn)
}

// FailpointRegistry returns the failpoint registry of the test cluster, for example to subscribe
// to its events. On a shared cluster it also reports the events of other tests.
func FailpointRegistry(env *TestEnv) failpoint.Registry {
	return env.cluster.host.testHooks.Failpoints()
}
