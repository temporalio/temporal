package testcore

import (
	"testing"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/testx/failpoint"
)

// FailpointOption changes how InjectFailpoint treats the failpoint.
type FailpointOption func(*failpointOptions)

type failpointOptions struct {
	mayNotFire bool
}

// MayNotFire lets the test pass although the failpoint never fired. Without it, the test fails if
// a failpoint that it injected never fired, since a failpoint with the wrong key, scope or
// configuration would otherwise make the test pass without testing anything.
func MayNotFire() FailpointOption {
	return func(o *failpointOptions) { o.mayNotFire = true }
}

// InjectFailpoint registers a failpoint for key in the test cluster, scoped to the test's
// namespace, and returns it for the test to configure. The failpoint is disabled and removed when
// the test ends, and the test fails if the failpoint never fired, unless it has the MayNotFire
// option.
func InjectFailpoint[T any](env *TestEnv, key failpoint.Key[T], opts ...FailpointOption) *failpoint.Failpoint[T] {
	return InjectFailpointIn(env.t, env.cluster, key.ForScope(env.nsID.String()), opts...)
}

// InjectFailpointIn is InjectFailpoint for a cluster other than the one of the test's env, such as
// the standby cluster of a replication test. It registers the failpoint under key as given, so the
// caller scopes it. The failpoint is disabled and removed when t ends.
func InjectFailpointIn[T any](
	t testing.TB,
	cluster *TestCluster,
	key failpoint.Key[T],
	opts ...FailpointOption,
) *failpoint.Failpoint[T] {
	return injectFailpoint(t, FailpointRegistry(cluster), key, opts)
}

// InjectUnscopedFailpoint is InjectFailpoint for a failpoint that applies to all scopes, such as
// a persistence failpoint of a store method whose requests carry no namespace. Like global test
// hooks, it requires a dedicated cluster.
func InjectUnscopedFailpoint[T any](env *TestEnv, key failpoint.Key[T], opts ...FailpointOption) *failpoint.Failpoint[T] {
	if env.isShared && !testClusterRouter.hasSuiteScoped(env.t) {
		env.t.Fatal("InjectUnscopedFailpoint: unscoped failpoints require a dedicated cluster; use testcore.WithDedicatedCluster()")
	}
	env.dedicatedGuard.record("unscoped failpoint injected")
	return InjectFailpointIn(env.t, env.cluster, key, opts...)
}

func injectFailpoint[T any](
	t testing.TB,
	registry *failpoint.Registry,
	key failpoint.Key[T],
	opts []FailpointOption,
) *failpoint.Failpoint[T] {
	var options failpointOptions
	for _, opt := range opts {
		opt(&options)
	}
	if registry == nil {
		t.Fatal("InjectFailpoint: the test cluster has no failpoint registry")
	}
	fp := failpoint.NewFailpoint(key)
	// A failpoint that is already registered, such as a rule of dynamic config or one that the test
	// injected before, is not replaced: it can hold executions that nothing would release.
	registered := false
	registry.Atomically(func() {
		registered = registry.GetOrSet(fp) == fp
	})
	if !registered {
		t.Fatalf("InjectFailpoint: a failpoint is already registered for %s", key)
	}
	t.Cleanup(func() {
		// Only remove this failpoint, not one that replaced it, such as a rule of dynamic config.
		// Remove it before disabling it, so a concurrent injection
		// either configures it before it is disabled, or configures a new one.
		registry.CompareAndDelete(fp)
		fp.Disable()
		if !options.mayNotFire {
			requireFired(t, key, fp)
		}
	})
	return fp
}

// FailpointRegistry returns the failpoint registry of the cluster, for example to look up a
// failpoint. On a shared cluster it holds the failpoints of other tests too.
func FailpointRegistry(cluster *TestCluster) *failpoint.Registry {
	return cluster.host.failpointSource
}

// requireFired fails the test if the failpoint never fired. It counts the fires of the failpoint's
// lifetime, so a test that fired it and then set another trigger passes.
func requireFired[T any](t testing.TB, key failpoint.Key[T], fp *failpoint.Failpoint[T]) {
	t.Helper()
	if fp.TotalFires() == 0 {
		executions, _ := fp.Stats()
		t.Errorf("failpoint %s was injected but never fired (%d executions); check its key, scope and "+
			"configuration, or use testcore.MayNotFire", key, executions)
	}
}

// StandardBuggifyKeys returns the standard failpoints.buggify.keys allowlist: gRPC and HTTP calls
// between services fail with Unavailable, the hand-placed keys take their branch, and dynamic
// config settings get other values. It lists no persistence, response or health key, so BUGGIFY
// never fails those: the health service has an entry with an activation probability of 0, before the
// broad gRPC entry that would match it. docs/development/failpoints.md has the same list as YAML to
// copy into a server config, and a test compares the two.
func StandardBuggifyKeys() []config.FailpointsBuggifyKey {
	probability := func(p float64) *float64 { return &p }
	return []config.FailpointsBuggifyKey{
		{Match: "grpc:/grpc.health.v1.Health/*", ActivationProbability: probability(0), FireProbability: probability(0)},
		{Match: "grpc:*", ActivationProbability: probability(0.1), FireProbability: probability(0.01), Error: "Unavailable"},
		{Match: "http:*", ActivationProbability: probability(0.1), FireProbability: probability(0.01), Error: "Unavailable"},
		{Match: matching.SkipSyncMatchKey.Name(), ActivationProbability: probability(0.25), FireProbability: probability(0.25)},
		{Match: serverbuggify.SettingPrefix + "*", ActivationProbability: probability(0.25), FireProbability: probability(0.25)},
	}
}

// WithBuggify gives the servers of the test's cluster the failpoints.buggify block of the static
// config, which seeds BUGGIFY, lists the keys that it applies to (see StandardBuggifyKeys) and
// switches on the trace. It is the only way that a test cluster configures BUGGIFY: the servers
// read no environment variable and no dynamic config setting. BUGGIFY applies to the whole
// cluster, so this implies a dedicated cluster.
func WithBuggify(buggify config.FailpointsBuggify) TestOption {
	return func(o *testOptions) {
		o.dedicatedCluster = true
		o.dedicatedReason = "BUGGIFY static config"
		o.clusterOptions = append(o.clusterOptions, WithClusterBuggify(buggify))
	}
}

// WithClusterBuggify gives the servers of the cluster the failpoints.buggify block of the static
// config, like WithBuggify, as an option of a suite's cluster, such as that of an XDC suite. It
// does not make the cluster dedicated: the suite already owns its clusters.
func WithClusterBuggify(buggify config.FailpointsBuggify) TestClusterOption {
	return func(params *testClusterParams) {
		params.Buggify = buggify
	}
}
