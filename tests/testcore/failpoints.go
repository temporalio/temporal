package testcore

import (
	"slices"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
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
	fp := failpoint.New(key)
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
	return cluster.host.failpointRegistry
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

// buggifiable lists the dynamic config settings that a test cluster with BUGGIFY randomizes, with
// the alternative values that are legal in every deployment and likely to expose bugs.
var buggifiable = []struct {
	setting dynamicconfig.GenericSetting
	values  []int
}{
	// Small task writer batches split concurrent task appends into several writes.
	{dynamicconfig.MatchingMaxTaskBatchSize, []int{1, 2, 10}},
	// Small batches make the timer queue page through its tasks and advance its reader often.
	{dynamicconfig.TimerTaskBatchSize, []int{1, 2, 10}},
	// Small batches make the transfer queue page through its tasks and advance its reader often.
	{dynamicconfig.TransferTaskBatchSize, []int{1, 2, 10}},
}

// BuggifiableSettings returns the alternative values of the settings that a test cluster with
// BUGGIFY randomizes, by key.
func BuggifiableSettings() map[dynamicconfig.Key][]int {
	settings := make(map[dynamicconfig.Key][]int, len(buggifiable))
	for _, b := range buggifiable {
		settings[b.setting.Key()] = b.values
	}
	return settings
}

// WithBuggify runs the test on a dedicated cluster whose servers run BUGGIFY with the seed, through
// the testing.buggify dynamic config setting, like a deployment would. The overrides are the keys of
// the setting, see serverbuggify.Config: they adjust the default keys. The dynamic config settings
// of BuggifiableSettings are randomized too: from the seed, each is activated with a probability of
// 0.25, and then takes one of its values for the whole test. A setting that the test sets with
// WithDynamicConfig keeps the value of the test, whatever the order of the options. A seed of 0
// leaves BUGGIFY off. The setting is global, so the test needs a dedicated cluster.
func WithBuggify(seed uint64, overrides ...serverbuggify.Key) TestOption {
	return func(o *testOptions) {
		o.dedicatedReason = "BUGGIFY dynamic config"
		WithDynamicConfig(serverbuggify.Setting, serverbuggify.Config{Seed: seed, Keys: overrides})(o)
		for _, b := range buggifiable {
			explicit := slices.ContainsFunc(o.dynamicConfigSettings, func(c dynamicConfigOverride) bool {
				return c.setting.Key() == b.setting.Key()
			})
			if value, ok := buggify.Pick(seed, b.setting.Key().String(), 0.25, b.values...); ok && !explicit {
				WithDynamicConfig(b.setting, value)(o)
			}
		}
	}
}
