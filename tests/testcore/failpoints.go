package testcore

import (
	"slices"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
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
