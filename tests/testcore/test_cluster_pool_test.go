package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestGlobalOverridesSurviveTestCleanup(t *testing.T) {
	var dcClient *dynamicconfig.MemoryClient

	t.Run("create", func(t *testing.T) {
		impl := newTemporal(t, &TemporalParams{
			ClusterMetadataConfig: &cluster.Config{},
		})
		dcClient = impl.dcClient
	})
	// "create" subtest finished — its t.Cleanup has run
	// but we expect global dynamic config overrides to still be in place.
	for k, v := range defaultDynamicConfigOverrides {
		got := dcClient.GetValue(k)
		require.NotEmpty(t, got, "key %s missing after cleanup", k)
		require.Equal(t, v, got[0].Value, "key %s wrong after cleanup", k)
	}
}

func TestSuiteScopedClusterConfigMatchesClusterOptions(t *testing.T) {
	shared := newSuiteScopedClusterConfig(nil)
	customShards := newSuiteScopedClusterConfig([]TestClusterOption{WithNumHistoryShards(8)})

	require.True(t, customShards.matches(newSuiteScopedClusterConfig([]TestClusterOption{WithNumHistoryShards(8)})))
	require.False(t, shared.matches(customShards))
}

func TestPoolMaxUsageRecyclesAfterActiveTest(t *testing.T) {
	p := newPool(1, false, 1)

	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{}
	}

	t.Run("uses cluster", func(t *testing.T) {
		cluster := p.get(t, createCluster)

		require.Same(t, cluster, p.slots[0].cluster)
		require.Equal(t, 1, p.slots[0].active)
		require.Equal(t, 1, p.slots[0].usage)
	})

	require.Nil(t, p.slots[0].cluster)
	require.Equal(t, 0, p.slots[0].active)
	require.Equal(t, 0, p.slots[0].usage)

	t.Run("recreates cluster", func(t *testing.T) {
		cluster := p.get(t, createCluster)

		require.Same(t, cluster, p.slots[0].cluster)
		require.Equal(t, 2, created)
	})
}

func TestClusterSlotMaxUsageWaitsForActiveLeases(t *testing.T) {
	slot := &clusterSlot{maxUsage: 1}

	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{}
	}

	first := slot.acquire(t, createCluster)
	second := slot.acquire(t, createCluster)
	require.Same(t, first, second)
	require.Equal(t, 1, created)
	require.Equal(t, 2, slot.active)
	require.Equal(t, 2, slot.usage)

	slot.release(t)
	require.NotNil(t, slot.cluster)
	require.Equal(t, 1, slot.active)

	slot.release(t)
	require.Nil(t, slot.cluster)
	require.Equal(t, 0, slot.active)
	require.Equal(t, 0, slot.usage)
}

func TestSuiteScopedClusterSharesClusterAndNamespaces(t *testing.T) {
	UseSuiteScopedCluster(t, "reuse suite cluster")

	var firstCluster *TestCluster
	var firstNamespace string

	t.Run("first", func(t *testing.T) {
		env := NewEnv(t)

		firstCluster = env.GetTestCluster()
		firstNamespace = env.Namespace().String()
		require.True(t, env.isShared)
		require.False(t, env.GetTestClusterConfig().WorkerConfig.DisableWorker)
	})

	t.Run("second", func(t *testing.T) {
		env := NewEnv(t)

		require.Same(t, firstCluster, env.GetTestCluster())
		require.NotEqual(t, firstNamespace, env.Namespace().String())
		require.True(t, env.isShared)
		require.False(t, env.GetTestClusterConfig().WorkerConfig.DisableWorker)
	})
}
