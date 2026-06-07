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

func TestPoolMaxUsageRecyclesOnNextAcquire(t *testing.T) {
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

	firstCluster := p.slots[0].cluster
	require.NotNil(t, firstCluster)
	require.Equal(t, 0, p.slots[0].active)
	require.Equal(t, 1, p.slots[0].usage)

	// Max-usage recycling happens on the next acquire after the prior lease releases.
	t.Run("recreates cluster", func(t *testing.T) {
		cluster := p.get(t, createCluster)
		require.Same(t, cluster, p.slots[0].cluster)
		require.NotSame(t, firstCluster, cluster)
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

	// Releasing one of multiple active leases must not tear down the shared cluster.
	slot.release()
	require.NotNil(t, slot.cluster)
	require.Equal(t, 1, slot.active)

	slot.release()
	require.NotNil(t, slot.cluster)
	require.Equal(t, 0, slot.active)
	require.Equal(t, 2, slot.usage)

	third := slot.acquire(t, createCluster)
	require.NotSame(t, first, third)
	require.Equal(t, 2, created)
}

func TestClusterSlotPoisonedActiveClusterSwapsWithoutRecycling(t *testing.T) {
	slot := &clusterSlot{maxUsage: 1}
	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{
			t: &sharedClusterT{name: t.Name()},
		}
	}

	first := slot.acquire(t, createCluster)
	first.t.failed.Store(true)

	// Poison swaps the slot immediately, but the old active lease still has to release.
	second := slot.acquire(t, createCluster)

	require.NotSame(t, first, second)
	require.Same(t, second, slot.cluster)
	require.Equal(t, 2, created)
	require.Equal(t, 2, slot.active)
	require.Equal(t, 1, slot.usage)

	slot.release()
	slot.release()
	require.Equal(t, 0, slot.active)
}
