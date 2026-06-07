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

func TestClusterPool_MaxUsageRecyclesOnNextAcquire(t *testing.T) {
	// maxUsage 1 makes the first completed lease immediately eligible for recycling.
	p := newClusterPool(1, false, 1)
	slot := p.slots[0]
	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{}
	}

	t.Run("uses cluster", func(t *testing.T) {
		cluster := p.get(t, createCluster)
		require.Same(t, cluster, slot.cluster)
		require.Equal(t, 1, slot.active)
		require.Equal(t, 1, slot.usage)
	})

	// The subtest cleanup has released the only active lease.
	firstCluster := slot.cluster
	require.NotNil(t, firstCluster)
	require.Equal(t, 0, slot.active)
	require.Equal(t, 1, slot.usage)

	// Max-usage recycling happens on the next acquire after the prior lease releases.
	t.Run("recreates cluster", func(t *testing.T) {
		cluster := p.get(t, createCluster)
		require.Same(t, cluster, slot.cluster)
		require.NotSame(t, firstCluster, cluster)
		require.Equal(t, 2, created)
	})
}

func TestClusterPoolSlot_MaxUsageWaitsForActiveLeases(t *testing.T) {
	// maxUsage is already reached after the first acquire, but the slot is still active.
	slot := &clusterPoolSlot{maxUsage: 1}
	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{}
	}

	activeCluster := slot.acquire(t, createCluster)
	concurrentCluster := slot.acquire(t, createCluster)

	// Concurrent leases share the current cluster even after usage crosses maxUsage.
	require.Same(t, activeCluster, concurrentCluster)
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

	// Once all leases are gone, the next acquire can recycle the overused cluster.
	recycledCluster := slot.acquire(t, createCluster)
	require.NotSame(t, activeCluster, recycledCluster)
	require.Equal(t, 2, created)
}

func TestClusterPoolSlot_PoisonedActiveClusterSwapsWithoutRecycling(t *testing.T) {
	// Use maxUsage 1 to prove poison replacement wins over max-usage recycling.
	slot := &clusterPoolSlot{maxUsage: 1}
	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{
			t: &sharedClusterT{name: t.Name()},
		}
	}

	poisonedCluster := slot.acquire(t, createCluster)
	poisonedCluster.t.failed.Store(true)

	// Poison swaps the slot immediately, but the old active lease still has to release.
	replacementCluster := slot.acquire(t, createCluster)

	require.NotSame(t, poisonedCluster, replacementCluster)
	require.Same(t, replacementCluster, slot.cluster)
	require.Equal(t, 2, created)
	// The old poisoned lease remains active, while usage restarts on the replacement.
	require.Equal(t, 2, slot.active)
	require.Equal(t, 1, slot.usage)

	slot.release()
	slot.release()
	require.Equal(t, 0, slot.active)
}
