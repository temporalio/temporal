package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestClusterPool_GlobalOverridesSurviveTestCleanup(t *testing.T) {
	dc := dynamicconfig.NewMemoryClient()

	t.Run("apply", func(t *testing.T) {
		// Apply global defaults the same way newTemporal does: via PartialOverrideValue
		// without registering a t.Cleanup, so they persist beyond the test's lifetime.
		for k, v := range defaultDynamicConfigOverrides {
			dc.PartialOverrideValue(k, v)
		}
	})
	// "apply" subtest finished - its t.Cleanup has run.
	// Global overrides must still be in place.
	for k, v := range defaultDynamicConfigOverrides {
		got := dc.GetValue(k)
		require.NotEmpty(t, got, "key %s missing after cleanup", k)
		require.Equal(t, v, got[0].Value, "key %s wrong after cleanup", k)
	}
}

func TestClusterPool_MaxLeasesRecyclesOnNextAcquire(t *testing.T) {
	// maxLeases 1 makes the first completed lease immediately eligible for recycling.
	p := newClusterPool(1, false, 1)
	slot := p.allSlots[0]
	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{}
	}

	t.Run("uses cluster", func(t *testing.T) {
		cluster := p.get(t, createCluster)
		require.Same(t, cluster, slot.cluster)
		require.Equal(t, 1, slot.activeLeases)
		require.Equal(t, 1, slot.leaseCount)
	})

	// The subtest cleanup has released the only active lease.
	firstCluster := slot.cluster
	require.NotNil(t, firstCluster)
	require.Equal(t, 0, slot.activeLeases)
	require.Equal(t, 1, slot.leaseCount)

	// Lease-limit recycling happens on the next acquire after the prior lease releases.
	t.Run("recreates cluster", func(t *testing.T) {
		cluster := p.get(t, createCluster)
		require.Same(t, cluster, slot.cluster)
		require.NotSame(t, firstCluster, cluster)
		require.Equal(t, 2, created)
	})
}

func TestClusterPool_FreshClusterOptionReplacesIdlePooledCluster(t *testing.T) {
	p := newClusterPool(1, true, 0)
	slot := p.allSlots[0]
	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{}
	}

	t.Run("uses pooled cluster", func(t *testing.T) {
		cluster := p.get(t, createCluster)
		require.Same(t, cluster, slot.cluster)
	})

	pooledCluster := slot.cluster
	require.NotNil(t, pooledCluster)

	t.Run("uses fresh cluster", func(t *testing.T) {
		cluster := p.getFresh(t, createCluster)
		require.NotSame(t, pooledCluster, cluster)
		require.Nil(t, slot.cluster)
	})

	require.Nil(t, slot.cluster)
	require.Equal(t, 2, created)
}

func TestClusterPool_MaxLeasesWaitsForActiveLeases(t *testing.T) {
	// maxLeases is already reached after the first acquire, but the slot is still active.
	p := newClusterPool(1, false, 1)
	slot := p.allSlots[0]
	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{}
	}

	activeCluster := p.get(t, createCluster)
	concurrentCluster := p.get(t, createCluster)

	// Concurrent leases share the current cluster even after usage crosses maxLeases.
	require.Same(t, activeCluster, concurrentCluster)
	require.Equal(t, 1, created)
	require.Equal(t, 2, slot.activeLeases)
	require.Equal(t, 2, slot.leaseCount)
	require.NotNil(t, slot.cluster)
}

func TestClusterPool_PoisonedActiveClusterSwapsWithoutRecycling(t *testing.T) {
	// Use maxLeases 1 to prove poison replacement wins over lease-limit recycling.
	p := newClusterPool(1, false, 1)
	slot := p.allSlots[0]
	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{
			t: &sharedClusterT{name: t.Name()},
		}
	}

	poisonedCluster := p.get(t, createCluster)
	poisonedCluster.t.failed.Store(true)

	// Poison swaps the slot immediately, but the old active lease still has to release.
	replacementCluster := p.get(t, createCluster)

	require.NotSame(t, poisonedCluster, replacementCluster)
	require.Same(t, replacementCluster, slot.cluster)
	require.Equal(t, 2, created)
	// The old poisoned lease remains active, while leaseCount restarts on the replacement.
	require.Equal(t, 2, slot.activeLeases)
	require.Equal(t, 1, slot.leaseCount)
}
