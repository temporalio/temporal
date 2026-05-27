package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/primitives"
)

func TestClusterPool_GlobalOverridesSurviveTestCleanup(t *testing.T) {
	var dcClient *dynamicconfig.MemoryClient

	t.Run("create", func(t *testing.T) {
		impl := newTemporal(t, &TemporalParams{
			ClusterMetadataConfig: &cluster.Config{},
			HostsByProtocolByService: map[transferProtocol]map[primitives.ServiceName]static.Hosts{
				httpProtocol: {
					primitives.FrontendService: {
						All: []string{"127.0.0.1:0"},
					},
				},
			},
		})
		dcClient = impl.dcClient
	})
	// "create" subtest finished - its t.Cleanup has run.
	// Global overrides must still be in place.
	for k, v := range defaultDynamicConfigOverrides {
		got := dcClient.GetValue(k)
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
