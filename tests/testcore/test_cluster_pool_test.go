package testcore

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
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

func TestClusterPool_ExclusiveClusterReusesAfterRelease(t *testing.T) {
	p := newClusterPool(3, true, 0)
	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{}
	}

	t.Run("uses pooled cluster", func(t *testing.T) {
		cluster := p.get(t, createCluster)
		require.NotNil(t, cluster)
	})

	pooledCluster := p.allSlots[2].cluster
	require.NotNil(t, pooledCluster)

	t.Run("reuses pooled cluster", func(t *testing.T) {
		cluster := p.get(t, createCluster)
		require.Same(t, pooledCluster, cluster)
	})

	require.Same(t, pooledCluster, p.allSlots[2].cluster)
	require.Equal(t, 1, created)
}

func TestClusterPool_OneOffClusterUsesSeparatePool(t *testing.T) {
	dedicated := newClusterPool(1, true, 0)
	oneOff := newOneOffClusterPool(1)
	dedicatedSlot := dedicated.allSlots[0]
	var created int
	createCluster := func() *FunctionalTestBase {
		created++
		return &FunctionalTestBase{}
	}

	t.Run("uses pooled cluster", func(t *testing.T) {
		cluster := dedicated.get(t, createCluster)
		require.Same(t, cluster, dedicatedSlot.cluster)
	})

	pooledCluster := dedicatedSlot.cluster
	require.NotNil(t, pooledCluster)

	t.Run("uses fresh cluster", func(t *testing.T) {
		cluster := oneOff.get(t, createCluster)
		require.NotSame(t, pooledCluster, cluster)
		require.Same(t, pooledCluster, dedicatedSlot.cluster)
	})

	require.Same(t, pooledCluster, dedicatedSlot.cluster)
	require.Equal(t, 2, created)
}

func TestClusterPool_RecordsOneOffClusterKind(t *testing.T) {
	eventsFile, err := os.CreateTemp(t.TempDir(), "cluster-events-*.jsonl")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eventsFile.Close()) })

	router := &clusterRouter{eventsFile: eventsFile}
	router.recordCreation(t, clusterRequest{
		kind:            clusterKindOneOff,
		dedicatedReason: "custom history shard count used",
	})

	_, err = eventsFile.Seek(0, 0)
	require.NoError(t, err)
	var event struct {
		Kind   string `json:"kind"`
		Reason string `json:"reason"`
	}
	require.NoError(t, json.NewDecoder(eventsFile).Decode(&event))
	require.Equal(t, clusterKindOneOff, event.Kind)
	require.Equal(t, "custom history shard count used", event.Reason)
}

func TestClusterPool_ReservedSlotBlocksNextExclusiveOwner(t *testing.T) {
	p := newClusterPool(1, true, 0)
	acquired := make(chan struct{})
	parentT := t

	t.Run("reserved", func(t *testing.T) {
		p.reserveSlot(t)

		go func() {
			p.reserveSlot(parentT)
			close(acquired)
		}()

		require.Never(t, func() bool {
			select {
			case <-acquired:
				return true
			default:
				return false
			}
		}, 50*time.Millisecond, 5*time.Millisecond)
	})

	await.RequireTrue(t, func() bool {
		select {
		case <-acquired:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
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
