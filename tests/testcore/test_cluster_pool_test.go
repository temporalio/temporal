package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
)

// clusterCreateFnArgs allow us to inspect the arguments passed to the cluster creation function in unit tests of this module.
// This works when paired with a mocked clusterCreationFn that populates a slice of these structs for later inspection.
type clusterCreateFnArgs struct {
	shared        bool
	workerEnabled bool
}

// newMockClusterPoolRouter creates a clusterRouter with small pools and a mockClusterCreationFn
// that simply records calls. It allows testing routing logic without starting a real server.
// Note that for the function calls, we need to return *[]clusterCreateFnArgs so that the appended
// clusterCreateFnArgs are visible to the caller.
func newMockClusterPoolRouter(t *testing.T) (*clusterRouter, *[]clusterCreateFnArgs) {
	t.Helper()

	var calls []clusterCreateFnArgs
	mockClusterCreationFn := func(t *testing.T, dc map[dynamicconfig.Key]any, shared bool, opts []TestClusterOption) *FunctionalTestBase {
		// Keep the worker service off unless explicitly enabled via WithWorkerService (this mirrors the actual createClusterFn implementation).
		baseOpts := []TestClusterOption{withWorkerService(false)}
		params := ApplyTestClusterOptions(append(baseOpts, opts...))
		calls = append(calls, clusterCreateFnArgs{shared: shared, workerEnabled: params.EnableWorkerService})
		return &FunctionalTestBase{}
	}

	r := &clusterRouter{
		shared:           newClusterPool(2, false, 0),
		sharedWithWorker: newClusterPool(1, false, 0), // small pool to force reuse of the same cluster for multiple tests
		dedicated:        newClusterPool(2, true, 0),
		clusterCreatorFn: mockClusterCreationFn,
	}
	return r, &calls
}

func TestClusterPool_GlobalOverridesSurviveTestCleanup(t *testing.T) {
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

func TestClusterPool_SharedWorkerServiceRouting(t *testing.T) {
	t.Run("worker service uses and reuses shared-with-worker pool", func(t *testing.T) {
		router, calls := newMockClusterPoolRouter(t)
		var first, second *FunctionalTestBase

		t.Run("first call creates cluster", func(t *testing.T) {
			first = router.get(t, false /* dedicated */, true /* workerService */, nil, nil)
		})
		require.Equal(t, 1, router.sharedWithWorker.allSlots[0].leaseCount, "sharedWithWorker should have one lease")
		require.Equal(t, 0, router.shared.allSlots[0].leaseCount, "shared pool should be unused")
		require.Len(t, *calls, 1)
		require.Equal(t, clusterCreateFnArgs{shared: true, workerEnabled: true}, (*calls)[0])

		t.Run("second call reuses cluster", func(t *testing.T) {
			second = router.get(t, false /* dedicated */, true /* workerService */, nil, nil)
		})
		require.Same(t, first, second, "second call should return the same cluster, not allocate a new one")
		require.Len(t, *calls, 1, "no new cluster should have been created")
		require.Equal(t, 2, router.sharedWithWorker.allSlots[0].leaseCount, "slot 0 should have been leased twice")
		require.Equal(t, 0, router.shared.allSlots[0].leaseCount+router.shared.allSlots[1].leaseCount,
			"plain shared pool should be unused")
	})

	t.Run("worker service with dedicated uses dedicated pool", func(t *testing.T) {
		router, calls := newMockClusterPoolRouter(t)

		router.get(t, true /* dedicated */, true /* workerService */, nil, nil)
		require.Equal(t, 0, router.sharedWithWorker.allSlots[0].leaseCount, "sharedWithWorker pool should be unused")
		require.Len(t, *calls, 1)
		require.Equal(t, clusterCreateFnArgs{shared: false, workerEnabled: true}, (*calls)[0])
	})

	t.Run("no worker service uses plain shared pool", func(t *testing.T) {
		router, calls := newMockClusterPoolRouter(t)

		router.get(t, false /* dedicated */, false /* workerService */, nil, nil)
		require.Equal(t, 1, router.shared.allSlots[0].leaseCount, "shared pool should have one lease")
		require.Equal(t, 0, router.sharedWithWorker.allSlots[0].leaseCount, "sharedWithWorker pool should be unused")
		require.Len(t, *calls, 1)
		require.Equal(t, clusterCreateFnArgs{shared: true, workerEnabled: false}, (*calls)[0])
	})
}
