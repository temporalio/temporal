package testcore

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
)

func TestConfiguredClusterMode(t *testing.T) {
	require.Equal(t, clusterModePerTest, configuredClusterMode(""))
	require.Equal(t, clusterModePooled, configuredClusterMode(clusterModePooled))
}

func TestConfiguredPerTestClusterLimits(t *testing.T) {
	t.Setenv("TEMPORAL_TEST_LIVE_CLUSTERS", "")
	t.Setenv("TEMPORAL_TEST_WARM_SPARES", "")

	maxLiveTests, warmSpares := configuredPerTestClusterLimits()
	require.Equal(t, 40, maxLiveTests)
	require.Zero(t, warmSpares)

	t.Setenv("TEMPORAL_TEST_LIVE_CLUSTERS", "7")
	t.Setenv("TEMPORAL_TEST_WARM_SPARES", "3")

	maxLiveTests, warmSpares = configuredPerTestClusterLimits()
	require.Equal(t, 7, maxLiveTests)
	require.Equal(t, 3, warmSpares)
}

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

func TestDefaultDynamicConfigOverridesForCluster(t *testing.T) {
	local := defaultDynamicConfigOverridesForCluster(false)
	require.Equal(t, 64, local[dynamicconfig.TransferProcessorSchedulerWorkerCount.Key()])
	require.Equal(t, 64, local[dynamicconfig.TimerProcessorSchedulerWorkerCount.Key()])
	require.Equal(t, 64, local[dynamicconfig.VisibilityProcessorSchedulerWorkerCount.Key()])
	require.Equal(t, 64, local[dynamicconfig.MemoryTimerProcessorSchedulerWorkerCount.Key()])
	require.Equal(t, 64, local[dynamicconfig.ArchivalProcessorSchedulerWorkerCount.Key()])
	require.Equal(t, 64, local[dynamicconfig.ReplicationProcessorSchedulerWorkerCount.Key()])
	require.Equal(t, 64, local[dynamicconfig.ReplicationLowPriorityProcessorSchedulerWorkerCount.Key()])

	global := defaultDynamicConfigOverridesForCluster(true)
	require.NotContains(t, global, dynamicconfig.ReplicationProcessorSchedulerWorkerCount.Key())
	require.NotContains(t, global, dynamicconfig.ReplicationLowPriorityProcessorSchedulerWorkerCount.Key())
	require.Equal(t, 64, global[dynamicconfig.TransferProcessorSchedulerWorkerCount.Key()])
	require.Equal(t, 64, global[dynamicconfig.ArchivalProcessorSchedulerWorkerCount.Key()])
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

func TestClusterRequestCanUseWarmSpare(t *testing.T) {
	require.True(t, (clusterRequest{}).canUseWarmSpare())
	require.True(t, (clusterRequest{dedicated: true}).canUseWarmSpare())
	require.True(t, (clusterRequest{needWorkerService: true}).canUseWarmSpare())
	require.True(t, (clusterRequest{dynamicConfig: map[dynamicconfig.Key]any{dynamicconfig.TransferProcessorSchedulerWorkerCount.Key(): 1}}).canUseWarmSpare())
	require.False(t, (clusterRequest{
		dynamicConfig:         map[dynamicconfig.Key]any{dynamicconfig.TransferProcessorSchedulerWorkerCount.Key(): 1},
		requiresStartupConfig: true,
	}).canUseWarmSpare())
	require.False(t, (clusterRequest{clusterOpts: []TestClusterOption{func(*testClusterParams) {}}}).canUseWarmSpare())
}

func TestPerTestClusterProviderAppliesDynamicConfigToWarmSpare(t *testing.T) {
	dc := dynamicconfig.NewMemoryClient()
	cluster := &FunctionalTestBase{
		testCluster: &TestCluster{host: &temporalImpl{dcClient: dc}},
	}
	var inlineCreates atomic.Int64
	provider := newPerTestClusterProvider(
		1,
		1,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			inlineCreates.Add(1)
			return cluster, nil
		},
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			return cluster, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	t.Cleanup(provider.close)
	require.NoError(t, provider.startAndWait())

	key := dynamicconfig.TransferProcessorSchedulerWorkerCount.Key()
	lease, err := provider.acquire(t.Name(), clusterRequest{dynamicConfig: map[dynamicconfig.Key]any{key: 17}})
	require.NoError(t, err)
	require.Equal(t, clusterAcquireSourceWarmSpare, lease.acquireSource)
	require.NoError(t, lease.release())

	require.Zero(t, inlineCreates.Load())
	values := dc.GetValue(key)
	require.Len(t, values, 1)
	require.Equal(t, 17, values[0].Value)
}

func TestPerTestClusterProviderLimitsOwnedClustersAndDestroysOnRelease(t *testing.T) {
	var created atomic.Int64
	var destroyed atomic.Int64
	provider := newPerTestClusterProvider(
		1,
		0,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			created.Add(1)
			return &FunctionalTestBase{}, nil
		},
		nil,
		func(*FunctionalTestBase) error {
			destroyed.Add(1)
			return nil
		},
	)
	t.Cleanup(provider.close)

	first, err := provider.acquire(t.Name(), clusterRequest{})
	require.NoError(t, err)
	type acquireResult struct {
		lease *perTestClusterLease
		err   error
	}
	acquired := make(chan acquireResult, 1)
	go func() {
		lease, err := provider.acquire(t.Name()+"/second", clusterRequest{})
		acquired <- acquireResult{lease: lease, err: err}
	}()

	select {
	case result := <-acquired:
		if result.err == nil {
			require.NoError(t, result.lease.release())
		}
		t.Fatal("second cluster acquired before the first was released")
	case <-time.After(20 * time.Millisecond):
	}

	require.NoError(t, first.release())

	var second *perTestClusterLease
	select {
	case result := <-acquired:
		require.NoError(t, result.err)
		second = result.lease
	case <-time.After(time.Second):
		t.Fatal("second cluster did not acquire after the first was released")
	}
	require.NoError(t, second.release())
	require.Equal(t, int64(2), created.Load())
	require.Equal(t, int64(2), destroyed.Load())
	require.Equal(t, clusterAcquireSourceWarmMiss, first.acquireSource)
	require.Equal(t, clusterAcquireSourceWarmMiss, second.acquireSource)
	require.NotSame(t, first.cluster, second.cluster)
	require.True(t, first.cluster.usePreseededNamespace)
	require.True(t, second.cluster.usePreseededNamespace)
}

func TestPerTestClusterProviderBoundsGoTestParallelism(t *testing.T) {
	provider := newPerTestClusterProvider(
		2,
		0,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) { return &FunctionalTestBase{}, nil },
		nil,
		func(*FunctionalTestBase) error { return nil },
	)
	t.Cleanup(provider.close)

	require.Equal(t, 2, provider.testParallelism(12))
	require.Equal(t, 1, provider.testParallelism(1))
}

func TestPerTestClusterProviderConvertsBootPanicToError(t *testing.T) {
	provider := newPerTestClusterProvider(
		1,
		0,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			panic("boot panic")
		},
		nil,
		func(*FunctionalTestBase) error { return nil },
	)
	t.Cleanup(provider.close)

	lease, err := provider.acquire(t.Name(), clusterRequest{})
	require.Nil(t, lease)
	require.ErrorContains(t, err, "cluster boot panicked: boot panic")
}

func TestPerTestClusterProviderRejectsFailedBoot(t *testing.T) {
	var destroyed atomic.Int64
	provider := newPerTestClusterProvider(
		1,
		0,
		func(owner clusterTest, _ clusterRequest) (*FunctionalTestBase, error) {
			owner.Errorf("boot failed: %s", "bad config")
			return &FunctionalTestBase{}, nil
		},
		nil,
		func(*FunctionalTestBase) error {
			destroyed.Add(1)
			return nil
		},
	)
	t.Cleanup(provider.close)

	lease, err := provider.acquire(t.Name(), clusterRequest{})
	require.Nil(t, lease)
	require.ErrorContains(t, err, "boot failed: bad config")
	require.Equal(t, int64(1), destroyed.Load())
}

func TestPerTestClusterProviderAbortsOnBootFailNow(t *testing.T) {
	var continued atomic.Bool
	provider := newPerTestClusterProvider(
		1,
		0,
		func(owner clusterTest, _ clusterRequest) (*FunctionalTestBase, error) {
			owner.FailNow()
			continued.Store(true)
			return &FunctionalTestBase{}, nil
		},
		nil,
		func(*FunctionalTestBase) error { return nil },
	)
	t.Cleanup(provider.close)

	lease, err := provider.acquire(t.Name(), clusterRequest{})
	require.Nil(t, lease)
	require.ErrorContains(t, err, "cluster boot failed")
	require.False(t, continued.Load())
}

func TestPerTestClusterProviderDestroysClusterReturnedWithError(t *testing.T) {
	wantErr := errors.New("boot failed")
	var destroyed atomic.Int64
	provider := newPerTestClusterProvider(
		1,
		0,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			return &FunctionalTestBase{}, wantErr
		},
		nil,
		func(*FunctionalTestBase) error {
			destroyed.Add(1)
			return nil
		},
	)
	t.Cleanup(provider.close)

	lease, err := provider.acquire(t.Name(), clusterRequest{})
	require.Nil(t, lease)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, int64(1), destroyed.Load())
}

func TestPerTestClusterProviderDoesNotStartSparesForIneligibleRequest(t *testing.T) {
	warmSpareStarted := make(chan struct{})
	provider := newPerTestClusterProvider(
		1,
		1,
		func(_ clusterTest, request clusterRequest) (*FunctionalTestBase, error) {
			if request.dedicatedReason == "warm spare" {
				close(warmSpareStarted)
			}
			return &FunctionalTestBase{}, nil
		},
		nil,
		func(*FunctionalTestBase) error { return nil },
	)
	t.Cleanup(provider.close)

	lease, err := provider.acquire(t.Name(), clusterRequest{clusterOpts: []TestClusterOption{func(*testClusterParams) {}}})
	require.NoError(t, err)
	require.Equal(t, clusterAcquireSourceCustom, lease.acquireSource)
	t.Cleanup(func() { require.NoError(t, lease.release()) })

	select {
	case <-warmSpareStarted:
		t.Fatal("ineligible cluster request started the warm-spare filler")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestPerTestClusterProviderUsesReadyCreatorOnlyForWarmSpares(t *testing.T) {
	var inlineCreates atomic.Int64
	var warmCreates atomic.Int64
	provider := newPerTestClusterProvider(
		1,
		1,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			inlineCreates.Add(1)
			return &FunctionalTestBase{}, nil
		},
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			warmCreates.Add(1)
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	t.Cleanup(provider.close)

	lease, err := provider.acquire(t.Name(), clusterRequest{clusterOpts: []TestClusterOption{func(*testClusterParams) {}}})
	require.NoError(t, err)
	require.NoError(t, lease.release())
	require.Equal(t, int64(1), inlineCreates.Load())
	require.Zero(t, warmCreates.Load())

	require.NoError(t, provider.startAndWait())
	require.Equal(t, int64(1), inlineCreates.Load())
	require.Equal(t, int64(1), warmCreates.Load())
}

func TestPerTestClusterProviderFillsCoreAndWorkerInventoriesConcurrently(t *testing.T) {
	createStarted := make(chan bool, 2)
	releaseCreate := make(chan struct{})
	provider := newPerTestClusterProvider(
		2,
		2,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			return &FunctionalTestBase{}, nil
		},
		func(_ clusterTest, request clusterRequest) (*FunctionalTestBase, error) {
			createStarted <- request.needWorkerService
			<-releaseCreate
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	t.Cleanup(provider.close)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCreate) }) }
	t.Cleanup(release)

	started := make(chan error, 1)
	go func() { started <- provider.startAndWait() }()

	createdClasses := make(map[bool]struct{}, 2)
	for range 2 {
		select {
		case worker := <-createStarted:
			createdClasses[worker] = struct{}{}
		case <-time.After(time.Second):
			t.Fatal("core and worker warm-spare creation did not run concurrently")
		}
	}
	require.Contains(t, createdClasses, false)
	require.Contains(t, createdClasses, true)

	release()
	require.NoError(t, <-started)
}

func TestPerTestClusterProviderUsesWorkerWarmSpare(t *testing.T) {
	dc := dynamicconfig.NewMemoryClient()
	workerCluster := &FunctionalTestBase{
		testCluster: &TestCluster{host: &temporalImpl{dcClient: dc}},
	}
	var inlineCreates atomic.Int64
	var workerWarmCreates atomic.Int64
	provider := newPerTestClusterProvider(
		1,
		2,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			inlineCreates.Add(1)
			return workerCluster, nil
		},
		func(_ clusterTest, request clusterRequest) (*FunctionalTestBase, error) {
			if request.needWorkerService {
				workerWarmCreates.Add(1)
				return workerCluster, nil
			}
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	t.Cleanup(provider.close)
	require.NoError(t, provider.startAndWait())

	key := dynamicconfig.WorkerHeartbeatsEnabled.Key()
	lease, err := provider.acquire(t.Name(), clusterRequest{
		needWorkerService: true,
		dynamicConfig:     map[dynamicconfig.Key]any{key: true},
	})
	require.NoError(t, err)
	require.Equal(t, clusterAcquireSourceWarmSpare, lease.acquireSource)
	require.NoError(t, lease.release())

	require.Zero(t, inlineCreates.Load())
	require.Equal(t, int64(1), workerWarmCreates.Load())
	values := dc.GetValue(key)
	require.Len(t, values, 1)
	require.Equal(t, true, values[0].Value)
}

func TestClusterRouterPerTestModePreservesSuiteWorkerRequirement(t *testing.T) {
	var captured clusterRequest
	router := &clusterRouter{}
	router.perTest = newPerTestClusterProvider(
		1,
		0,
		func(_ clusterTest, request clusterRequest) (*FunctionalTestBase, error) {
			captured = request
			return &FunctionalTestBase{}, nil
		},
		nil,
		func(*FunctionalTestBase) error { return nil },
	)
	t.Cleanup(router.perTest.close)
	router.suiteWorkerServices.Store(t.Name(), "deployment APIs")

	require.NotNil(t, router.get(t, clusterRequest{}))
	require.True(t, captured.needWorkerService)
	require.Equal(t, "worker service required: deployment APIs", captured.dedicatedReason)
}
