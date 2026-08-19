package testcore

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestPositiveEnv(t *testing.T) {
	t.Setenv("TEMPORAL_TEST_LIVE_CLUSTERS", "7")
	require.Equal(t, 7, positiveEnv("TEMPORAL_TEST_LIVE_CLUSTERS", 1))
}

func TestNonNegativeEnvAllowsDisabledWarmReserve(t *testing.T) {
	t.Setenv("TEMPORAL_TEST_WARM_CLUSTERS", "0")
	require.Zero(t, nonNegativeEnv("TEMPORAL_TEST_WARM_CLUSTERS", 4))
}

func TestPerTestClusterProviderCreatesAndDestroysEveryCluster(t *testing.T) {
	var created atomic.Int32
	var destroyed atomic.Int32
	provider := newPerTestClusterProvider(
		2,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			created.Add(1)
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error {
			destroyed.Add(1)
			return nil
		},
	)

	first, err := provider.acquire("first", clusterRequest{})
	require.NoError(t, err)
	require.NoError(t, first.release())
	second, err := provider.acquire("second", clusterRequest{})
	require.NoError(t, err)
	require.NoError(t, second.release())

	require.EqualValues(t, 2, created.Load())
	require.EqualValues(t, 2, destroyed.Load())
}

func TestPerTestClusterProviderUsesEachWarmClusterOnce(t *testing.T) {
	var mu sync.Mutex
	var created []*FunctionalTestBase
	var destroyed atomic.Int32
	provider := newPerTestClusterProvider(
		4,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			cluster := &FunctionalTestBase{}
			mu.Lock()
			created = append(created, cluster)
			mu.Unlock()
			return cluster, nil
		},
		func(*FunctionalTestBase) error {
			destroyed.Add(1)
			return nil
		},
	)
	require.NoError(t, provider.startWarmReserve(1, 1))
	t.Cleanup(func() { require.NoError(t, provider.stopWarmReserve()) })

	first, err := provider.acquire("first", clusterRequest{})
	require.NoError(t, err)
	mu.Lock()
	require.Same(t, created[0], first.cluster)
	mu.Unlock()
	await.RequireTrue(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(created) == 2
	}, time.Second, time.Millisecond)
	require.NoError(t, first.release())

	second, err := provider.acquire("second", clusterRequest{})
	require.NoError(t, err)
	mu.Lock()
	require.Same(t, created[1], second.cluster)
	mu.Unlock()
	require.NoError(t, second.release())
	require.EqualValues(t, 2, destroyed.Load())
}

func TestPerTestClusterProviderKeepsCustomClustersOutOfWarmReserve(t *testing.T) {
	var mu sync.Mutex
	var requests []clusterRequest
	provider := newPerTestClusterProvider(
		3,
		func(_ clusterTest, request clusterRequest) (*FunctionalTestBase, error) {
			mu.Lock()
			requests = append(requests, request)
			mu.Unlock()
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	require.NoError(t, provider.startWarmReserve(1, 1))
	t.Cleanup(func() { require.NoError(t, provider.stopWarmReserve()) })

	custom, err := provider.acquire("custom", clusterRequest{needWorkerService: true})
	require.NoError(t, err)
	mu.Lock()
	require.Len(t, requests, 2)
	require.False(t, requests[0].needWorkerService)
	require.True(t, requests[1].needWorkerService)
	mu.Unlock()

	core, err := provider.acquire("core", clusterRequest{})
	require.NoError(t, err)
	require.NotSame(t, custom.cluster, core.cluster)
	require.NoError(t, custom.release())
	require.NoError(t, core.release())
}

func TestPerTestClusterProviderEvictsWarmClusterForCustomDemand(t *testing.T) {
	provider := newPerTestClusterProvider(
		2,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	require.NoError(t, provider.startWarmReserve(1, 1))
	t.Cleanup(func() { require.NoError(t, provider.stopWarmReserve()) })
	first, err := provider.acquire("first", clusterRequest{needWorkerService: true})
	require.NoError(t, err)

	acquired := make(chan *perTestClusterLease, 1)
	go func() {
		lease, _ := provider.acquire("second", clusterRequest{needWorkerService: true})
		acquired <- lease
	}()
	select {
	case second := <-acquired:
		require.NoError(t, first.release())
		require.NoError(t, second.release())
	case <-time.After(100 * time.Millisecond):
		require.NoError(t, first.release())
		second := <-acquired
		require.NoError(t, second.release())
		t.Fatal("custom cluster demand blocked behind an unused warm cluster")
	}
}

func TestPerTestClusterProviderUsesRefillThatFinishesWhileWaiting(t *testing.T) {
	var created atomic.Int32
	refillStarted := make(chan struct{})
	allowRefill := make(chan struct{})
	provider := newPerTestClusterProvider(
		2,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			if created.Add(1) == 2 {
				close(refillStarted)
				<-allowRefill
			}
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	require.NoError(t, provider.startWarmReserve(1, 1))
	t.Cleanup(func() { require.NoError(t, provider.stopWarmReserve()) })
	first, err := provider.acquire("first", clusterRequest{})
	require.NoError(t, err)
	<-refillStarted

	acquired := make(chan *perTestClusterLease, 1)
	acquireStarted := make(chan struct{})
	go func() {
		close(acquireStarted)
		lease, _ := provider.acquire("second", clusterRequest{})
		acquired <- lease
	}()
	<-acquireStarted
	select {
	case <-acquired:
		t.Fatal("second cluster acquired before the refill completed")
	default:
	}
	close(allowRefill)
	select {
	case second := <-acquired:
		require.EqualValues(t, 2, created.Load())
		require.NoError(t, first.release())
		require.NoError(t, second.release())
	case <-time.After(100 * time.Millisecond):
		require.NoError(t, first.release())
		second := <-acquired
		require.NoError(t, second.release())
		t.Fatal("waiting acquisition did not use the completed warm refill")
	}
}

func TestPerTestClusterProviderEvictsRefillThatFinishesWhileCustomDemandWaits(t *testing.T) {
	var created atomic.Int32
	refillStarted := make(chan struct{})
	allowRefill := make(chan struct{})
	provider := newPerTestClusterProvider(
		2,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			if created.Add(1) == 2 {
				close(refillStarted)
				<-allowRefill
			}
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	require.NoError(t, provider.startWarmReserve(1, 1))
	t.Cleanup(func() { require.NoError(t, provider.stopWarmReserve()) })
	core, err := provider.acquire("core", clusterRequest{})
	require.NoError(t, err)
	<-refillStarted

	acquired := make(chan *perTestClusterLease, 1)
	acquireStarted := make(chan struct{})
	go func() {
		close(acquireStarted)
		lease, _ := provider.acquire("custom", clusterRequest{needWorkerService: true})
		acquired <- lease
	}()
	<-acquireStarted
	select {
	case <-acquired:
		t.Fatal("custom cluster acquired before the refill completed")
	default:
	}
	close(allowRefill)
	select {
	case custom := <-acquired:
		require.NoError(t, core.release())
		require.NoError(t, custom.release())
	case <-time.After(100 * time.Millisecond):
		require.NoError(t, core.release())
		custom := <-acquired
		require.NoError(t, custom.release())
		t.Fatal("waiting custom demand did not evict the completed warm refill")
	}
}

func TestPerTestClusterProviderStopsUnusedWarmClusters(t *testing.T) {
	var created atomic.Int32
	var destroyed atomic.Int32
	provider := newPerTestClusterProvider(
		4,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			created.Add(1)
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error {
			destroyed.Add(1)
			return nil
		},
	)

	require.NoError(t, provider.startWarmReserve(2, 1))
	require.NoError(t, provider.stopWarmReserve())
	require.EqualValues(t, 2, created.Load())
	require.EqualValues(t, 2, destroyed.Load())
	require.Empty(t, provider.live)
}

func TestPerTestClusterProviderRunsWhenWarmReserveStartupFails(t *testing.T) {
	wantErr := errors.New("boot failed")
	provider := newPerTestClusterProvider(
		1,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			return nil, wantErr
		},
		func(*FunctionalTestBase) error { return nil },
	)
	runCalled := false

	exitCode := provider.runWithWarmReserve(func() int {
		runCalled = true
		return 7
	}, 1, 1)

	require.True(t, runCalled)
	require.Equal(t, 7, exitCode)
	require.Empty(t, provider.live)
}

func TestPerTestClusterProviderDisablesRefillsAfterBackgroundFailure(t *testing.T) {
	wantErr := errors.New("refill failed")
	var created atomic.Int32
	refillFailed := make(chan struct{})
	provider := newPerTestClusterProvider(
		2,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			if created.Add(1) == 2 {
				close(refillFailed)
				return nil, wantErr
			}
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	require.NoError(t, provider.startWarmReserve(1, 1))
	t.Cleanup(func() { require.NoError(t, provider.stopWarmReserve()) })

	first, err := provider.acquire("first", clusterRequest{})
	require.NoError(t, err)
	<-refillFailed
	await.RequireTrue(t, func() bool {
		provider.warmMu.Lock()
		defer provider.warmMu.Unlock()
		return provider.warm.refillsDisabled
	}, time.Second, time.Millisecond)
	require.NoError(t, first.release())

	second, err := provider.acquire("second", clusterRequest{})
	require.NoError(t, err)
	require.NoError(t, second.release())
	require.EqualValues(t, 3, created.Load())
}

func TestPerTestClusterProviderWaitsForRefillTeardown(t *testing.T) {
	var created atomic.Int32
	refillStarted := make(chan struct{})
	allowRefill := make(chan struct{})
	destroyStarted := make(chan struct{})
	allowDestroy := make(chan struct{})
	var destroyed atomic.Int32
	provider := newPerTestClusterProvider(
		3,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			if created.Add(1) > 1 {
				close(refillStarted)
				<-allowRefill
			}
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error {
			if destroyed.Add(1) == 1 {
				close(destroyStarted)
				<-allowDestroy
			}
			return nil
		},
	)
	require.NoError(t, provider.startWarmReserve(1, 1))
	lease, err := provider.acquire("test", clusterRequest{})
	require.NoError(t, err)
	<-refillStarted

	stopped := make(chan error, 1)
	go func() { stopped <- provider.stopWarmReserve() }()
	await.RequireTrue(t, func() bool {
		provider.warmMu.Lock()
		defer provider.warmMu.Unlock()
		return provider.warm == nil
	}, time.Second, time.Millisecond)
	close(allowRefill)
	<-destroyStarted
	returnedEarly := false
	select {
	case <-stopped:
		returnedEarly = true
	case <-time.After(50 * time.Millisecond):
	}

	close(allowDestroy)
	if !returnedEarly {
		require.NoError(t, <-stopped)
	}
	require.False(t, returnedEarly, "warm reserve stopped before refill teardown completed")
	require.NoError(t, lease.release())
}

func TestPerTestClusterProviderReportsRefillTeardownFailure(t *testing.T) {
	wantErr := errors.New("teardown failed")
	var created atomic.Int32
	refillStarted := make(chan struct{})
	allowRefill := make(chan struct{})
	var destroyed atomic.Int32
	provider := newPerTestClusterProvider(
		3,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			if created.Add(1) > 1 {
				close(refillStarted)
				<-allowRefill
			}
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error {
			if destroyed.Add(1) == 1 {
				return wantErr
			}
			return nil
		},
	)
	require.NoError(t, provider.startWarmReserve(1, 1))
	lease, err := provider.acquire("test", clusterRequest{})
	require.NoError(t, err)
	<-refillStarted

	stopped := make(chan error, 1)
	go func() { stopped <- provider.stopWarmReserve() }()
	await.RequireTrue(t, func() bool {
		provider.warmMu.Lock()
		defer provider.warmMu.Unlock()
		return provider.warm == nil
	}, time.Second, time.Millisecond)
	close(allowRefill)
	require.ErrorIs(t, <-stopped, wantErr)
	require.NoError(t, lease.release())
}

func TestPerTestClusterProviderBoundsOwnedClusters(t *testing.T) {
	provider := newPerTestClusterProvider(
		1,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	first, err := provider.acquire("first", clusterRequest{})
	require.NoError(t, err)

	acquired := make(chan *perTestClusterLease, 1)
	go func() {
		lease, _ := provider.acquire("second", clusterRequest{})
		acquired <- lease
	}()
	select {
	case <-acquired:
		t.Fatal("second cluster acquired before the first was released")
	default:
	}

	require.NoError(t, first.release())
	second := <-acquired
	require.NoError(t, second.release())
}

func TestPerTestClusterProviderLeavesHeadroomForParallelTests(t *testing.T) {
	provider := &perTestClusterProvider{live: make(chan struct{}, 4)}
	require.Equal(t, 2, provider.testParallelism(4))
}

func TestPerTestClusterProviderRejectsLimitWithoutHeadroom(t *testing.T) {
	provider := &perTestClusterProvider{live: make(chan struct{}, 1)}
	require.Panics(t, func() { provider.testParallelism(1) })
}

func TestPerTestClusterProviderCleansUpFailedCreation(t *testing.T) {
	wantErr := errors.New("boot failed")
	var destroyed atomic.Int32
	provider := newPerTestClusterProvider(
		1,
		func(clusterTest, clusterRequest) (*FunctionalTestBase, error) {
			return &FunctionalTestBase{}, wantErr
		},
		func(*FunctionalTestBase) error {
			destroyed.Add(1)
			return nil
		},
	)

	_, err := provider.acquire("failed", clusterRequest{})
	require.ErrorIs(t, err, wantErr)
	require.EqualValues(t, 1, destroyed.Load())
	require.Empty(t, provider.live)
}

func TestClusterRouterPreservesSuiteWorkerRequirement(t *testing.T) {
	var captured clusterRequest
	router := &clusterRouter{legacySlots: make(chan struct{}, 1)}
	router.perTest = newPerTestClusterProvider(
		1,
		func(_ clusterTest, request clusterRequest) (*FunctionalTestBase, error) {
			captured = request
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)
	router.suiteWorkerServices.Store(t.Name(), "deployment APIs")
	t.Run("leaf", func(t *testing.T) {
		router.get(t, clusterRequest{})
	})

	require.True(t, captured.needWorkerService)
	require.Equal(t, "worker service required: deployment APIs", captured.reason)
}

func TestClusterRouterReusesOneClusterLeasePerTest(t *testing.T) {
	var created atomic.Int32
	var destroyed atomic.Int32
	router := &clusterRouter{legacySlots: make(chan struct{}, 1)}
	router.perTest = newPerTestClusterProvider(
		2,
		func(_ clusterTest, _ clusterRequest) (*FunctionalTestBase, error) {
			created.Add(1)
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error {
			destroyed.Add(1)
			return nil
		},
	)

	t.Run("leaf", func(t *testing.T) {
		first := router.get(t, clusterRequest{})
		second := router.get(t, clusterRequest{})

		require.Same(t, first, second)
		require.EqualValues(t, 1, created.Load())
		require.Len(t, router.perTest.live, 1)
	})

	require.EqualValues(t, 1, destroyed.Load())
	require.Empty(t, router.perTest.live)
}

func TestPerTestClusterProviderRejectsLaterClusterRequirements(t *testing.T) {
	provider := newPerTestClusterProvider(
		2,
		func(_ clusterTest, _ clusterRequest) (*FunctionalTestBase, error) {
			return &FunctionalTestBase{}, nil
		},
		func(*FunctionalTestBase) error { return nil },
	)

	t.Run("leaf", func(t *testing.T) {
		_, err := provider.clusterForTest(t, clusterRequest{})
		require.NoError(t, err)

		_, err = provider.clusterForTest(t, clusterRequest{needWorkerService: true})
		require.ErrorContains(t, err, "worker service")
	})
}
