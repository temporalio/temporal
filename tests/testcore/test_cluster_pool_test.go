package testcore

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPositiveEnv(t *testing.T) {
	t.Setenv("TEMPORAL_TEST_LIVE_CLUSTERS", "7")
	require.Equal(t, 7, positiveEnv("TEMPORAL_TEST_LIVE_CLUSTERS", 1))
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
