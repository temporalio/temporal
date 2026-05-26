package testcore

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
)

var testClusterPool *clusterPool

func init() {
	sharedSize := max(1, runtime.GOMAXPROCS(0)/2)
	if v := os.Getenv("TEMPORAL_TEST_SHARED_CLUSTERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			panic("TEMPORAL_TEST_SHARED_CLUSTERS must be a positive integer")
		}
		sharedSize = n
	}

	dedicatedSize := runtime.GOMAXPROCS(0)
	if v := os.Getenv("TEMPORAL_TEST_DEDICATED_CLUSTERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			panic("TEMPORAL_TEST_DEDICATED_CLUSTERS must be a positive integer")
		}
		dedicatedSize = n
	}

	// In CI, recreate clusters after 50 tests to prevent resource accumulation.
	// Locally, clusters are reused indefinitely for faster iteration.
	var maxUsage int
	if os.Getenv("CI") != "" {
		maxUsage = 50
	}

	testClusterPool = &clusterPool{
		pools: newClusterPools(sharedSize, dedicatedSize, maxUsage),
	}
}

// pool manages a fixed number of test clusters with lazy initialization.
type pool struct {
	clusters []*FunctionalTestBase
	inits    []sync.Once
	counter  atomic.Int64 // for round-robin (when slots is nil)
	slots    chan int     // for exclusive access (nil means shared/concurrent access)

	// For shared pools: track usage and support teardown/recreate after maxUsage tests
	usageCounts []atomic.Int64
	clusterMu   []sync.Mutex // protects cluster teardown/recreate
	maxUsage    int          // max tests per cluster before recreate (0 = unlimited)
	createFn    func() *FunctionalTestBase
}

func newPool(size int, exclusive bool) *pool {
	p := &pool{
		clusters:    make([]*FunctionalTestBase, size),
		inits:       make([]sync.Once, size),
		usageCounts: make([]atomic.Int64, size),
		clusterMu:   make([]sync.Mutex, size),
	}
	if exclusive {
		p.slots = make(chan int, size)
		for i := range size {
			p.slots <- i
		}
	}
	return p
}

func newPoolWithMaxUsage(size int, exclusive bool, maxUsage int) *pool {
	p := newPool(size, exclusive)
	p.maxUsage = maxUsage
	return p
}

func newClusterPools(sharedSize, dedicatedSize, maxUsage int) map[clusterPoolKey]*pool {
	pools := make(map[clusterPoolKey]*pool, 4)
	for _, key := range []clusterPoolKey{
		{kind: poolKindShared},
		{kind: poolKindShared, workerService: true},
		{kind: poolKindDedicated},
		{kind: poolKindDedicated, workerService: true},
	} {
		size := sharedSize
		if key.kind == poolKindDedicated {
			size = dedicatedSize
		}
		pools[key] = newPoolWithMaxUsage(size, key.kind == poolKindDedicated, maxUsage)
	}
	return pools
}

func DefaultSuiteClusterPoolSize() int {
	return max(1, runtime.GOMAXPROCS(0)/2)
}

// get returns a cluster from the pool, creating it lazily if needed.
// For exclusive pools, blocks until a slot is available and registers cleanup.
// For shared pools, uses round-robin.
// Both pool types may recreate clusters after maxUsage tests (in CI).
func (p *pool) get(t *testing.T, createCluster func() *FunctionalTestBase) *FunctionalTestBase {
	var idx int
	if p.slots != nil {
		idx = <-p.slots
		t.Cleanup(func() { p.slots <- idx })
	} else {
		idx = int(p.counter.Add(1)-1) % len(p.clusters)
	}

	// Check if we need to recreate the cluster after maxUsage tests
	if p.maxUsage > 0 {
		usage := p.usageCounts[idx].Add(1)
		if usage > int64(p.maxUsage) {
			p.clusterMu[idx].Lock()
			// Double-check after acquiring lock
			if p.usageCounts[idx].Load() > int64(p.maxUsage) && p.clusters[idx] != nil {
				if err := p.clusters[idx].tearDownTestCluster(); err != nil {
					t.Logf("Failed to tear down cluster %d: %v", idx, err)
				}
				p.clusters[idx] = createCluster()
				p.usageCounts[idx].Store(1) // Reset to 1 (this test counts)
			}
			p.clusterMu[idx].Unlock()
		}
	}

	// Lazy initialization for first use
	p.inits[idx].Do(func() {
		p.clusters[idx] = createCluster()
	})

	cluster := p.clusters[idx]

	// Swap out poisoned clusters. The poisoned cluster will tear itself down during its last
	// test run's cleanup.
	if cluster.Poisoned() {
		p.clusterMu[idx].Lock()
		if p.clusters[idx].Poisoned() {
			p.clusters[idx] = createCluster()
			if p.maxUsage > 0 {
				p.usageCounts[idx].Store(1)
			}
		}
		cluster = p.clusters[idx]
		p.clusterMu[idx].Unlock()
	}

	cluster.SetT(t)
	return cluster
}

// acquireSlot gets exclusive access to a slot without using a pooled cluster.
// Used when a fresh cluster is needed (e.g., custom dynamic config).
func (p *pool) acquireSlot(t *testing.T) {
	if p.slots == nil {
		return
	}
	idx := <-p.slots
	t.Cleanup(func() { p.slots <- idx })
}

type clusterPool struct {
	pools       map[clusterPoolKey]*pool
	suiteScoped sync.Map
}

type suiteScopedCluster struct {
	pools map[clusterPoolKey]*pool
}

type poolKind int

const (
	poolKindShared poolKind = iota
	poolKindDedicated
)

type clusterPoolKey struct {
	kind          poolKind
	workerService bool
}

// UseSuiteScopedClusters makes NewEnv use suite-local cluster pools for all
// tests under `t`. Clusters are created on first use and torn down when `t`
// completes.
func UseSuiteScopedClusters(t *testing.T, size int) {
	t.Helper()
	if size <= 0 {
		t.Fatalf("suite-scoped cluster pool size must be positive, got %d", size)
	}
	rootName, _, _ := strings.Cut(t.Name(), "/")
	if t.Name() != rootName {
		t.Fatalf("UseSuiteScopedClusters must be called from a top-level test, got %q", t.Name())
	}
	suiteCluster := &suiteScopedCluster{
		pools: map[clusterPoolKey]*pool{
			{kind: poolKindShared}:                      newPool(size, false),
			{kind: poolKindShared, workerService: true}: newPool(size, false),
		},
	}
	actual, loaded := testClusterPool.suiteScoped.LoadOrStore(rootName, suiteCluster)
	if loaded {
		suiteCluster = actual.(*suiteScopedCluster)
	}

	t.Cleanup(func() {
		suiteCluster.tearDown(t)
		testClusterPool.suiteScoped.Delete(rootName)
	})
}

func (p *clusterPool) get(t *testing.T, dedicated bool, workerService bool, dynamicConfig map[dynamicconfig.Key]any, clusterOpts []TestClusterOption) (tb *FunctionalTestBase) {
	defer func() {
		tb.RegisterTest(t)
	}()
	if dedicated || len(dynamicConfig) > 0 || len(clusterOpts) > 0 {
		return p.getDedicated(t, workerService, dynamicConfig, clusterOpts)
	}
	if cluster := p.getSuiteScoped(t, workerService); cluster != nil {
		return cluster
	}
	return p.getPooled(t, clusterPoolKey{
		kind:          poolKindShared,
		workerService: workerService,
	}, nil, true, nil)
}

func (p *clusterPool) getSuiteScoped(t *testing.T, workerService bool) *FunctionalTestBase {
	rootName, _, _ := strings.Cut(t.Name(), "/")
	suiteClusterAny, ok := p.suiteScoped.Load(rootName)
	if !ok {
		return nil
	}
	suiteCluster := suiteClusterAny.(*suiteScopedCluster)
	return suiteCluster.get(t, p, workerService)
}

func (p *clusterPool) getDedicated(t *testing.T, workerService bool, dynamicConfig map[dynamicconfig.Key]any, clusterOpts []TestClusterOption) *FunctionalTestBase {
	key := clusterPoolKey{
		kind:          poolKindDedicated,
		workerService: workerService,
	}
	if len(dynamicConfig) > 0 || len(clusterOpts) > 0 {
		// Custom config or fx options require a fresh cluster (can't reuse).
		p.pools[key].acquireSlot(t)
		cluster := p.createCluster(t, dynamicConfig, false, workerService, clusterOpts)

		// Register cleanup to tear down the cluster when the test completes.
		t.Cleanup(func() {
			if err := cluster.tearDownTestCluster(); err != nil {
				t.Logf("Failed to tear down cluster: %v", err)
			}
		})

		return cluster
	}

	// If no custom config is provided, reuse an existing cluster.
	return p.getPooled(t, key, nil, false, nil)
}

func (p *clusterPool) acquireDedicatedSlot(t *testing.T, workerService bool) {
	p.pools[clusterPoolKey{
		kind:          poolKindDedicated,
		workerService: workerService,
	}].acquireSlot(t)
}

func (p *clusterPool) getPooled(t *testing.T, key clusterPoolKey, dynamicConfig map[dynamicconfig.Key]any, shared bool, clusterOpts []TestClusterOption) *FunctionalTestBase {
	return p.pools[key].get(t, func() *FunctionalTestBase {
		return p.createCluster(t, dynamicConfig, shared, key.workerService, clusterOpts)
	})
}

func (s *suiteScopedCluster) get(t *testing.T, clusterPool *clusterPool, workerService bool) *FunctionalTestBase {
	key := clusterPoolKey{
		kind:          poolKindShared,
		workerService: workerService,
	}
	return s.pools[key].get(t, func() *FunctionalTestBase {
		return clusterPool.createCluster(t, nil, true, workerService, nil)
	})
}

func (s *suiteScopedCluster) tearDown(t *testing.T) {
	for _, pool := range s.pools {
		pool.tearDown(t)
	}
}

func (p *pool) tearDown(t *testing.T) {
	for idx, cluster := range p.clusters {
		if cluster == nil {
			continue
		}
		if err := cluster.testCluster.TearDownCluster(); err != nil {
			t.Logf("Failed to tear down suite-scoped cluster %d: %v", idx, err)
		}
	}
}

func (p *clusterPool) createCluster(t *testing.T, dynamicConfig map[dynamicconfig.Key]any, shared bool, workerService bool, clusterOpts []TestClusterOption) *FunctionalTestBase {
	tbase := &FunctionalTestBase{}
	tbase.SetT(t)

	// Keep the worker service off unless explicitly enabled via WithWorkerService.
	opts := []TestClusterOption{withWorkerService(workerService)}
	if shared {
		opts = append(opts, WithSharedCluster())
	}
	if len(dynamicConfig) > 0 {
		opts = append(opts, WithDynamicConfigOverrides(dynamicConfig))
	}
	opts = append(opts, clusterOpts...)

	tbase.setupCluster(opts...)

	return tbase
}
