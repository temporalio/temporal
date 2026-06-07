package testcore

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
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
		shared:    newPool(sharedSize, false, maxUsage),
		dedicated: newPool(dedicatedSize, true, maxUsage),
	}
}

// pool manages a fixed number of test cluster slots.
type pool struct {
	slots []*clusterSlot
	next  int
	mu    sync.Mutex

	available chan *clusterSlot // for exclusive access (nil means shared/concurrent access)
}

type clusterSlot struct {
	idx int
	mu  sync.Mutex

	cluster *FunctionalTestBase

	// For shared pools: track usage and support teardown/recreate after maxUsage tests
	usage    int
	active   int
	maxUsage int // max tests per cluster before recreate (0 = unlimited)
}

func newPool(size int, exclusive bool, maxUsage int) *pool {
	p := &pool{
		slots: make([]*clusterSlot, size),
	}
	for i := range size {
		p.slots[i] = &clusterSlot{
			idx:      i,
			maxUsage: maxUsage,
		}
	}
	if exclusive {
		p.available = make(chan *clusterSlot, size)
		for _, slot := range p.slots {
			p.available <- slot
		}
	}
	return p
}

// get returns a cluster from the pool, creating it lazily if needed.
// For exclusive pools, blocks until a slot is available and registers cleanup.
// For shared pools, uses round-robin.
// Both pool types may recreate clusters after maxUsage tests (in CI).
func (p *pool) get(t *testing.T, createCluster func() *FunctionalTestBase) *FunctionalTestBase {
	slot := p.reserveSlot(t)
	cluster := slot.acquire(t, createCluster)
	t.Cleanup(slot.release)
	return cluster
}

func (p *pool) reserveSlot(t *testing.T) *clusterSlot {
	if p.available != nil {
		slot := <-p.available
		t.Cleanup(func() { p.available <- slot })
		return slot
	}
	return p.nextSlot()
}

func (p *pool) nextSlot() *clusterSlot {
	p.mu.Lock()
	defer p.mu.Unlock()
	slot := p.slots[p.next]
	p.next = (p.next + 1) % len(p.slots)
	return slot
}

func (s *clusterSlot) acquire(t *testing.T, createCluster func() *FunctionalTestBase) *FunctionalTestBase {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Lazy initialization for first use
	if s.cluster == nil {
		s.cluster = createCluster()
	}
	cluster := s.cluster

	// Swap out poisoned clusters. The poisoned cluster will tear itself down during its last
	// test run's cleanup.
	if cluster.Poisoned() {
		if s.active == 0 {
			s.tearDownLocked(t)
		}
		s.cluster = createCluster()
		s.usage = 0
		cluster = s.cluster
	}

	// Check if we need to recreate the cluster after maxUsage tests
	if s.maxUsage > 0 && s.usage >= s.maxUsage && s.active == 0 {
		s.tearDownLocked(t)
		s.cluster = createCluster()
		cluster = s.cluster
	}

	s.usage++
	s.active++
	cluster.SetT(t)
	return cluster
}

func (s *clusterSlot) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == 0 {
		panic("release called without matching acquire")
	}
	s.active--
}

func (s *clusterSlot) tearDownLocked(t *testing.T) {
	if s.cluster == nil {
		return
	}
	if err := s.cluster.tearDownTestCluster(); err != nil {
		t.Logf("Failed to tear down cluster %d: %v", s.idx, err)
	}
	s.cluster = nil
	s.usage = 0
}

type clusterPool struct {
	shared      *pool
	dedicated   *pool
	suiteScoped sync.Map
}

type suiteScopedCluster struct {
	once    sync.Once
	cluster *FunctionalTestBase
}

// UseSuiteScopedCluster makes NewEnv use one cluster for all tests under `t`.
// The cluster is created on first use and torn down when `t` completes.
//
// Deprecated: this only exists for backwards-compatibility with legacy sequential
// suite execution.
func UseSuiteScopedCluster(t *testing.T) {
	t.Helper()
	rootName, _, _ := strings.Cut(t.Name(), "/")
	if t.Name() != rootName {
		t.Fatalf("UseSuiteScopedCluster must be called from a top-level test, got %q", t.Name())
	}
	testClusterPool.suiteScoped.LoadOrStore(rootName, &suiteScopedCluster{})

	t.Cleanup(func() {
		suiteClusterAny, ok := testClusterPool.suiteScoped.Load(rootName)
		if ok {
			suiteCluster := suiteClusterAny.(*suiteScopedCluster)
			if suiteCluster.cluster != nil {
				if err := suiteCluster.cluster.tearDownTestCluster(); err != nil {
					t.Logf("Failed to tear down suite-scoped cluster: %v", err)
				}
			}
		}
		testClusterPool.suiteScoped.Delete(rootName)
	})
}

func (p *clusterPool) get(t *testing.T, dedicated bool, dynamicConfig map[dynamicconfig.Key]any, clusterOpts []TestClusterOption) (tb *FunctionalTestBase) {
	defer func() {
		tb.RegisterTest(t)
	}()
	if dedicated || len(dynamicConfig) > 0 || len(clusterOpts) > 0 {
		return p.getDedicated(t, dynamicConfig, clusterOpts)
	}
	if cluster := p.getSuiteScoped(t); cluster != nil {
		return cluster
	}
	return p.getShared(t)
}

func (p *clusterPool) getShared(t *testing.T) *FunctionalTestBase {
	return p.shared.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, nil, true, nil)
	})
}

func (p *clusterPool) getSuiteScoped(t *testing.T) *FunctionalTestBase {
	rootName, _, _ := strings.Cut(t.Name(), "/")
	if _, ok := p.suiteScoped.Load(rootName); !ok {
		return nil
	}

	suiteClusterAny, _ := p.suiteScoped.LoadOrStore(rootName, &suiteScopedCluster{})
	suiteCluster := suiteClusterAny.(*suiteScopedCluster)
	suiteCluster.once.Do(func() {
		suiteCluster.cluster = p.createCluster(t, nil, true, nil)
	})
	suiteCluster.cluster.SetT(t)
	return suiteCluster.cluster
}

func (p *clusterPool) getDedicated(t *testing.T, dynamicConfig map[dynamicconfig.Key]any, clusterOpts []TestClusterOption) *FunctionalTestBase {
	if len(dynamicConfig) > 0 || len(clusterOpts) > 0 {
		// Custom config or fx options require a fresh cluster (can't reuse).
		p.dedicated.reserveSlot(t)
		cluster := p.createCluster(t, dynamicConfig, false, clusterOpts)

		// Register cleanup to tear down the cluster when the test completes.
		t.Cleanup(func() {
			if err := cluster.tearDownTestCluster(); err != nil {
				t.Logf("Failed to tear down cluster: %v", err)
			}
		})

		return cluster
	}

	// If no custom config is provided, reuse an existing cluster.
	return p.dedicated.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, nil, false, nil)
	})
}

func (p *clusterPool) createCluster(t *testing.T, dynamicConfig map[dynamicconfig.Key]any, shared bool, clusterOpts []TestClusterOption) *FunctionalTestBase {
	tbase := &FunctionalTestBase{}
	tbase.SetT(t)

	// Keep the worker service off unless explicitly enabled via WithWorkerService.
	opts := []TestClusterOption{withWorkerService(false)}
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
