package testcore

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
)

var testClusterRouter *clusterRouter

func init() {
	applyPoolSizeOverride := func(size *int, envVar string) {
		if v := os.Getenv(envVar); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				panic(fmt.Sprintf("Failed to parse %s with %v", envVar, err))
			} else if n <= 0 {
				panic(fmt.Sprintf("%s must be a positive integer", envVar))
			}
			*size = n
		}
	}

	sharedSize := max(1, runtime.GOMAXPROCS(0)/2)
	applyPoolSizeOverride(&sharedSize, "TEMPORAL_TEST_SHARED_CLUSTERS")

	sharedWorkerSize := max(1, runtime.GOMAXPROCS(0)/2)
	applyPoolSizeOverride(&sharedWorkerSize, "TEMPORAL_TEST_SHARED_WORKER_CLUSTERS")

	dedicatedSize := runtime.GOMAXPROCS(0)
	applyPoolSizeOverride(&dedicatedSize, "TEMPORAL_TEST_DEDICATED_CLUSTERS")

	r := &clusterRouter{
		shared:           newClusterPool(sharedSize, false, 0),
		sharedWithWorker: newClusterPool(sharedWorkerSize, false, 0),
		dedicated:        newClusterPool(dedicatedSize, true, 0),
	}
	r.createClusterFn = createCluster
	testClusterRouter = r
}

// clusterPool manages a fixed number of test [clusterPoolSlot]s.
type clusterPool struct {
	sync.Mutex
	allSlots       []*clusterPoolSlot
	availableSlots chan *clusterPoolSlot // for exclusive access (nil means shared/concurrent access)
	nextSlotIdx    int
}

// clusterPoolSlot owns one pooled cluster and its lease state.
type clusterPoolSlot struct {
	sync.Mutex
	idx          int
	cluster      *FunctionalTestBase
	activeLeases int // how many tests are currently using this cluster
	leaseCount   int // how often it has been leased
	maxLeases    int // max tests per cluster before recreate (0 = unlimited)
}

func newClusterPool(size int, exclusive bool, maxLeases int) *clusterPool {
	p := &clusterPool{
		allSlots: make([]*clusterPoolSlot, size),
	}
	for i := range size {
		p.allSlots[i] = &clusterPoolSlot{
			idx:       i,
			maxLeases: maxLeases,
		}
	}
	if exclusive {
		p.availableSlots = make(chan *clusterPoolSlot, size)
		for _, slot := range p.allSlots {
			p.availableSlots <- slot
		}
	}
	return p
}

// get returns a cluster from the [clusterPool], creating it lazily if needed.
// For exclusive pools, blocks until a slot is available and registers cleanup.
// For shared pools, uses round-robin.
func (p *clusterPool) get(t *testing.T, createCluster func() *FunctionalTestBase) *FunctionalTestBase {
	slot := p.reserveSlot(t)
	cluster := slot.acquire(t, createCluster)
	t.Cleanup(slot.release)
	return cluster
}

func (p *clusterPool) reserveSlot(t *testing.T) *clusterPoolSlot {
	if p.availableSlots != nil {
		slot := <-p.availableSlots
		t.Cleanup(func() { p.availableSlots <- slot })
		return slot
	}
	return p.nextSlot()
}

func (p *clusterPool) nextSlot() *clusterPoolSlot {
	p.Lock()
	defer p.Unlock()
	slot := p.allSlots[p.nextSlotIdx]
	p.nextSlotIdx = (p.nextSlotIdx + 1) % len(p.allSlots)
	return slot
}

func (s *clusterPoolSlot) acquire(t *testing.T, createCluster func() *FunctionalTestBase) *FunctionalTestBase {
	s.Lock()
	defer s.Unlock()

	// Lazy initialization for first use
	if s.cluster == nil {
		s.cluster = createCluster()
	}
	cluster := s.cluster

	// Swap out poisoned clusters. An active poisoned cluster will tear itself down during its
	// last test run's cleanup; an idle poisoned cluster can be torn down here.
	if cluster.Poisoned() {
		if s.activeLeases == 0 {
			s.tearDownLocked(t)
		}
		s.cluster = createCluster()
		s.leaseCount = 0
		cluster = s.cluster
	}

	// Recreate idle clusters after the lease limit is reached.
	if s.maxLeases > 0 && s.leaseCount >= s.maxLeases && s.activeLeases == 0 {
		s.tearDownLocked(t)
		s.cluster = createCluster()
		cluster = s.cluster
	}

	s.leaseCount++
	s.activeLeases++
	cluster.SetT(t)
	return cluster
}

func (s *clusterPoolSlot) release() {
	s.Lock()
	defer s.Unlock()
	if s.activeLeases == 0 {
		panic("release called without matching acquire")
	}
	s.activeLeases--
}

func (s *clusterPoolSlot) tearDownLocked(t *testing.T) {
	if s.cluster == nil {
		return
	}
	if err := s.cluster.tearDownTestCluster(); err != nil {
		t.Logf("Failed to tear down cluster %d: %v", s.idx, err)
	}
	s.cluster = nil
	s.leaseCount = 0
}

// clusterRequest carries all the parameters needed to create a test cluster
// in the correct pool.
type clusterRequest struct {
	dedicated         bool
	needWorkerService bool
	dynamicConfig     map[dynamicconfig.Key]any
	clusterOpts       []TestClusterOption
}

// mustBeFresh returns true if the request requires a freshly created cluster, rather than an
// existing one in one of the cluster pools.
func (r clusterRequest) mustBeFresh() bool { return len(r.dynamicConfig) > 0 || len(r.clusterOpts) > 0 }

func (r clusterRequest) needsDedicated() bool { return r.dedicated || r.mustBeFresh() }

// clusterRouter routes tests to shared/dedicated [clusterPool] or [suiteScopedCluster]s.
type clusterRouter struct {
	shared           *clusterPool
	sharedWithWorker *clusterPool
	dedicated        *clusterPool
	suiteScoped      sync.Map

	// createClusterFn is the function used to create new clusters. This allows us to inject a mock
	// creation function in unit tests of this module to avoid spinning up clusters.
	createClusterFn func(*testing.T, map[dynamicconfig.Key]any, bool, []TestClusterOption) *FunctionalTestBase
}

// suiteScopedCluster owns one lazily created legacy suite cluster.
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
	testClusterRouter.suiteScoped.LoadOrStore(rootName, &suiteScopedCluster{})

	t.Cleanup(func() {
		suiteClusterAny, ok := testClusterRouter.suiteScoped.Load(rootName)
		if ok {
			suiteCluster := suiteClusterAny.(*suiteScopedCluster)
			if suiteCluster.cluster != nil {
				if err := suiteCluster.cluster.tearDownTestCluster(); err != nil {
					t.Logf("Failed to tear down suite-scoped cluster: %v", err)
				}
			}
		}
		testClusterRouter.suiteScoped.Delete(rootName)
	})
}

func (p *clusterRouter) get(t *testing.T, req clusterRequest) (tb *FunctionalTestBase) {
	defer func() {
		if tb != nil {
			tb.RegisterTest(t)
		}
	}()
	if req.needsDedicated() {
		return p.getDedicated(t, req)
	}
	if cluster := p.getSuiteScoped(t); cluster != nil {
		return cluster
	}
	return p.getShared(t, req.needWorkerService)
}

func (p *clusterRouter) getShared(t *testing.T, needWorkerService bool) *FunctionalTestBase {
	if needWorkerService {
		return p.sharedWithWorker.get(t, func() *FunctionalTestBase {
			return p.createClusterFn(t, nil, true, []TestClusterOption{withWorkerService(true)})
		})
	}
	return p.shared.get(t, func() *FunctionalTestBase {
		return p.createClusterFn(t, nil, true, nil)
	})
}

func (p *clusterRouter) hasSuiteScoped(t *testing.T) bool {
	rootName, _, _ := strings.Cut(t.Name(), "/")
	_, ok := p.suiteScoped.Load(rootName)
	return ok
}

func (p *clusterRouter) getSuiteScoped(t *testing.T) *FunctionalTestBase {
	rootName, _, _ := strings.Cut(t.Name(), "/")
	if _, ok := p.suiteScoped.Load(rootName); !ok {
		return nil
	}

	suiteClusterAny, _ := p.suiteScoped.LoadOrStore(rootName, &suiteScopedCluster{})
	suiteCluster := suiteClusterAny.(*suiteScopedCluster)
	suiteCluster.once.Do(func() {
		// TODO(stephan, #10580): remove this workaround once the proper cluster-pool fix lands.
		// Enable the worker service on suite-scoped clusters. The only current user (Versioning3) needs the system
		// worker for worker-deployment APIs.
		suiteCluster.cluster = p.createClusterFn(t, nil, true, []TestClusterOption{withWorkerService(true)})
	})
	suiteCluster.cluster.SetT(t)
	return suiteCluster.cluster
}

func (p *clusterRouter) getDedicated(t *testing.T, req clusterRequest) *FunctionalTestBase {
	if req.mustBeFresh() || req.needWorkerService {
		// Always create a new cluster in the following cases:
		// - custom configs are set, since they can't be shared across tests
		// - worker service is needed, since goroutines (system workers, matching partition managers) don't clean up between
		//   tests and would accumulate in a long-lived pooled cluster.
		p.dedicated.reserveSlot(t)
		cluster := p.createClusterFn(t, req.dynamicConfig, false, append(req.clusterOpts, withWorkerService(req.needWorkerService)))

		// Register cleanup to tear down the cluster when the test completes.
		t.Cleanup(func() {
			if err := cluster.tearDownTestCluster(); err != nil {
				t.Logf("Failed to tear down cluster: %v", err)
			}
		})

		return cluster
	}

	// If no custom config or worker service is needed, reuse an existing cluster.
	return p.dedicated.get(t, func() *FunctionalTestBase {
		return p.createClusterFn(t, nil, false, nil)
	})
}

func createCluster(t *testing.T, dynamicConfig map[dynamicconfig.Key]any, shared bool, clusterOpts []TestClusterOption) *FunctionalTestBase {
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
