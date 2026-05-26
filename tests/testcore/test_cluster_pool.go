package testcore

import (
	"encoding/json"
	"log"
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
	var maxLeases int
	if os.Getenv("CI") != "" {
		maxLeases = 50
	}

	var eventsFile *os.File
	if path := os.Getenv("TEMPORAL_TEST_CLUSTER_EVENTS_FILE"); path != "" {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("cluster events disabled: cannot open %q: %v", path, err)
		}
		eventsFile = f
	}

	testClusterRouter = &clusterRouter{
		shared:          newClusterPool(sharedSize, false, maxLeases),
		sharedWorker:    newClusterPool(sharedSize, false, maxLeases),
		dedicated:       newClusterPool(dedicatedSize, true, maxLeases),
		dedicatedWorker: newClusterPool(dedicatedSize, true, maxLeases),
		eventsFile:      eventsFile,
	}
}

func DefaultSuiteClusterPoolSize() int {
	return max(1, runtime.GOMAXPROCS(0)/2)
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

func (p *clusterPool) tearDown(t *testing.T) {
	for _, slot := range p.allSlots {
		slot.Lock()
		slot.tearDownLocked(t)
		slot.Unlock()
	}
}

// clusterRouter routes tests to shared/dedicated [clusterPool] or [suiteScopedCluster]s.
type clusterRouter struct {
	shared          *clusterPool
	sharedWorker    *clusterPool
	dedicated       *clusterPool
	dedicatedWorker *clusterPool
	suiteScoped     sync.Map

	eventsFile *os.File
}

type suiteScopedCluster struct {
	pools map[bool]*clusterPool
}

// UseSuiteScopedCluster makes NewEnv use one cluster for all tests under `t`.
// The cluster is created on first use and torn down when `t` completes.
//
// Deprecated: this only exists for backwards-compatibility with legacy sequential
// suite execution.
func UseSuiteScopedCluster(t *testing.T) {
	UseSuiteScopedClusters(t, 1)
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
		pools: map[bool]*clusterPool{
			false: newClusterPool(size, false, 0),
			true:  newClusterPool(size, false, 0),
		},
	}
	actual, loaded := testClusterRouter.suiteScoped.LoadOrStore(rootName, suiteCluster)
	if loaded {
		suiteCluster = actual.(*suiteScopedCluster)
	}

	t.Cleanup(func() {
		suiteCluster.tearDown(t)
		testClusterRouter.suiteScoped.Delete(rootName)
	})
}

func (s *suiteScopedCluster) tearDown(t *testing.T) {
	for _, pool := range s.pools {
		pool.tearDown(t)
	}
}

// Cluster kinds recorded in creation events.
const (
	clusterKindShared      = "shared"
	clusterKindDedicated   = "dedicated"
	clusterKindSuiteScoped = "suite-scoped"
)

// clusterRequest describes what a test needs from the cluster router.
type clusterRequest struct {
	kind              string // set by the router: shared, dedicated, or suite-scoped
	dedicated         bool
	dedicatedReason   string
	needWorkerService bool
	dynamicConfig     map[dynamicconfig.Key]any
	clusterOpts       []TestClusterOption
}

// mustBeFresh reports whether the request requires a brand-new cluster that
// cannot be reused.
func (r clusterRequest) mustBeFresh() bool {
	return len(r.dynamicConfig) > 0 || len(r.clusterOpts) > 0
}

// needsDedicated reports whether the request must be served by a dedicated
// cluster rather than the shared pool.
func (r clusterRequest) needsDedicated() bool {
	return r.dedicated || r.mustBeFresh()
}

// reason explains why the cluster was created, for analytics. It falls back to a
// generic reason when the caller did not provide one.
func (r clusterRequest) reason() string {
	switch r.kind {
	case clusterKindShared:
		return "shared pool"
	case clusterKindSuiteScoped:
		return "suite-scoped"
	}
	switch {
	case r.dedicatedReason != "":
		return r.dedicatedReason
	case r.mustBeFresh():
		return "custom config"
	default:
		return "dedicated (pooled)"
	}
}

// recordCreation appends one JSON Lines event per test-cluster creation so a CI
// run can be queried for which suite created how many clusters of each kind, and
// why. Events fall back to the test log when no events file is configured.
func (r clusterRequest) recordCreation(t *testing.T) {
	suite, _, _ := strings.Cut(t.Name(), "/")
	line, err := json.Marshal(map[string]any{
		"suite":  suite,
		"test":   t.Name(),
		"kind":   r.kind,
		"reason": r.reason(),
		"worker": r.needWorkerService,
	})
	if err != nil {
		return
	}

	if testClusterRouter.eventsFile == nil {
		log.Printf("CLUSTEREVENT %s", line)
		return
	}
	// O_APPEND makes each write land atomically at EOF and os.File serializes
	// concurrent writes, so lines from parallel tests don't interleave.
	_, _ = testClusterRouter.eventsFile.Write(append(line, '\n'))
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
	if cluster := p.getSuiteScoped(t, req.needWorkerService); cluster != nil {
		return cluster
	}
	return p.getShared(t, req.needWorkerService)
}

func (p *clusterRouter) getShared(t *testing.T, workerService bool) *FunctionalTestBase {
	pool := p.shared
	if workerService {
		pool = p.sharedWorker
	}
	return pool.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, clusterRequest{kind: clusterKindShared, needWorkerService: workerService})
	})
}

func (p *clusterRouter) hasSuiteScoped(t *testing.T) bool {
	rootName, _, _ := strings.Cut(t.Name(), "/")
	_, ok := p.suiteScoped.Load(rootName)
	return ok
}

func (p *clusterRouter) getSuiteScoped(t *testing.T, workerService bool) *FunctionalTestBase {
	rootName, _, _ := strings.Cut(t.Name(), "/")
	suiteClusterAny, ok := p.suiteScoped.Load(rootName)
	if !ok {
		return nil
	}
	suiteCluster := suiteClusterAny.(*suiteScopedCluster)
	return suiteCluster.pools[workerService].get(t, func() *FunctionalTestBase {
		return p.createCluster(t, clusterRequest{kind: clusterKindSuiteScoped, needWorkerService: workerService})
	})
}

func (p *clusterRouter) getDedicated(t *testing.T, req clusterRequest) *FunctionalTestBase {
	req.kind = clusterKindDedicated
	pool := p.dedicated
	if req.needWorkerService {
		pool = p.dedicatedWorker
	}
	if req.mustBeFresh() {
		// Custom config or fx options require a fresh cluster (can't reuse).
		pool.reserveSlot(t)
		cluster := p.createCluster(t, req)

		// Register cleanup to tear down the cluster when the test completes.
		t.Cleanup(func() {
			if err := cluster.tearDownTestCluster(); err != nil {
				t.Logf("Failed to tear down cluster: %v", err)
			}
		})

		return cluster
	}

	// If no custom config is provided, reuse an existing cluster.
	return pool.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, req)
	})
}

func (p *clusterRouter) acquireDedicatedSlot(t *testing.T, workerService bool) {
	pool := p.dedicated
	if workerService {
		pool = p.dedicatedWorker
	}
	pool.reserveSlot(t)
}

func (p *clusterRouter) createCluster(t *testing.T, req clusterRequest) *FunctionalTestBase {
	tbase := &FunctionalTestBase{}
	tbase.SetT(t)

	// The worker service is off unless the request explicitly needs it.
	opts := []TestClusterOption{withWorkerService(req.needWorkerService)}
	if req.kind != clusterKindDedicated {
		opts = append(opts, WithSharedCluster())
	}
	if len(req.dynamicConfig) > 0 {
		opts = append(opts, WithDynamicConfigOverrides(req.dynamicConfig))
	}
	opts = append(opts, req.clusterOpts...)

	tbase.setupCluster(opts...)
	req.recordCreation(t)

	return tbase
}
