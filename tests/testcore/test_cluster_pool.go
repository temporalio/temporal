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

	var eventsFile *os.File
	if path := os.Getenv("TEMPORAL_TEST_CLUSTER_EVENTS_FILE"); path != "" {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("cluster events disabled: cannot open %q: %v", path, err)
		}
		eventsFile = f
	}

	testClusterRouter = &clusterRouter{
		shared:     newClusterPool(sharedSize, false, 0),
		dedicated:  newClusterPool(dedicatedSize, true, 0),
		oneOff:     newOneOffClusterPool(dedicatedSize),
		eventsFile: eventsFile,
	}
}

// clusterPool manages a fixed number of test [clusterPoolSlot]s.
type clusterPool struct {
	sync.Mutex
	allSlots       []*clusterPoolSlot
	availableSlots []*clusterPoolSlot // for exclusive access (nil means shared/concurrent access)
	availableCond  *sync.Cond
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
		p.availableCond = sync.NewCond(&p.Mutex)
		p.availableSlots = append(p.availableSlots, p.allSlots...)
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
	if p.availableSlots == nil {
		return p.nextSlot()
	}

	p.Lock()
	defer p.Unlock()
	for len(p.availableSlots) == 0 {
		p.availableCond.Wait()
	}
	idx := len(p.availableSlots) - 1
	slot := p.availableSlots[idx]
	p.availableSlots = p.availableSlots[:idx]

	t.Cleanup(func() { p.releaseSlot(slot) })
	return slot
}

func (p *clusterPool) releaseSlot(slot *clusterPoolSlot) {
	p.Lock()
	defer p.Unlock()
	p.availableSlots = append(p.availableSlots, slot)
	p.availableCond.Signal()
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

type oneOffClusterPool struct {
	leases chan struct{}
}

func newOneOffClusterPool(size int) *oneOffClusterPool {
	return &oneOffClusterPool{
		leases: make(chan struct{}, size),
	}
}

func (p *oneOffClusterPool) get(t *testing.T, createCluster func() *FunctionalTestBase) *FunctionalTestBase {
	p.leases <- struct{}{}
	var cluster *FunctionalTestBase
	t.Cleanup(func() {
		defer func() { <-p.leases }()
		if cluster == nil {
			return
		}
		if err := cluster.tearDownTestCluster(); err != nil {
			t.Logf("Failed to tear down cluster: %v", err)
		}
	})
	cluster = createCluster()
	return cluster
}

// clusterRouter routes tests to shared/dedicated [clusterPool] or [suiteScopedCluster]s.
type clusterRouter struct {
	shared      *clusterPool
	dedicated   *clusterPool
	oneOff      *oneOffClusterPool
	suiteScoped sync.Map

	eventsFile *os.File
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
	rootName := suiteRootName(t)
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

// Cluster kinds recorded in creation events.
const (
	clusterKindShared      = "shared"
	clusterKindDedicated   = "dedicated"
	clusterKindOneOff      = "one-off"
	clusterKindSuiteScoped = "suite-scoped"
)

// clusterRequest describes what a test needs from the cluster router.
type clusterRequest struct {
	kind                string // set by the router: shared, dedicated, or suite-scoped
	dedicated           bool
	dedicatedReason     string
	needWorkerService   bool
	globalDynamicConfig map[dynamicconfig.Key]any
	clusterOpts         []TestClusterOption
}

// requiresDedicatedCluster reports whether the request must be served by a dedicated
// cluster rather than the shared pool.
func (r clusterRequest) requiresDedicatedCluster() bool {
	return r.dedicated || r.needWorkerService || len(r.globalDynamicConfig) > 0 || len(r.clusterOpts) > 0
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
	case r.requiresDedicatedCluster():
		return "custom config"
	default:
		return "dedicated"
	}
}

// recordCreation appends one JSON Lines event per test-cluster creation so a CI
// run can be queried for which suite created how many clusters of each kind, and
// why. Events fall back to the test log when no events file is configured.
func (p *clusterRouter) recordCreation(t *testing.T, req clusterRequest) {
	line, err := json.Marshal(struct {
		Suite  string `json:"suite"`
		Test   string `json:"test"`
		Kind   string `json:"kind"`
		Reason string `json:"reason"`
		Worker bool   `json:"worker"`
	}{
		Suite:  suiteRootName(t),
		Test:   t.Name(),
		Kind:   req.kind,
		Reason: req.reason(),
		Worker: req.needWorkerService,
	})
	if err != nil {
		return
	}

	if p.eventsFile == nil {
		log.Printf("CLUSTEREVENT %s", line)
		return
	}
	// O_APPEND makes each write land atomically at EOF and os.File serializes
	// concurrent writes, so lines from parallel tests don't interleave.
	_, _ = p.eventsFile.Write(append(line, '\n'))
}

func (p *clusterRouter) get(t *testing.T, req clusterRequest) (tb *FunctionalTestBase) {
	defer func() {
		if tb != nil {
			tb.RegisterTest(t)
		}
	}()
	if req.requiresDedicatedCluster() {
		return p.getDedicated(t, req)
	}
	if cluster := p.getSuiteScoped(t); cluster != nil {
		return cluster
	}
	return p.getShared(t)
}

func (p *clusterRouter) getShared(t *testing.T) *FunctionalTestBase {
	return p.shared.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, clusterRequest{kind: clusterKindShared})
	})
}

func (p *clusterRouter) hasSuiteScoped(t *testing.T) bool {
	_, ok := p.suiteScoped.Load(suiteRootName(t))
	return ok
}

func (p *clusterRouter) getSuiteScoped(t *testing.T) *FunctionalTestBase {
	rootName := suiteRootName(t)
	if _, ok := p.suiteScoped.Load(rootName); !ok {
		return nil
	}

	suiteClusterAny, _ := p.suiteScoped.LoadOrStore(rootName, &suiteScopedCluster{})
	suiteCluster := suiteClusterAny.(*suiteScopedCluster)
	suiteCluster.once.Do(func() {
		// TODO(stephan, #10580): remove this workaround once the proper cluster-pool fix lands.
		// Enable the worker service on suite-scoped clusters. The only current user (Versioning3) needs the system
		// worker for worker-deployment APIs.
		suiteCluster.cluster = p.createCluster(t, clusterRequest{kind: clusterKindSuiteScoped, needWorkerService: true})
	})
	suiteCluster.cluster.SetT(t)
	return suiteCluster.cluster
}

func (p *clusterRouter) getDedicated(t *testing.T, req clusterRequest) *FunctionalTestBase {
	// TestEnv dedicated requests take exclusive ownership of a dedicated pool slot,
	// and reset per-test dynamic config before releasing the slot.
	req.kind = clusterKindDedicated

	var cluster *FunctionalTestBase
	if len(req.clusterOpts) > 0 {
		// These options are cluster-startup state and cannot be reset on a pooled cluster.
		req.kind = clusterKindOneOff
		cluster = p.oneOff.get(t, func() *FunctionalTestBase {
			return p.createCluster(t, req.withWorkerService())
		})
	} else {
		cluster = p.dedicated.get(t, func() *FunctionalTestBase {
			return p.createCluster(t, req.withWorkerService())
		})
	}
	p.applyDynamicConfig(t, cluster, req.globalDynamicConfig)
	return cluster
}

func (r clusterRequest) withWorkerService() clusterRequest {
	r.needWorkerService = true // always enable the worker service on dedicated clusters
	return r
}

func (p *clusterRouter) applyDynamicConfig(t *testing.T, cluster *FunctionalTestBase, overrides map[dynamicconfig.Key]any) {
	for key, value := range overrides {
		cluster.testCluster.host.overrideDynamicConfigForTest(t, key, value)
	}
}

func (p *clusterRouter) createCluster(t *testing.T, req clusterRequest) *FunctionalTestBase {
	tbase := &FunctionalTestBase{}
	tbase.SetT(t)

	// The worker service is off unless the request explicitly needs it.
	opts := []TestClusterOption{withWorkerService(req.needWorkerService)}
	if shared := req.kind != clusterKindDedicated; shared {
		opts = append(opts, WithSharedCluster())
		if len(req.globalDynamicConfig) > 0 {
			opts = append(opts, WithDynamicConfigOverrides(req.globalDynamicConfig))
		}
	}
	opts = append(opts, req.clusterOpts...)

	tbase.setupCluster(opts...)
	p.recordCreation(t, req)

	return tbase
}

func suiteRootName(t *testing.T) string {
	rootName, _, _ := strings.Cut(t.Name(), "/")
	return rootName
}
