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
	"go.uber.org/multierr"
)

var (
	testClusterRouter   *clusterRouter
	defaultRouterConfig clusterRouterConfig
)

type clusterRouterConfig struct {
	sharedSize    int
	dedicatedSize int
	maxLeases     int
	eventsFile    *os.File
}

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

	defaultRouterConfig = clusterRouterConfig{
		sharedSize:    sharedSize,
		dedicatedSize: dedicatedSize,
		maxLeases:     maxLeases,
		eventsFile:    eventsFile,
	}
	testClusterRouter = newClusterRouter(NewClusterFactory(), defaultRouterConfig)
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

func (p *clusterPool) close() error {
	var errs error
	for _, slot := range p.allSlots {
		slot.Lock()
		errs = multierr.Append(errs, slot.tearDownLocked())
		slot.Unlock()
	}
	return errs
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
			if err := s.tearDownLocked(); err != nil {
				t.Logf("Failed to tear down cluster %d: %v", s.idx, err)
			}
		}
		s.cluster = createCluster()
		s.leaseCount = 0
		cluster = s.cluster
	}

	// Recreate idle clusters after the lease limit is reached.
	if s.maxLeases > 0 && s.leaseCount >= s.maxLeases && s.activeLeases == 0 {
		if err := s.tearDownLocked(); err != nil {
			t.Logf("Failed to tear down cluster %d: %v", s.idx, err)
		}
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

func (s *clusterPoolSlot) tearDownLocked() error {
	if s.cluster == nil {
		return nil
	}
	err := s.cluster.tearDownTestCluster()
	s.cluster = nil
	s.leaseCount = 0
	return err
}

// clusterRouter routes tests to shared/dedicated [clusterPool] or [suiteScopedCluster]s.
type clusterRouter struct {
	shared      *clusterPool
	dedicated   *clusterPool
	suiteScoped sync.Map

	factory    ClusterFactory
	eventsFile *os.File
}

func newClusterRouter(factory ClusterFactory, config clusterRouterConfig) *clusterRouter {
	return &clusterRouter{
		shared:     newClusterPool(config.sharedSize, false, config.maxLeases),
		dedicated:  newClusterPool(config.dedicatedSize, true, config.maxLeases),
		factory:    factory,
		eventsFile: config.eventsFile,
	}
}

func (p *clusterRouter) close() error {
	errs := multierr.Append(p.shared.close(), p.dedicated.close())
	p.suiteScoped.Range(func(key, value any) bool {
		p.suiteScoped.Delete(key)
		errs = multierr.Append(errs, value.(*suiteScopedCluster).tearDown())
		return true
	})
	return errs
}

// suiteScopedCluster owns one lazily created suite cluster.
type suiteScopedCluster struct {
	mu      sync.Mutex
	cluster *FunctionalTestBase
}

func (s *suiteScopedCluster) get(t *testing.T, createCluster func() *FunctionalTestBase) *FunctionalTestBase {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cluster == nil {
		s.cluster = createCluster()
	}
	s.cluster.SetT(t)
	return s.cluster
}

func (s *suiteScopedCluster) tearDown() error {
	s.mu.Lock()
	cluster := s.cluster
	s.cluster = nil
	s.mu.Unlock()
	if cluster == nil {
		return nil
	}
	return cluster.tearDownTestCluster()
}

// UseSuiteScopedCluster makes NewEnv use one cluster for all tests below t.
// In an imported runner, the runner parent owns teardown after every parallel
// descendant has completed.
//
// Deprecated: this only exists for backwards-compatibility with legacy sequential
// suite execution.
func UseSuiteScopedCluster(t *testing.T) {
	t.Helper()
	router := routerFor(t)
	scopeName := logicalTestName(t)
	if runContextFor(t) == nil && scopeName != t.Name() {
		t.Fatalf("UseSuiteScopedCluster must be called from a top-level test, got %q", t.Name())
	}
	router.suiteScoped.LoadOrStore(scopeName, &suiteScopedCluster{})
	if runContextFor(t) != nil {
		return
	}

	t.Cleanup(func() {
		suiteClusterAny, ok := router.suiteScoped.Load(scopeName)
		if ok {
			suiteCluster := suiteClusterAny.(*suiteScopedCluster)
			if err := suiteCluster.tearDown(); err != nil {
				t.Logf("Failed to tear down suite-scoped cluster: %v", err)
			}
		}
		router.suiteScoped.Delete(scopeName)
	})
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
	return r.needWorkerService || len(r.dynamicConfig) > 0 || len(r.clusterOpts) > 0
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
func (r clusterRequest) recordCreation(t *testing.T, router *clusterRouter) {
	logicalName := logicalTestName(t)
	suite, _, _ := strings.Cut(logicalName, "/")
	line, err := json.Marshal(map[string]any{
		"suite":  suite,
		"test":   logicalName,
		"kind":   r.kind,
		"reason": r.reason(),
		"worker": r.needWorkerService,
	})
	if err != nil {
		return
	}

	if router.eventsFile == nil {
		log.Printf("CLUSTEREVENT %s", line)
		return
	}
	// O_APPEND makes each write land atomically at EOF and os.File serializes
	// concurrent writes, so lines from parallel tests don't interleave.
	_, _ = router.eventsFile.Write(append(line, '\n'))
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
	return p.getShared(t)
}

func (p *clusterRouter) getShared(t *testing.T) *FunctionalTestBase {
	return p.shared.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, clusterRequest{kind: clusterKindShared})
	})
}

func (p *clusterRouter) hasSuiteScoped(t *testing.T) bool {
	return p.suiteScopedFor(t) != nil
}

func (p *clusterRouter) suiteScopedFor(t *testing.T) *suiteScopedCluster {
	logicalName := logicalTestName(t)
	var match *suiteScopedCluster
	var matchLen int
	p.suiteScoped.Range(func(key, value any) bool {
		scopeName := key.(string)
		if logicalName != scopeName && !strings.HasPrefix(logicalName, scopeName+"/") {
			return true
		}
		if len(scopeName) > matchLen {
			match = value.(*suiteScopedCluster)
			matchLen = len(scopeName)
		}
		return true
	})
	return match
}

func (p *clusterRouter) getSuiteScoped(t *testing.T) *FunctionalTestBase {
	suiteCluster := p.suiteScopedFor(t)
	if suiteCluster == nil {
		return nil
	}

	return suiteCluster.get(t, func() *FunctionalTestBase {
		// TODO(stephan, #10580): remove this workaround once the proper cluster-pool fix lands.
		// Enable the worker service on suite-scoped clusters. The only current user (Versioning3) needs the system
		// worker for worker-deployment APIs.
		return p.createCluster(t, clusterRequest{kind: clusterKindSuiteScoped, needWorkerService: true})
	})
}

func (p *clusterRouter) getDedicated(t *testing.T, req clusterRequest) *FunctionalTestBase {
	req.kind = clusterKindDedicated
	if req.mustBeFresh() {
		// Custom config or fx options require a fresh cluster (can't reuse).
		p.dedicated.reserveSlot(t)
		cluster := p.createCluster(t, req)

		// Register cleanup to tear down the cluster when the test completes.
		t.Cleanup(func() {
			reportFreshDedicatedTearDown(t, cluster)
		})

		return cluster
	}

	// If no custom config is provided, reuse an existing cluster.
	return p.dedicated.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, req)
	})
}

func reportFreshDedicatedTearDown(t interface{ Errorf(string, ...any) }, cluster *FunctionalTestBase) {
	if err := cluster.tearDownTestCluster(); err != nil {
		t.Errorf("Failed to tear down fresh dedicated cluster: %v", err)
	}
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
	req.recordCreation(t, p)

	return tbase
}
