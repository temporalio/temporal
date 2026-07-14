package testcore

import (
	"encoding/json"
	"log"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
)

var testClusterRouter *clusterRouter
var testClusterEventCounters clusterEventCounters

type clusterEventCounters struct {
	nextID  atomic.Int64
	created atomic.Int64
	live    atomic.Int64
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
		eventsFile: eventsFile,
	}
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

// clusterRouter routes tests to shared/dedicated [clusterPool] or [suiteScopedCluster]s.
type clusterRouter struct {
	shared      *clusterPool
	dedicated   *clusterPool
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

type clusterEvent struct {
	id       int64
	sequence int64
	suite    string
	test     string
	kind     string
	reason   string
	worker   bool
}

func newClusterEvent(t *testing.T, req clusterRequest) clusterEvent {
	suite, _, _ := strings.Cut(t.Name(), "/")
	sequence := testClusterEventCounters.created.Add(1)
	return clusterEvent{
		id:       testClusterEventCounters.nextID.Add(1),
		sequence: sequence,
		suite:    suite,
		test:     t.Name(),
		kind:     req.kind,
		reason:   req.reason(),
		worker:   req.needWorkerService,
	}
}

// record appends lifecycle events with process memory counters so CI artifacts
// can correlate cluster churn with the test binary's memory high-water marks.
func (e clusterEvent) record(event string) {
	if e.id == 0 {
		return
	}

	liveClusters := testClusterEventCounters.live.Load()
	switch event {
	case "created":
		liveClusters = testClusterEventCounters.live.Add(1)
	case "teardown-done":
		liveClusters = testClusterEventCounters.live.Add(-1)
	default:
	}

	fields := map[string]any{
		"event":            event,
		"timestamp":        time.Now().UTC().Format(time.RFC3339Nano),
		"cluster_id":       e.id,
		"cluster_sequence": e.sequence,
		"suite":            e.suite,
		"test":             e.test,
		"cluster_kind":     e.kind,
		"cluster_reason":   e.reason,
		"worker":           e.worker,
		"clusters_created": testClusterEventCounters.created.Load(),
		"live_clusters":    liveClusters,
	}
	if event == "created" {
		fields["kind"] = e.kind
		fields["reason"] = e.reason
	}
	addClusterMemoryFields(fields)

	writeClusterEvent(fields)
}

func addClusterMemoryFields(fields map[string]any) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	fields["goroutines"] = runtime.NumGoroutine()
	fields["heap_alloc_bytes"] = mem.HeapAlloc
	fields["heap_sys_bytes"] = mem.HeapSys
	fields["heap_released_bytes"] = mem.HeapReleased
	fields["stack_sys_bytes"] = mem.StackSys
	fields["sys_bytes"] = mem.Sys

	for name, value := range runtimeMetricValues(
		"/memory/classes/total:bytes",
		"/memory/classes/heap/objects:bytes",
		"/memory/classes/heap/free:bytes",
		"/memory/classes/heap/released:bytes",
		"/memory/classes/heap/stacks:bytes",
		"/memory/classes/os-stacks:bytes",
	) {
		fields["runtime_metric_"+name] = value
	}

	for name, value := range procSelfStatusValues("VmRSS", "VmHWM", "VmSize", "VmData", "RssAnon", "RssFile", "Threads") {
		fields["proc_"+name+"_kb"] = value
	}
}

func runtimeMetricValues(names ...string) map[string]uint64 {
	samples := make([]metrics.Sample, len(names))
	for i, name := range names {
		samples[i].Name = name
	}
	metrics.Read(samples)

	values := make(map[string]uint64, len(samples))
	for _, sample := range samples {
		if sample.Value.Kind() == metrics.KindUint64 {
			values[strings.NewReplacer("/", "_", ":", "_").Replace(strings.TrimPrefix(sample.Name, "/"))] = sample.Value.Uint64()
		}
	}
	return values
}

func procSelfStatusValues(keys ...string) map[string]uint64 {
	contents, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return nil
	}

	wanted := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		wanted[key] = struct{}{}
	}

	values := make(map[string]uint64, len(keys))
	for line := range strings.SplitSeq(string(contents), "\n") {
		key, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if _, ok := wanted[key]; !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseUint(fields[0], 10, 64)
		if err == nil {
			values[key] = value
		}
	}
	return values
}

func writeClusterEvent(fields map[string]any) {
	line, err := json.Marshal(fields)
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
		suiteCluster.cluster = p.createCluster(t, clusterRequest{kind: clusterKindSuiteScoped, needWorkerService: true})
	})
	suiteCluster.cluster.SetT(t)
	return suiteCluster.cluster
}

func (p *clusterRouter) getDedicated(t *testing.T, req clusterRequest) *FunctionalTestBase {
	req.kind = clusterKindDedicated
	if req.mustBeFresh() {
		// Custom config or fx options require a fresh cluster (can't reuse).
		p.dedicated.reserveSlot(t)
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
	return p.dedicated.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, req)
	})
}

func (p *clusterRouter) createCluster(t *testing.T, req clusterRequest) *FunctionalTestBase {
	tbase := &FunctionalTestBase{}
	tbase.SetT(t)
	tbase.clusterEvent = newClusterEvent(t, req)
	tbase.clusterEvent.record("build-start")

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
	tbase.clusterEvent.record("created")

	return tbase
}
