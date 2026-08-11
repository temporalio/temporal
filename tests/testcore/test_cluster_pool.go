package testcore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var testClusterRouter *clusterRouter

const (
	clusterModePooled  = "pooled"
	clusterModePerTest = "per-test"
)

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

	router := &clusterRouter{
		shared:    newClusterPool(sharedSize, false, maxLeases),
		dedicated: newClusterPool(dedicatedSize, true, maxLeases),
	}

	mode := configuredClusterMode(os.Getenv("TEMPORAL_TEST_CLUSTER_MODE"))
	switch mode {
	case clusterModePooled:
	case clusterModePerTest:
		maxLiveTests, warmSpares := configuredPerTestClusterLimits()
		router.perTest = newPerTestClusterProvider(
			maxLiveTests,
			warmSpares,
			router.createClusterWithOwner,
			router.createReadyClusterWithOwner,
			func(cluster *FunctionalTestBase) error { return cluster.tearDownTestCluster() },
		)
	default:
		panic("TEMPORAL_TEST_CLUSTER_MODE must be pooled or per-test")
	}
	testClusterRouter = router
}

func configuredClusterMode(mode string) string {
	if mode == "" {
		return clusterModePerTest
	}
	return mode
}

func configuredPerTestClusterLimits() (maxLiveTests int, warmSpares int) {
	maxLiveTests = positiveEnv("TEMPORAL_TEST_LIVE_CLUSTERS", 40)
	warmSpares = nonNegativeEnv("TEMPORAL_TEST_WARM_SPARES", 0)
	return maxLiveTests, warmSpares
}

func positiveEnv(name string, defaultValue int) int {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		panic(name + " must be a positive integer")
	}
	return n
}

func nonNegativeEnv(name string, defaultValue int) int {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		panic(name + " must be a non-negative integer")
	}
	return n
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

// clusterRouter routes tests to shared/dedicated [clusterPool]s or the per-test provider.
type clusterRouter struct {
	shared              *clusterPool
	dedicated           *clusterPool
	perTest             *perTestClusterProvider
	suiteWorkerServices sync.Map

	events *clusterEventRecorder
}

// UseWorkerServiceForSuite enables the system worker on every test environment
// created below the top-level test t. Each leaf still receives its own cluster
// when the provider is in per-test mode.
func UseWorkerServiceForSuite(t *testing.T, reason string) {
	t.Helper()
	rootName, _, _ := strings.Cut(t.Name(), "/")
	if t.Name() != rootName {
		t.Fatalf("UseWorkerServiceForSuite must be called from a top-level test, got %q", t.Name())
	}
	if reason == "" {
		t.Fatal("UseWorkerServiceForSuite requires a reason")
	}
	testClusterRouter.suiteWorkerServices.Store(rootName, reason)
	t.Cleanup(func() { testClusterRouter.suiteWorkerServices.Delete(rootName) })
}

// Cluster kinds recorded in creation events.
const (
	clusterKindShared    = "shared"
	clusterKindDedicated = "dedicated"
)

// clusterRequest describes what a test needs from the cluster router.
type clusterRequest struct {
	kind                  string // set by the router: shared or dedicated
	dedicated             bool
	dedicatedReason       string
	needWorkerService     bool
	dynamicConfig         map[dynamicconfig.Key]any
	requiresStartupConfig bool
	clusterOpts           []TestClusterOption
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
func (r clusterRequest) recordCreation(testName string) {
	suite, _, _ := strings.Cut(testName, "/")
	line, err := json.Marshal(map[string]any{
		"suite":  suite,
		"test":   testName,
		"kind":   r.kind,
		"reason": r.reason(),
		"worker": r.needWorkerService,
	})
	if err != nil {
		return
	}

	log.Printf("CLUSTEREVENT %s", line)
}

func (p *clusterRouter) get(t *testing.T, req clusterRequest) (tb *FunctionalTestBase) {
	rootName, _, _ := strings.Cut(t.Name(), "/")
	if reason, ok := p.suiteWorkerServices.Load(rootName); ok {
		req.needWorkerService = true
		if req.dedicatedReason == "" {
			req.dedicatedReason = "worker service required: " + reason.(string)
		}
	}
	acquireStart := time.Now()
	acquireSource := clusterAcquireSourcePooled
	defer func() {
		if tb != nil {
			tb.RegisterTest(t)
			if p.events != nil {
				p.events.recordClusterAcquire(
					tb.clusterEventID,
					t.Name(),
					time.Since(acquireStart),
					acquireSource,
				)
			}
		}
	}()
	if p.perTest != nil {
		tb, acquireSource = p.getPerTest(t, req)
		return tb
	}
	if req.needsDedicated() {
		return p.getDedicated(t, req)
	}
	return p.getShared(t)
}

func (p *clusterRouter) getPerTest(
	t *testing.T,
	req clusterRequest,
) (*FunctionalTestBase, clusterAcquireSource) {
	req.kind = clusterKindDedicated
	if req.dedicatedReason == "" {
		req.dedicatedReason = "per-test"
	}
	lease, err := p.perTest.acquire(t.Name(), req)
	require.NoError(t, err)
	cluster := lease.cluster
	cluster.SetT(t)
	t.Cleanup(func() {
		if err := lease.release(); err != nil {
			t.Logf("Failed to tear down per-test cluster: %v", err)
		}
	})
	return cluster, lease.acquireSource
}

func (p *clusterRouter) getShared(t *testing.T) *FunctionalTestBase {
	return p.shared.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, clusterRequest{kind: clusterKindShared})
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
	tbase, err := p.createClusterWithOwner(t, req)
	require.NoError(t, err)
	tbase.SetT(t)
	return tbase
}

func (p *clusterRouter) createClusterWithOwner(owner clusterTest, req clusterRequest) (*FunctionalTestBase, error) {
	return p.createClusterWithOwnerAndReadiness(owner, req, false)
}

func (p *clusterRouter) createReadyClusterWithOwner(owner clusterTest, req clusterRequest) (*FunctionalTestBase, error) {
	return p.createClusterWithOwnerAndReadiness(owner, req, true)
}

func (p *clusterRouter) createClusterWithOwnerAndReadiness(
	owner clusterTest,
	req clusterRequest,
	waitUntilReady bool,
) (*FunctionalTestBase, error) {
	tbase := &FunctionalTestBase{}
	createdAt := time.Now()
	phases := newBootPhaseDurations()

	// The worker service is off unless the request explicitly needs it.
	opts := []TestClusterOption{
		withWorkerService(req.needWorkerService),
		withBootPhaseObserver(phases.record),
	}
	if p.perTest != nil {
		opts = append(opts, withReusablePersistenceDatabase())
	}
	if req.kind != clusterKindDedicated {
		opts = append(opts, WithSharedCluster())
	}
	if len(req.dynamicConfig) > 0 {
		opts = append(opts, WithDynamicConfigOverrides(req.dynamicConfig))
	}
	opts = append(opts, req.clusterOpts...)

	if err := tbase.setupClusterWithOwner(owner, opts...); err != nil {
		return nil, err
	}
	if waitUntilReady {
		readyAt := time.Now()
		if err := waitForFrontendReady(tbase.FrontendGRPCAddress()); err != nil {
			return nil, multierr.Combine(err, tbase.tearDownTestCluster())
		}
		phases.record("frontend-ready", time.Since(readyAt))
	}
	p.recordClusterCreation(owner.Name(), tbase, req, createdAt, phases.snapshot())

	return tbase, nil
}

func waitForFrontendReady(address string) error {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create frontend health client: %w", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	frontendClient := workflowservice.NewWorkflowServiceClient(conn)
	err = backoff.ThrottleRetryContext(
		ctx,
		func(ctx context.Context) error {
			_, err := frontendClient.GetSystemInfo(ctx, &workflowservice.GetSystemInfoRequest{})
			return err
		},
		backoff.NewConstantDelayRetryPolicy(20*time.Millisecond),
		nil,
	)
	if err != nil {
		return fmt.Errorf("wait for frontend readiness: %w", err)
	}
	return nil
}

func (p *clusterRouter) recordClusterCreation(
	testName string,
	tbase *FunctionalTestBase,
	req clusterRequest,
	createdAt time.Time,
	phases map[string]time.Duration,
) {
	if p.events == nil {
		req.recordCreation(testName)
		return
	}
	clusterID := p.events.nextClusterID()
	tbase.clusterEventID = clusterID
	tbase.clusterEventRecorder = p.events
	suite, _, _ := strings.Cut(testName, "/")
	p.events.recordClusterCreated(clusterID, clusterCreationEvent{
		suite:      suite,
		test:       testName,
		kind:       req.kind,
		reason:     req.reason(),
		worker:     req.needWorkerService,
		duration:   time.Since(createdAt),
		namespaces: len(tbase.testClusterConfig.preseededNamespaces),
		phases:     phases,
	})
}
