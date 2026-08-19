package testcore

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
)

var testClusterRouter *clusterRouter

func init() {
	maxLive := positiveEnv("TEMPORAL_TEST_LIVE_CLUSTERS", 2*runtime.GOMAXPROCS(0))
	router := &clusterRouter{
		legacySlots: make(chan struct{}, runtime.GOMAXPROCS(0)),
	}
	router.perTest = newPerTestClusterProvider(
		maxLive,
		router.createClusterWithOwner,
		func(cluster *FunctionalTestBase) error { return cluster.tearDownTestCluster() },
	)
	testClusterRouter = router
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

// clusterRouter gives every NewEnv test a newly-created cluster. Legacy suites
// that manage their own cluster lifetime use a separate bounded slot.
type clusterRouter struct {
	perTest             *perTestClusterProvider
	legacySlots         chan struct{}
	suiteWorkerServices sync.Map
	events              *clusterEventRecorder
}

func (p *clusterRouter) reserveLegacyCluster(t *testing.T) {
	p.legacySlots <- struct{}{}
	t.Cleanup(func() { <-p.legacySlots })
}

// UseWorkerServiceForSuite enables the system worker on every test environment
// created below the top-level test t. Each leaf still receives its own cluster.
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

// clusterRequest describes the configuration of one test-owned cluster.
type clusterRequest struct {
	reason            string
	needWorkerService bool
	dynamicConfig     map[dynamicconfig.Key]any
	clusterOpts       []TestClusterOption
}

func (p *clusterRouter) get(t *testing.T, req clusterRequest) *FunctionalTestBase {
	rootName, _, _ := strings.Cut(t.Name(), "/")
	if reason, ok := p.suiteWorkerServices.Load(rootName); ok {
		req.needWorkerService = true
		if req.reason == "" {
			req.reason = "worker service required: " + reason.(string)
		}
	}
	if req.reason == "" {
		req.reason = "per-test"
	}

	cluster, err := p.perTest.clusterForTest(t, req)
	require.NoError(t, err)
	return cluster
}

func (p *clusterRouter) createClusterWithOwner(owner clusterTest, req clusterRequest) (*FunctionalTestBase, error) {
	tbase := &FunctionalTestBase{}
	createdAt := time.Now()
	phases := newBootPhaseDurations()

	// The worker service is off unless the request explicitly needs it.
	opts := []TestClusterOption{
		withWorkerService(req.needWorkerService),
		withBootPhaseObserver(phases.record),
	}
	if len(req.dynamicConfig) > 0 {
		opts = append(opts, WithDynamicConfigOverrides(req.dynamicConfig))
	}
	opts = append(opts, req.clusterOpts...)

	if err := tbase.setupClusterWithOwner(owner, opts...); err != nil {
		return nil, err
	}
	p.recordClusterCreation(owner.Name(), tbase, req, createdAt, phases.snapshot())
	return tbase, nil
}

func (p *clusterRouter) recordClusterCreation(
	testName string,
	tbase *FunctionalTestBase,
	req clusterRequest,
	createdAt time.Time,
	phases map[string]time.Duration,
) {
	if p.events == nil {
		return
	}
	clusterID := p.events.nextClusterID()
	tbase.clusterEventID = clusterID
	tbase.clusterEventRecorder = p.events
	suite, _, _ := strings.Cut(testName, "/")
	p.events.recordClusterCreated(clusterID, clusterCreationEvent{
		suite:      suite,
		test:       testName,
		reason:     req.reason,
		worker:     req.needWorkerService,
		duration:   time.Since(createdAt),
		namespaces: len(tbase.testClusterConfig.preseededNamespaces),
		phases:     phases,
	})
}
