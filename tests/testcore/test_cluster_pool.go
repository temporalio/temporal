package testcore

import (
	"os"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
)

const suiteDedicatedWorkerClusterMinEnvCount = 8

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
		suites:    &suiteRegistry{},
	}
}

// pool manages a fixed number of test cluster slots.
type pool struct {
	slots     []*clusterSlot
	next      int
	mu        sync.Mutex
	available chan *clusterSlot
}

func newPool(size int, exclusive bool, maxUsage int) *pool {
	p := &pool{
		slots: make([]*clusterSlot, size),
	}
	for i := range size {
		p.slots[i] = &clusterSlot{idx: i, maxUsage: maxUsage}
	}
	if exclusive {
		p.available = make(chan *clusterSlot, size)
		for _, slot := range p.slots {
			p.available <- slot
		}
	}
	return p
}

// get returns a cluster from the pool, creating it lazily if needed. Exclusive
// pools block until a slot is available. Shared pools choose slots round-robin.
// Both pool types may recreate clusters after maxUsage tests once active users release them.
func (p *pool) get(t *testing.T, createCluster func() *FunctionalTestBase) *FunctionalTestBase {
	var slot *clusterSlot
	if p.available != nil {
		slot = <-p.available
	} else {
		slot = p.nextSlot()
	}

	cluster := slot.acquire(t, createCluster)
	t.Cleanup(func() {
		slot.release(t)
		if p.available != nil {
			p.available <- slot
		}
	})

	return cluster
}

func (p *pool) nextSlot() *clusterSlot {
	p.mu.Lock()
	defer p.mu.Unlock()
	slot := p.slots[p.next%len(p.slots)]
	p.next++
	return slot
}

// acquireSlot gets exclusive access to a slot without using a pooled cluster.
// Used when a fresh cluster is needed (e.g., custom dynamic config).
func (p *pool) acquireSlot(t *testing.T) {
	if p.available == nil {
		return
	}
	slot := <-p.available
	t.Cleanup(func() { p.available <- slot })
}

type clusterSlot struct {
	idx     int
	mu      sync.Mutex
	cluster *FunctionalTestBase
	usage   int
	active  int
	// maxUsage is the max tests per cluster before recreate (0 = unlimited).
	maxUsage int
}

func (s *clusterSlot) acquire(t *testing.T, createCluster func() *FunctionalTestBase) *FunctionalTestBase {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cluster != nil && s.cluster.Poisoned() {
		s.cluster = createCluster()
		s.usage = 0
	}
	if s.maxUsage > 0 && s.cluster != nil && s.usage >= s.maxUsage && s.active == 0 {
		if err := s.cluster.tearDownTestCluster(); err != nil {
			t.Logf("Failed to tear down cluster %d: %v", s.idx, err)
		}
		s.cluster = nil
		s.usage = 0
	}
	if s.cluster == nil {
		s.cluster = createCluster()
	}

	s.usage++
	s.active++
	s.cluster.SetT(t)
	return s.cluster
}

func (s *clusterSlot) release(t *testing.T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.active--
	if s.active > 0 || s.maxUsage == 0 || s.usage < s.maxUsage || s.cluster == nil {
		return
	}

	s.tearDownLocked(t)
}

func (s *clusterSlot) tearDown(t *testing.T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tearDownLocked(t)
}

func (s *clusterSlot) tearDownLocked(t *testing.T) {
	if s.cluster == nil {
		return
	}
	if err := s.cluster.tearDownTestCluster(); err != nil {
		if s.idx < 0 {
			t.Logf("Failed to tear down suite-scoped cluster: %v", err)
		} else {
			t.Logf("Failed to tear down cluster %d: %v", s.idx, err)
		}
	}
	s.cluster = nil
	s.usage = 0
}

type clusterPool struct {
	shared    *pool
	dedicated *pool
	suites    *suiteRegistry
}

func (p *clusterPool) get(t *testing.T, dedicated bool, dynamicConfig map[dynamicconfig.Key]any, clusterOpts []TestClusterOption) (tb *FunctionalTestBase) {
	defer func() {
		tb.RegisterTest(t)
	}()
	if !dedicated && len(dynamicConfig) == 0 {
		if cluster := p.getSuiteScoped(t, clusterOpts); cluster != nil {
			return cluster
		}
	}
	if dedicated || len(dynamicConfig) > 0 || len(clusterOpts) > 0 {
		return p.getDedicated(t, dynamicConfig, clusterOpts)
	}
	return p.getShared(t)
}

func (p *clusterPool) getShared(t *testing.T) *FunctionalTestBase {
	return p.shared.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, nil, true, nil)
	})
}

func (p *clusterPool) getSuiteScoped(t *testing.T, clusterOpts []TestClusterOption) *FunctionalTestBase {
	return p.suites.get(t, rootTestName(t), clusterOpts, func(clusterOpts []TestClusterOption) *FunctionalTestBase {
		return p.createCluster(t, nil, true, clusterOpts)
	})
}

func (p *clusterPool) getDedicated(t *testing.T, dynamicConfig map[dynamicconfig.Key]any, clusterOpts []TestClusterOption) *FunctionalTestBase {
	if len(dynamicConfig) > 0 || len(clusterOpts) > 0 {
		// Custom config or fx options require a fresh cluster (can't reuse).
		p.dedicated.acquireSlot(t)
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

func (p *clusterPool) canUseSuiteScopedCluster(t *testing.T, dedicated bool) bool {
	return !dedicated && p.suites.registered(rootTestName(t))
}

func (p *clusterPool) useSuiteScopedCluster(t *testing.T, reason string) {
	t.Helper()
	rootName := rootTestName(t)
	p.suites.register(t, rootName, reason)

	t.Cleanup(func() {
		p.suites.cleanup(t, rootName)
	})
}

func (p *clusterPool) recordEnvUsage(t *testing.T, workerServiceReason string, workerServiceDedicated bool) {
	t.Helper()
	rootName := rootTestName(t)
	p.suites.recordUsage(t, rootName, workerServiceReason, workerServiceDedicated)
}

type suiteClusterUsage struct {
	mu                     sync.Mutex
	envCount               int
	workerDedicatedCount   int
	workerDedicatedReasons []string
}

type suiteScopedClusterConfig struct {
	clusterOpts []TestClusterOption
	params      TestClusterParams
}

type suiteScopedCluster struct {
	slot   *clusterSlot
	config suiteScopedClusterConfig
}

type suiteState struct {
	reason   string
	mu       sync.Mutex
	clusters []*suiteScopedCluster
}

func newSuiteScopedClusterConfig(clusterOpts []TestClusterOption) suiteScopedClusterConfig {
	opts := []TestClusterOption{withWorkerService(false), WithSharedCluster()}
	opts = append(opts, clusterOpts...)

	return suiteScopedClusterConfig{
		clusterOpts: slices.Clone(clusterOpts),
		params:      ApplyTestClusterOptions(opts),
	}
}

func (c suiteScopedClusterConfig) matches(other suiteScopedClusterConfig) bool {
	return reflect.DeepEqual(c.params, other.params)
}

// UseSuiteScopedCluster makes NewEnv reuse suite-owned clusters for shareable
// cluster configs under `t`. The reason documents why the suite should pool
// those clusters instead of using the shared pool or one-off dedicated clusters.
func UseSuiteScopedCluster(t *testing.T, reason string) {
	t.Helper()
	testClusterPool.useSuiteScopedCluster(t, reason)
}

type suiteRegistry struct {
	suites sync.Map
	usage  sync.Map
}

func (s *suiteRegistry) register(t *testing.T, rootName string, reason string) {
	t.Helper()
	if t.Name() != rootName {
		t.Fatalf("UseSuiteScopedCluster must be called from a top-level test, got %q", t.Name())
	}
	if reason == "" {
		t.Fatalf("UseSuiteScopedCluster requires a reason")
	}
	stateAny, loaded := s.suites.LoadOrStore(rootName, &suiteState{reason: reason})
	if !loaded {
		return
	}
	state := stateAny.(*suiteState)
	if state.reason != reason {
		t.Fatalf("suite-scoped cluster reason for %q differs from the registered reason", rootName)
	}
}

func (s *suiteRegistry) get(
	t *testing.T,
	rootName string,
	clusterOpts []TestClusterOption,
	createCluster func([]TestClusterOption) *FunctionalTestBase,
) *FunctionalTestBase {
	stateAny, ok := s.suites.Load(rootName)
	if !ok {
		return nil
	}
	state := stateAny.(*suiteState)
	suiteCluster := state.getCluster(clusterOpts)
	cluster := suiteCluster.slot.acquire(t, func() *FunctionalTestBase {
		return createCluster(suiteCluster.config.clusterOpts)
	})
	t.Cleanup(func() {
		suiteCluster.slot.release(t)
	})
	return cluster
}

func (s *suiteRegistry) registered(rootName string) bool {
	_, ok := s.suites.Load(rootName)
	return ok
}

func (s *suiteRegistry) cleanup(t *testing.T, rootName string) {
	stateAny, ok := s.suites.Load(rootName)
	if ok {
		state := stateAny.(*suiteState)
		state.mu.Lock()
		clusters := slices.Clone(state.clusters)
		state.mu.Unlock()
		for _, suiteCluster := range clusters {
			suiteCluster.slot.tearDown(t)
		}
	}
	s.suites.Delete(rootName)
	s.usage.Delete(rootName)
}

func (s *suiteState) getCluster(clusterOpts []TestClusterOption) *suiteScopedCluster {
	config := newSuiteScopedClusterConfig(clusterOpts)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cluster := range s.clusters {
		if cluster.config.matches(config) {
			return cluster
		}
	}
	cluster := &suiteScopedCluster{
		slot:   &clusterSlot{idx: -1},
		config: config,
	}
	s.clusters = append(s.clusters, cluster)
	return cluster
}

func (s *suiteRegistry) recordUsage(t *testing.T, rootName string, workerServiceReason string, workerServiceDedicated bool) {
	t.Helper()
	usageAny, _ := s.usage.LoadOrStore(rootName, &suiteClusterUsage{})
	usage := usageAny.(*suiteClusterUsage)
	usage.mu.Lock()
	defer usage.mu.Unlock()

	usage.envCount++
	if workerServiceDedicated {
		usage.workerDedicatedCount++
		usage.workerDedicatedReasons = append(usage.workerDedicatedReasons, workerServiceReason)
	}

	// Check for too many dedicated worker clusters.
	if tooManyDedicatedWorkerClusters(usage.envCount, usage.workerDedicatedCount) {
		t.Fatalf(
			"suite %s created %d worker-service dedicated clusters across %d test envs (reasons: %s); if those clusters can be suite-owned, use testcore.UseSuiteScopedCluster(t, \"reason\")",
			rootName,
			usage.workerDedicatedCount,
			usage.envCount,
			strings.Join(usage.workerDedicatedReasons, "; "),
		)
	}
}

func tooManyDedicatedWorkerClusters(envCount int, workerDedicatedCount int) bool {
	return envCount >= suiteDedicatedWorkerClusterMinEnvCount &&
		workerDedicatedCount*2 > envCount
}

func rootTestName(t *testing.T) string {
	rootName, _, _ := strings.Cut(t.Name(), "/")
	return rootName
}
