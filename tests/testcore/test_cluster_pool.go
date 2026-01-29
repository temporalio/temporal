package testcore

import (
	"os"
	"runtime"
	"strconv"
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

	sharedPool := newPool(sharedSize, false)
	sharedPool.maxUsage = maxUsage

	dedicatedPool := newPool(dedicatedSize, true)
	dedicatedPool.maxUsage = maxUsage

	testClusterPool = &clusterPool{
		shared:    sharedPool,
		dedicated: dedicatedPool,
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
				if err := p.clusters[idx].testCluster.TearDownCluster(); err != nil {
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
	shared    *pool
	dedicated *pool
}

func (p *clusterPool) get(t *testing.T, opts envOptions) *FunctionalTestBase {
	if opts.dedicatedCluster {
		return p.getDedicated(t, opts)
	}
	return p.getShared(t)
}

func (p *clusterPool) getShared(t *testing.T) *FunctionalTestBase {
	return p.shared.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, envOptions{})
	})
}

func (p *clusterPool) getDedicated(t *testing.T, opts envOptions) *FunctionalTestBase {
	// Dedicated clusters always get a fresh cluster (no reuse).
	p.dedicated.acquireSlot(t)
	cluster := p.createCluster(t, opts)

	// Register cleanup to tear down the cluster when the test completes.
	t.Cleanup(func() {
		if err := cluster.testCluster.TearDownCluster(); err != nil {
			t.Logf("Failed to tear down cluster: %v", err)
		}
	})

	return cluster
}

func (p *clusterPool) createCluster(t *testing.T, opts envOptions) *FunctionalTestBase {
	tbase := &FunctionalTestBase{}
	tbase.SetT(t)

	var clusterOpts []TestClusterOption

	// Mark cluster shared, if applicable.
	if !opts.dedicatedCluster {
		clusterOpts = append(clusterOpts, WithSharedCluster())
	}

	// Apply dynamic config overrides, if any.
	dynamicConfig := make(map[dynamicconfig.Key]any, len(opts.dynamicConfigSettings))
	for _, override := range opts.dynamicConfigSettings {
		dynamicConfig[override.setting.Key()] = override.value
	}
	clusterOpts = append(clusterOpts, WithDynamicConfigOverrides(dynamicConfig))

	// Apply time source override, if any.
	if opts.fakeTimeSource != nil {
		clusterOpts = append(clusterOpts, withTimeSource(opts.fakeTimeSource))
	}

	tbase.setupCluster(clusterOpts...)

	return tbase
}
