package testcore

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
)

var testClusterPool *clusterPool

// Cluster statistics counters
var (
	// totalClustersCreated tracks the total number of test clusters created during this test run.
	totalClustersCreated atomic.Int64
	// sharedClusterAcquisitions tracks how many times a shared cluster was acquired (created or reused).
	sharedClusterAcquisitions atomic.Int64
	// dedicatedClusterAcquisitions tracks how many times a dedicated cluster was acquired (created or reused).
	dedicatedClusterAcquisitions atomic.Int64
)

// TotalClustersCreated returns the total number of test clusters created during this test run.
func TotalClustersCreated() int64 {
	return totalClustersCreated.Load()
}

// PrintClusterStats prints cluster creation statistics to stderr.
// Call this at the end of test runs (e.g., from TestMain) to see the total count.
func PrintClusterStats() {
	created := totalClustersCreated.Load()
	sharedReq := sharedClusterAcquisitions.Load()
	dedicatedReq := dedicatedClusterAcquisitions.Load()
	totalReq := sharedReq + dedicatedReq
	reused := totalReq - created

	fmt.Fprintf(os.Stderr, "\n=== Test Cluster Stats ===\n")
	fmt.Fprintf(os.Stderr, "Total cluster requests:     %d\n", totalReq)
	fmt.Fprintf(os.Stderr, "  - Shared requests:        %d\n", sharedReq)
	fmt.Fprintf(os.Stderr, "  - Dedicated requests:     %d\n", dedicatedReq)
	fmt.Fprintf(os.Stderr, "Clusters created:           %d\n", created)
	fmt.Fprintf(os.Stderr, "Clusters reused:            %d\n", reused)
	if totalReq > 0 {
		fmt.Fprintf(os.Stderr, "Reuse rate:                 %.1f%%\n", float64(reused)/float64(totalReq)*100)
	}
}

func init() {
	sharedSize := runtime.GOMAXPROCS(0)
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

func (p *clusterPool) get(t *testing.T, dedicated bool, dynamicConfig map[dynamicconfig.Key]any, clusterOpts []TestClusterOption) *FunctionalTestBase {
	if dedicated || len(dynamicConfig) > 0 || len(clusterOpts) > 0 {
		return p.getDedicated(t, dynamicConfig, clusterOpts)
	}
	return p.getShared(t)
}

func (p *clusterPool) getShared(t *testing.T) *FunctionalTestBase {
	sharedClusterAcquisitions.Add(1)
	return p.shared.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, nil, true, nil)
	})
}

func (p *clusterPool) getDedicated(t *testing.T, dynamicConfig map[dynamicconfig.Key]any, clusterOpts []TestClusterOption) *FunctionalTestBase {
	dedicatedClusterAcquisitions.Add(1)

	if len(dynamicConfig) > 0 || len(clusterOpts) > 0 {
		// Custom dynamic config or cluster options require a fresh cluster (can't reuse).
		p.dedicated.acquireSlot(t)
		cluster := p.createCluster(t, dynamicConfig, false, clusterOpts)

		// Register cleanup to tear down the cluster when the test completes.
		t.Cleanup(func() {
			if err := cluster.testCluster.TearDownCluster(); err != nil {
				t.Logf("Failed to tear down cluster: %v", err)
			}
		})

		return cluster
	}

	// If no custom dynamic config or cluster options are provided, reuse an existing cluster.
	return p.dedicated.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, nil, false, nil)
	})
}

func (p *clusterPool) createCluster(t *testing.T, dynamicConfig map[dynamicconfig.Key]any, shared bool, clusterOpts []TestClusterOption) *FunctionalTestBase {
	count := totalClustersCreated.Add(1)
	t.Logf("Creating test cluster #%d (shared=%v)", count, shared)

	tbase := &FunctionalTestBase{}
	tbase.SetT(t)

	var opts []TestClusterOption
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
