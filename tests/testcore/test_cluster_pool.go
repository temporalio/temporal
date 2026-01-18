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
	sharedSize := 4
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

	testClusterPool = &clusterPool{
		shared:    newPool(sharedSize, false),
		dedicated: newPool(dedicatedSize, true),
	}
}

// pool manages a fixed number of test clusters with lazy initialization.
type pool struct {
	clusters []*FunctionalTestBase
	inits    []sync.Once
	counter  atomic.Int64 // for round-robin (when slots is nil)
	slots    chan int     // for exclusive access (nil means shared/concurrent access)
}

func newPool(size int, exclusive bool) *pool {
	p := &pool{
		clusters: make([]*FunctionalTestBase, size),
		inits:    make([]sync.Once, size),
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
// For exclusive pools, blocks until a slot is slots and registers cleanup.
// For shared pools, uses round-robin.
func (p *pool) get(t *testing.T, createCluster func() *FunctionalTestBase) *FunctionalTestBase {
	var idx int
	if p.slots != nil {
		idx = <-p.slots
		t.Cleanup(func() { p.slots <- idx })
	} else {
		idx = int(p.counter.Add(1)-1) % len(p.clusters)
	}

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

func (p *clusterPool) get(t *testing.T, dedicated bool, dynamicConfig map[dynamicconfig.Key]any) *FunctionalTestBase {
	if dedicated || len(dynamicConfig) > 0 {
		return p.getDedicated(t, dynamicConfig)
	}
	return p.getShared(t)
}

func (p *clusterPool) getShared(t *testing.T) *FunctionalTestBase {
	return p.shared.get(t, func() *FunctionalTestBase {
		cluster := p.createCluster(t, nil)
		cluster.isShared = true
		return cluster
	})
}

func (p *clusterPool) getDedicated(t *testing.T, dynamicConfig map[dynamicconfig.Key]any) *FunctionalTestBase {
	if len(dynamicConfig) > 0 {
		// Custom dynamic config requires a fresh cluster (can't reuse)
		p.dedicated.acquireSlot(t)
		return p.createCluster(t, dynamicConfig)
	}
	return p.dedicated.get(t, func() *FunctionalTestBase {
		return p.createCluster(t, nil)
	})
}

func (p *clusterPool) createCluster(t *testing.T, dynamicConfig map[dynamicconfig.Key]any) *FunctionalTestBase {
	tbase := &FunctionalTestBase{}
	tbase.SetT(t)

	var opts []TestClusterOption
	if len(dynamicConfig) > 0 {
		opts = append(opts, WithDynamicConfigOverrides(dynamicConfig))
	}

	tbase.setupCluster(opts...)
	return tbase
}
