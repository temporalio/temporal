package testcore

import (
	"sync"
	"testing"

	"go.temporal.io/server/common/dynamicconfig"
)

const maxPoolSize = 4 // Maximum number of clusters in the pool

var basePool = &sharedBasePool{}

type sharedBasePool struct {
	mu       sync.Mutex
	cond     *sync.Cond
	createMu sync.Mutex // Serializes cluster creation (SetupSuiteWithCluster is not thread-safe)
	bases    []*FunctionalTestBase
	inUse    map[*FunctionalTestBase]bool
}

// acquire returns an available FunctionalTestBase, creates a new one if pool isn't full,
// or waits for one to become available.
// If dynamicConfig is non-empty, a new dedicated base is created (not pooled).
// The caller must call release() when done (for pooled bases).
func (cp *sharedBasePool) acquire(t *testing.T, dynamicConfig map[dynamicconfig.Key]any) *FunctionalTestBase {
	// If dynamic config is provided, create a dedicated base (not pooled)
	if len(dynamicConfig) > 0 {
		return cp.createBase(t, dynamicConfig)
	}

	cp.mu.Lock()

	// Initialize on first call
	if cp.inUse == nil {
		cp.inUse = make(map[*FunctionalTestBase]bool)
		cp.cond = sync.NewCond(&cp.mu)
	}

	for {
		// Find an available base
		for _, base := range cp.bases {
			if !cp.inUse[base] {
				cp.inUse[base] = true
				base.SetT(t)
				cp.mu.Unlock()
				return base
			}
		}

		// If pool isn't full, create a new base
		if len(cp.bases) < maxPoolSize {
			cp.mu.Unlock()
			tbase := cp.createBase(t, nil)
			cp.mu.Lock()
			cp.bases = append(cp.bases, tbase)
			cp.inUse[tbase] = true
			cp.mu.Unlock()
			return tbase
		}

		// Pool is full and all bases are in use, wait for one to be released
		cp.cond.Wait()
	}
}

// release marks a FunctionalTestBase as available for reuse.
// Only call this for bases acquired without dynamic config.
func (cp *sharedBasePool) release(base *FunctionalTestBase) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Only release if it's in our pool
	if _, ok := cp.inUse[base]; ok {
		cp.inUse[base] = false
		cp.cond.Signal() // Wake one waiting goroutine
	}
}

// createBase creates a new FunctionalTestBase with its own cluster.
// Uses createMu to serialize cluster creation since SetupSuiteWithCluster is not thread-safe.
func (cp *sharedBasePool) createBase(t *testing.T, dynamicConfig map[dynamicconfig.Key]any) *FunctionalTestBase {
	cp.createMu.Lock()
	defer cp.createMu.Unlock()

	tbase := &FunctionalTestBase{}
	tbase.SetT(t)

	var opts []TestClusterOption
	if len(dynamicConfig) > 0 {
		opts = append(opts, WithDynamicConfigOverrides(dynamicConfig))
	}

	tbase.SetupSuiteWithCluster(opts...)
	return tbase
}
