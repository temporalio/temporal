package sim_runtime_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	SIM "go.temporal.io/server/tools/gomad/runtime"
)

// TestConcurrentSimulations verifies that multiple simulations can run in the same
// process without interfering with each other, and that identical seeds produce
// identical results regardless of concurrent execution.
func TestConcurrentSimulations(t *testing.T) {
	const parallelism = 4
	const seeds = 2 // we run seeds [0, seeds) twice each

	var wg sync.WaitGroup
	results := make([]int64, parallelism)

	for i := range parallelism {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()

			seed := int64(slot % seeds)
			SIM.Start(SIM.Seed(seed))

			// perform deterministic work inside the simulation
			var sum int64
			for j := 0; j < 10; j++ {
				SIM.NewGoroutine(func() {
					sum += SIM.CurrentSimulator().Drng.Int63n(1000)
				}, false)
			}
			SIM.Join()
			results[slot] = sum
		}(i)
	}
	wg.Wait()

	// slots with the same seed must produce the same result
	for slot := 0; slot < seeds; slot++ {
		require.Equal(t, results[slot], results[slot+seeds],
			"seed %d produced different results across concurrent runs", slot)
	}
}

// TestRegistryLifecycle verifies that currentSim panics for an unregistered goroutine.
func TestRegistryLifecycle(t *testing.T) {
	done := make(chan struct{})

	go func() {
		defer close(done)
		require.Panics(t, func() {
			SIM.CurrentSimulator()
		})
	}()

	<-done
}
