// Package leakcheck hosts a resource-leak regression test for the functional
// test infrastructure. It guards against the class of bug that drove the
// functional-test OOMs: per-cluster resources (goroutines, gRPC connections,
// and — the subtler one — heap reachable from retained references) that are not
// released when a test cluster shuts down. The functional suite builds hundreds
// of clusters in one process, so any per-cluster leak accumulates until OOM.
//
// Three complementary leak detectors run on the same build+teardown loop:
//
//  1. Goroutine-count slope — catches accumulation of already-seen stacks that
//     goleak.Find misses (those are captured in its IgnoreCurrent baseline).
//
//  2. Live-heap slope — catches retained heap references that have no live
//     goroutine (invisible to all goroutine-based tools).
//
//  3. Weak-pointer object probes ([testcore.ClusterLeakRefs]) — the most
//     precise detector: weak.Pointer to each cluster's key objects (TestCluster,
//     TestBase, TemporalImpl) captured before teardown. After GC, Value() == nil
//     means collected (no leak); != nil names the exact retained type without
//     needing external tools.
//
//  4. goleak.Find — catches goroutine stacks that survive a clean shutdown.
//
// It does not run by default: it inspects all goroutines (so it must run in
// isolation) and spins up real clusters. Set RUN_LEAK_TEST=1 to enable it (a
// dedicated CI job does). It is intentionally NOT build-tagged, so it stays
// compiled — and protected from rot when testcore changes — in normal builds;
// the Makefile excludes tests/leakcheck from the unit and functional suites.
//
// Run (sqlite => no Docker required):
//
//	RUN_LEAK_TEST=1 go test -run TestClusterShutdownLeak -count=1 -v \
//	    ./tests/leakcheck/ -args -persistenceType=sql -persistenceDriver=sqlite
//
// Tunable via env (defaults set in the Makefile leak-test target):
//
//	LEAK_ITERS                        clusters built after warmup (default 15)
//	LEAK_WARMUP_CLUSTERS              warmup clusters before baselining (default 20)
//	LEAK_MAX_GOROUTINES_PER_CLUSTER   goroutine-slope failure threshold (default 2)
//	LEAK_MAX_HEAP_KB_PER_CLUSTER      live-heap-slope failure threshold, KB (default 0)
//	LEAK_OUTPUT_DIR                   on failure, write heap.prof / goroutines.txt /
//	                                  leaked-goroutines.txt here (CI uploads them)
package leakcheck

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/goleak"
)

func TestClusterShutdownLeak(t *testing.T) {
	if os.Getenv("RUN_LEAK_TEST") != "1" {
		t.Skip("set RUN_LEAK_TEST=1 to run the resource-leak regression test (inspects all goroutines; must run in isolation)")
	}
	iters := envInt("LEAK_ITERS", 15)
	maxGoroutinesPerCluster := envInt("LEAK_MAX_GOROUTINES_PER_CLUSTER", 2)
	maxHeapKBPerCluster := envInt("LEAK_MAX_HEAP_KB_PER_CLUSTER", 0)
	warmupClusters := envInt("LEAK_WARMUP_CLUSTERS", 20)

	// Warm up so process-lifetime singletons and first-use initialization (proto
	// registries, type caches, ...) exist before we snapshot the baselines —
	// those are one-time costs, not per-cluster leaks.
	warmupRefs := make([]testcore.ClusterLeakRefs, warmupClusters)
	for i := range warmupClusters {
		buildRunTeardownCluster(t, fmt.Sprintf("warmup-%02d", i), &warmupRefs[i])
	}
	goroutineBaseline := goleak.IgnoreCurrent()
	testcore.SettleLeakCheck()
	baseGoroutines := stableGoroutines()
	baseHeap := stableHeapAlloc()

	// Build and tear down more clusters. A clean shutdown leaves neither extra
	// goroutines nor extra reachable heap after teardown transients settle.
	goroutines := make([]int, iters)
	heap := make([]uint64, iters)
	refs := make([]testcore.ClusterLeakRefs, iters)
	for i := range iters {
		buildRunTeardownCluster(t, fmt.Sprintf("c%02d", i), &refs[i])
		goroutines[i] = stableGoroutines()
		testcore.SettleLeakCheck()
		heap[i] = stableHeapAlloc()
		t.Logf("cluster %2d: goroutines=%d heapAlloc=%d MB", i, goroutines[i], heap[i]>>20)
	}
	t.Run("settle-stack", func(t *testing.T) {})

	goroutinePerCluster := intSlopePerCluster(baseGoroutines, goroutines)
	heapKBPerCluster := int(uint64SlopePerCluster(baseHeap, heap) >> 10)
	t.Logf("PER-CLUSTER GROWTH: goroutines=%d (gate %d), heap=%d KB (gate %d KB)",
		goroutinePerCluster, maxGoroutinesPerCluster, heapKBPerCluster, maxHeapKBPerCluster)

	var failures []string
	// Goroutine-count slope catches accumulation of *already-seen* stacks, which
	// goleak.Find below cannot (its baseline ignores every stack alive at warmup).
	if goroutinePerCluster > maxGoroutinesPerCluster {
		failures = append(failures, fmt.Sprintf("goroutine growth %d/cluster exceeds %d", goroutinePerCluster, maxGoroutinesPerCluster))
	}
	// Settled live-heap growth catches retained references (objects still
	// reachable after shutdown) — leaks that have no live goroutine and so are
	// invisible to goleak.
	if heapKBPerCluster > maxHeapKBPerCluster {
		failures = append(failures, fmt.Sprintf("heap growth %d KB/cluster exceeds %d KB", heapKBPerCluster, maxHeapKBPerCluster))
	}
	// goleak.Find catches *new* goroutine stacks that survived a clean shutdown.
	if err := goleak.Find(goroutineBaseline); err != nil {
		failures = append(failures, fmt.Sprintf("leaked goroutines: %v", err))
	}
	// Weak-pointer probes: names the exact retained type on failure.
	failures = append(failures, testcore.CheckLeakRefs(append(warmupRefs, refs...))...)

	if len(failures) > 0 {
		writeDiagnostics(t, goroutineBaseline)
		for _, f := range failures {
			t.Error(f)
		}
	}
}

// buildRunTeardownCluster creates a freshly-built, dedicated worker-service
// cluster, captures its leak refs into refs, runs a trivial workflow, then
// tears the cluster down. Running in a subtest means teardown completes before
// this returns.
func buildRunTeardownCluster(t *testing.T, label string, refs *testcore.ClusterLeakRefs) {
	t.Run("cluster", func(t *testing.T) {
		env := testcore.NewEnv(
			t,
			testcore.WithWorkerService("leak regression test"),
			testcore.WithLogger(log.NewNoopLogger()),
		)
		*refs = env.LeakRefs(label)
		env.SdkWorker().RegisterWorkflow(smokeWorkflow)
		run, err := env.SdkClient().ExecuteWorkflow(
			env.Context(),
			sdkclient.StartWorkflowOptions{TaskQueue: env.WorkerTaskQueue()},
			smokeWorkflow,
		)
		require.NoError(t, err)
		require.NoError(t, run.Get(env.Context(), nil))
		run = nil
		env = nil
	})
}

func smokeWorkflow(workflow.Context) error { return nil }

// stableGoroutines forces GC and waits for the goroutine count to stop dropping.
func stableGoroutines() int {
	prev := -1
	for range 20 {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n == prev {
			return n
		}
		prev = n
		time.Sleep(50 * time.Millisecond)
	}
	return prev
}

func stableHeapAlloc() uint64 {
	minHeap := ^uint64(0)
	for range 10 {
		runtime.GC()
		debug.FreeOSMemory()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.HeapAlloc < minHeap {
			minHeap = m.HeapAlloc
		}
		time.Sleep(20 * time.Millisecond)
	}
	return minHeap
}

func intSlopePerCluster(base int, series []int) int {
	if len(series) < 2 {
		return 0
	}
	return (series[len(series)-1] - base) / len(series)
}

func uint64SlopePerCluster(base uint64, series []uint64) uint64 {
	if len(series) < 2 {
		return 0
	}

	n := len(series) + 1
	meanX := float64(n-1) / 2
	var meanY float64
	for i := 0; i < n; i++ {
		meanY += float64(heapSample(base, series, i))
	}
	meanY /= float64(n)

	var numerator, denominator float64
	for i := 0; i < n; i++ {
		x := float64(i)
		y := float64(heapSample(base, series, i))
		numerator += (x - meanX) * (y - meanY)
		denominator += (x - meanX) * (x - meanX)
	}
	if numerator <= 0 || denominator == 0 {
		return 0
	}
	return uint64(numerator / denominator)
}

func heapSample(base uint64, series []uint64, i int) uint64 {
	if i == 0 {
		return base
	}
	return series[i-1]
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func writeDiagnostics(t *testing.T, baseline goleak.Option) {
	dir := os.Getenv("LEAK_OUTPUT_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Logf("leak diagnostics: mkdir %s: %v", dir, err)
		return
	}
	runtime.GC()
	if f, err := os.Create(filepath.Join(dir, "heap.prof")); err == nil {
		_ = pprof.WriteHeapProfile(f)
		_ = f.Close()
	}
	if f, err := os.Create(filepath.Join(dir, "goroutines.txt")); err == nil {
		_ = pprof.Lookup("goroutine").WriteTo(f, 2)
		_ = f.Close()
	}
	var buf bytes.Buffer
	if err := goleak.Find(baseline); err != nil {
		buf.WriteString(err.Error())
	}
	_ = os.WriteFile(filepath.Join(dir, "leaked-goroutines.txt"), buf.Bytes(), 0o644)
	t.Logf("leak diagnostics written to %s", dir)
}
