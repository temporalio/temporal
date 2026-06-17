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
//  2. HeapInuse slope — catches retained heap references that have no live
//     goroutine (invisible to all goroutine-based tools).
//
//  3. Weak-pointer object probes — the most precise detector: a weak.Pointer
//     to each cluster's key objects (TestCluster, TestBase, TemporalImpl) is
//     captured before teardown. After GC, Value() == nil means the object was
//     collected (no leak); != nil means something still retains it. Unlike
//     heap slopes this names the specific retained type immediately, without
//     needing external tools (goref, viewcore) that are unreliable on Go 1.26.
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
//	LEAK_MAX_GOROUTINES_PER_CLUSTER   goroutine-slope failure threshold (default 2)
//	LEAK_MAX_HEAP_KB_PER_CLUSTER      HeapInuse-slope failure threshold, KB (default 2048)
//	LEAK_OUTPUT_DIR                   on failure, write heap.prof / goroutines.txt /
//	                                  leaked-goroutines.txt here (CI uploads them)
package leakcheck

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"
	"weak"

	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/goleak"
)

const warmupClusters = 3

// clusterRefs holds weak pointers to the key objects created for one cluster.
// All are captured before teardown; Value() != nil after GC means a leak.
type clusterRefs struct {
	cluster  weak.Pointer[testcore.TestCluster]
	testBase weak.Pointer[persistencetests.TestBase]
	host     weak.Pointer[testcore.TemporalImpl]
}

func TestClusterShutdownLeak(t *testing.T) {
	if os.Getenv("RUN_LEAK_TEST") != "1" {
		t.Skip("set RUN_LEAK_TEST=1 to run the resource-leak regression test (inspects all goroutines; must run in isolation)")
	}
	iters := envInt("LEAK_ITERS", 15)
	maxGoroutinesPerCluster := envInt("LEAK_MAX_GOROUTINES_PER_CLUSTER", 2)
	maxHeapKBPerCluster := envInt("LEAK_MAX_HEAP_KB_PER_CLUSTER", 2048)

	// Warm up so process-lifetime singletons and first-use initialization (proto
	// registries, type caches, ...) exist before we snapshot the baselines —
	// those are one-time costs, not per-cluster leaks.
	warmupRefs := make([]clusterRefs, warmupClusters)
	for i := range warmupClusters {
		buildRunTeardownCluster(t, &warmupRefs[i])
	}
	goroutineBaseline := goleak.IgnoreCurrent()
	baseGoroutines := stableGoroutines()
	baseHeap := heapInUse()

	// Build and tear down more clusters, measuring after each settles. A clean
	// shutdown leaves neither extra goroutines nor extra reachable heap.
	goroutines := make([]int, iters)
	heap := make([]uint64, iters)
	refs := make([]clusterRefs, iters)
	for i := range iters {
		buildRunTeardownCluster(t, &refs[i])
		goroutines[i] = stableGoroutines()
		heap[i] = heapInUse()
		t.Logf("cluster %2d: goroutines=%d heapInUse=%d MB", i, goroutines[i], heap[i]>>20)
	}

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
	// Heap-inuse slope catches retained references (objects still reachable after
	// shutdown) — leaks that have no live goroutine and so are invisible to goleak.
	if heapKBPerCluster > maxHeapKBPerCluster {
		failures = append(failures, fmt.Sprintf("heap growth %d KB/cluster exceeds %d KB", heapKBPerCluster, maxHeapKBPerCluster))
	}
	// goleak.Find catches *new* goroutine stacks that survived a clean shutdown.
	// No IgnoreTopFunction here: a surviving database/sql connectionOpener (or any
	// other stack) is a real leak we want to catch.
	if err := goleak.Find(goroutineBaseline); err != nil {
		failures = append(failures, fmt.Sprintf("leaked goroutines: %v", err))
	}
	// Weak-pointer probes: after extra GC cycles, any cluster object still
	// reachable is definitively retained by a lingering reference. TestCluster
	// and TestBase must always reach 0; TemporalImpl allows 2 for GC timing
	// (the most recently torn-down cluster may not have been collected yet).
	// These probes name the specific retained type on failure, which the slope
	// metrics and pprof profiles cannot.
	if retainedMsgs := checkWeakRefs(append(warmupRefs, refs...)); len(retainedMsgs) > 0 {
		for _, msg := range retainedMsgs {
			failures = append(failures, msg)
		}
	}

	if len(failures) > 0 {
		writeDiagnostics(t, goroutineBaseline)
		for _, f := range failures {
			t.Error(f)
		}
	}
}

// buildRunTeardownCluster creates a freshly-built, dedicated worker-service
// cluster via the public testEnv API, captures weak pointers to its key
// objects into refs (before teardown runs), runs a trivial workflow (exercising
// frontend -> history -> matching -> SDK worker), then tears the cluster down.
// Running it in a subtest means the env's cleanups run before this returns, so
// the caller observes a post-teardown state. The workflow doubles as a liveness
// check: if the cluster weren't functional it would fail here.
func buildRunTeardownCluster(t *testing.T, refs *clusterRefs) {
	t.Run("cluster", func(t *testing.T) {
		env := testcore.NewEnv(t, testcore.WithWorkerService("leak regression test"))
		tc := env.GetTestCluster()
		refs.cluster = weak.Make(tc)
		refs.testBase = weak.Make(tc.TestBase())
		refs.host = weak.Make(tc.Host())
		env.SdkWorker().RegisterWorkflow(smokeWorkflow)
		run, err := env.SdkClient().ExecuteWorkflow(
			env.Context(),
			sdkclient.StartWorkflowOptions{TaskQueue: env.WorkerTaskQueue()},
			smokeWorkflow,
		)
		require.NoError(t, err)
		require.NoError(t, run.Get(env.Context(), nil))
	})
}

func smokeWorkflow(workflow.Context) error { return nil }

// checkWeakRefs runs GC and checks which cluster objects survived. Returns
// failure messages for any that exceed their allowed retention count.
// Allowed: TestCluster=0 (always freed), TestBase=0, TemporalImpl≤2 (GC
// timing: the most recently torn-down cluster may not be collected yet).
func checkWeakRefs(all []clusterRefs) []string {
	for range 5 {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	var clusters, bases, hosts []int
	for i, r := range all {
		if r.cluster.Value() != nil {
			clusters = append(clusters, i)
		}
		if r.testBase.Value() != nil {
			bases = append(bases, i)
		}
		if r.host.Value() != nil {
			hosts = append(hosts, i)
		}
	}
	var msgs []string
	if len(clusters) > 0 {
		msgs = append(msgs, fmt.Sprintf("TestCluster retained after teardown: clusters %v", clusters))
	}
	if len(bases) > 0 {
		msgs = append(msgs, fmt.Sprintf("TestBase retained after teardown: clusters %v", bases))
	}
	if len(hosts) > 2 {
		msgs = append(msgs, fmt.Sprintf("TemporalImpl retained after teardown: %d clusters %v (allowed ≤2 for GC timing)", len(hosts), hosts))
	}
	return msgs
}

// stableGoroutines forces GC and waits for the goroutine count to stop dropping,
// so post-teardown draining isn't mistaken for a leak.
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

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

// intSlopePerCluster returns the average growth per cluster from the baseline to
// the last sample, ignoring the first sample to discount one-time post-baseline
// allocation.
func intSlopePerCluster(base int, series []int) int {
	if len(series) < 2 {
		return 0
	}
	return (series[len(series)-1] - base) / len(series)
}

func uint64SlopePerCluster(base uint64, series []uint64) uint64 {
	if len(series) < 2 || series[len(series)-1] < base {
		return 0
	}
	return (series[len(series)-1] - base) / uint64(len(series))
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// writeDiagnostics dumps a heap profile, a full goroutine dump, and the leaked
// goroutine stacks to LEAK_OUTPUT_DIR so CI can upload them for offline analysis.
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
