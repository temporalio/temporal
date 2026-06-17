// Package goleak hosts a goroutine-leak regression test for the functional
// test infrastructure.
//
// The test repeatedly creates and tears down a full test cluster (via the public
// testEnv API, with the worker service enabled) and asserts that the live
// goroutine count does not grow per cluster. A non-zero slope is the signature
// of a shutdown leak (clients, connections, background loops, SDK workers not
// released) — the class of bug that drove the functional-test OOMs.
//
// It does not run by default: it measures the global goroutine count, so it must
// run in isolation, and it spins up real clusters. Set RUN_GOLEAK_TEST=1 to
// enable it (a dedicated CI job does this). It is intentionally NOT build-tagged,
// so it stays compiled — and thus protected from rot when testcore changes — in
// normal builds. The Makefile already excludes tests/goleak from the unit and
// functional suites.
//
// On failure it writes diagnostics (a goroutine dump, a heap profile, and the
// leaked-goroutine stacks per goleak) to GOLEAK_OUTPUT_DIR so CI can upload them
// as artifacts for diagnosis.
//
// Run (sqlite => no Docker required):
//
//	RUN_GOLEAK_TEST=1 go test -run TestClusterGoroutineSlope -count=1 -v \
//	    ./tests/goleak/ -args -persistenceType=sql -persistenceDriver=sqlite
//
// Env knobs (all optional; defaults chosen to be sensible for CI):
//
//	RUN_GOLEAK_TEST=1               enable the test (otherwise skipped)
//	GOLEAK_ITERS                       cluster create/teardown iterations (default 12)
//	GOLEAK_MAX_GOROUTINES_PER_CLUSTER  slope failure threshold (default 12)
//	GOLEAK_SETTLE_SECONDS              max settle wait per measurement (default 2)
//	GOLEAK_OUTPUT_DIR                  dir for failure diagnostics (default a temp dir)

package goleak

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"go.temporal.io/server/tests/testcore"
	"go.uber.org/goleak"
)

const warmupIters = 3

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// TestClusterGoroutineSlope is the regression gate: it asserts that goroutines do
// not accumulate per cluster create/teardown. It is robust to legitimate fixed
// background goroutines (they don't grow per iteration) and needs no ignore-list.
func TestClusterGoroutineSlope(t *testing.T) {
	if os.Getenv("RUN_GOLEAK_TEST") != "1" {
		t.Skip("set RUN_GOLEAK_TEST=1 to run the memory-leak regression test (measures global goroutine count; must run in isolation)")
	}

	iters := envInt("GOLEAK_ITERS", 12)
	threshold := envInt("GOLEAK_MAX_GOROUTINES_PER_CLUSTER", 12)
	if iters <= warmupIters+1 {
		t.Fatalf("GOLEAK_ITERS must be > %d", warmupIters+1)
	}

	var series []int
	var leakBaseline goleak.Option
	for i := range iters {
		runOneCluster(t, i)
		g := numGoroutines()
		series = append(series, g)
		if i == warmupIters {
			// Snapshot goroutines after warmup so goleak attributes only what the
			// post-warmup clusters leak (not one-time singletons/caches).
			leakBaseline = goleak.IgnoreCurrent()
		}
		t.Logf("iter %2d  goroutines=%d", i, g)
	}

	span := iters - 1 - warmupIters
	slope := float64(series[iters-1]-series[warmupIters]) / float64(span)
	t.Logf("post-warmup goroutine growth: %+.2f per cluster over %d clusters (gate: <= %d/cluster)",
		slope, span, threshold)

	if slope > float64(threshold) {
		dir := writeDiagnostics(t, leakBaseline)
		t.Errorf("goroutine leak: %.2f goroutines/cluster exceeds threshold %d. "+
			"Diagnostics (goroutine dump, heap profile, leaked stacks) written to %s", slope, threshold, dir)
	}
}

// runOneCluster creates and tears down one worker-service cluster via the public
// testEnv API. Using a subtest means the env's cleanups (including cluster
// teardown) run before this returns, so the caller observes a post-teardown
// state. WithWorkerService also forces a dedicated, freshly-built cluster per
// iteration (rather than a pooled, reused one).
func runOneCluster(t *testing.T, i int) {
	t.Run(fmt.Sprintf("iter-%02d", i), func(t *testing.T) {
		env := testcore.NewEnv(t, testcore.WithWorkerService("goleak regression test"))
		_ = env.FrontendClient()
	})
}

// numGoroutines returns the live goroutine count after forcing GC (twice, so the
// second cycle reclaims finalizer-revived objects) and a short settle so that
// transient shutdown goroutines don't inflate the reading. The settle loop exits
// early once the count is stable, so a longer GOLEAK_SETTLE_SECONDS only matters if
// goroutines are still draining.
func numGoroutines() int {
	runtime.GC()
	runtime.GC()
	deadline := time.Now().Add(time.Duration(envInt("GOLEAK_SETTLE_SECONDS", 2)) * time.Second)
	prev := runtime.NumGoroutine()
	stable := 0
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		cur := runtime.NumGoroutine()
		if cur >= prev {
			if stable++; stable >= 2 {
				break
			}
		} else {
			stable = 0
		}
		prev = cur
	}
	return runtime.NumGoroutine()
}

// writeDiagnostics dumps a goroutine dump, a heap profile, and the leaked-
// goroutine stacks (relative to the post-warmup baseline) to GOLEAK_OUTPUT_DIR so a
// CI job can upload them as artifacts. Returns the directory used.
func writeDiagnostics(t *testing.T, baseline goleak.Option) string {
	dir := os.Getenv("GOLEAK_OUTPUT_DIR")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "goleak-diagnostics")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Logf("failed to create diagnostics dir %s: %v", dir, err)
		return dir
	}

	// Readable goroutine dump (debug=2) and a pprof heap profile for `go tool pprof`.
	writeProfile(t, filepath.Join(dir, "goroutines.txt"), "goroutine", 2)
	runtime.GC()
	writeProfile(t, filepath.Join(dir, "heap.prof"), "heap", 0)

	// goleak attributes the goroutines leaked since the post-warmup baseline.
	if err := goleak.Find(baseline); err != nil {
		path := filepath.Join(dir, "leaked-goroutines.txt")
		if werr := os.WriteFile(path, []byte(err.Error()), 0o644); werr != nil {
			t.Logf("failed to write %s: %v", path, werr)
		}
		t.Logf("leaked goroutines (goleak):\n%s", err.Error())
	}
	return dir
}

func writeProfile(t *testing.T, path, name string, debug int) {
	f, err := os.Create(path)
	if err != nil {
		t.Logf("failed to create %s: %v", path, err)
		return
	}
	defer f.Close()
	if p := pprof.Lookup(name); p != nil {
		if err := p.WriteTo(f, debug); err != nil {
			t.Logf("failed to write %s: %v", path, err)
		}
	}
}
