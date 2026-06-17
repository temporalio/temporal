// Package memleak hosts a goroutine/memory leak regression test for the
// functional test infrastructure.
//
// The test repeatedly creates and tears down a full test cluster and asserts
// that the live goroutine count does not grow per cluster. A non-zero slope is
// the signature of a shutdown leak (clients, connections, background loops not
// released) — the class of bug that drove the functional-test OOMs. On failure
// it prints the goroutine stacks whose counts grew, naming the leaking
// subsystem.
//
// It does not run by default: it measures the global goroutine count, so it must
// run in isolation, and it spins up real clusters. Set RUN_MEMLEAK_TEST=1 to
// enable it (a dedicated CI job does this). It is intentionally NOT build-tagged
// so it stays compiled — and thus protected from rot when testcore changes — in
// normal builds. The Makefile already excludes tests/memleak from the unit and
// functional suites.
//
// Run (sqlite => no Docker required):
//
//	RUN_MEMLEAK_TEST=1 go test -run TestClusterGoroutineSlope -count=1 -v \
//	    ./tests/memleak/ -args -persistenceType=sql -persistenceDriver=sqlite
//
// Env knobs:
//
//	RUN_MEMLEAK_TEST=1               enable the test (otherwise skipped)
//	LEAK_ITERS                       cluster create/teardown iterations (default 12)
//	LEAK_MAX_GOROUTINES_PER_CLUSTER  slope threshold for the gate (default 8)
//	LEAK_WORKER_SERVICE=1            also exercise the worker-service shutdown path

package memleak

import (
	"fmt"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.temporal.io/server/tests/testcore"
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

// numGoroutines returns the live goroutine count after forcing GC (twice, so the
// second cycle reclaims finalizer-revived objects) and a short settle so that
// transient shutdown goroutines don't inflate the reading.
func numGoroutines() int {
	runtime.GC()
	runtime.GC()
	deadline := time.Now().Add(time.Duration(envInt("LEAK_SETTLE_SECONDS", 2)) * time.Second)
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

// buildAndTearDownCluster runs one full cluster create+teardown cycle. Set
// LEAK_WORKER_SERVICE=1 to also exercise the worker-service shutdown path.
func buildAndTearDownCluster(t *testing.T) {
	tearDown := testcore.NewUnpooledCluster(t, os.Getenv("LEAK_WORKER_SERVICE") == "1")
	tearDown()
}

// TestClusterGoroutineSlope is the regression gate: it asserts that goroutines do
// not accumulate per cluster create/teardown. It is robust to legitimate fixed
// background goroutines (they don't grow per iteration) and needs no ignore-list.
func TestClusterGoroutineSlope(t *testing.T) {
	if os.Getenv("RUN_MEMLEAK_TEST") != "1" {
		t.Skip("set RUN_MEMLEAK_TEST=1 to run the memory-leak regression test (measures global goroutine count; must run in isolation)")
	}

	iters := envInt("LEAK_ITERS", 12)
	maxPerCluster := envInt("LEAK_MAX_GOROUTINES_PER_CLUSTER", 8)
	if iters <= warmupIters+1 {
		t.Fatalf("LEAK_ITERS must be > %d", warmupIters+1)
	}

	var series []int
	var baseDump, lastDump string
	for i := range iters {
		buildAndTearDownCluster(t)
		g := numGoroutines()
		series = append(series, g)
		switch i {
		case warmupIters:
			baseDump = goroutineDump()
			writeHeapProfile(t, "base")
		case iters - 1:
			lastDump = goroutineDump()
			writeHeapProfile(t, "final")
		}
		t.Logf("iter %2d  goroutines=%d", i, g)
	}

	span := iters - 1 - warmupIters
	slope := float64(series[iters-1]-series[warmupIters]) / float64(span)
	t.Logf("post-warmup goroutine growth: %+.2f per cluster over %d clusters (gate: <= %d)",
		slope, span, maxPerCluster)

	if slope > float64(maxPerCluster) {
		t.Errorf("goroutine leak: %.2f goroutines/cluster (threshold %d). Leaking stacks:\n%s",
			slope, maxPerCluster, diffBySignature(baseDump, lastDump))
	}
}

// writeHeapProfile writes an inuse heap profile to $LEAK_HEAP_PROFILE/<name>.prof
// if that dir is set. Diff with: go tool pprof -base base.prof final.prof
func writeHeapProfile(t *testing.T, name string) {
	dir := os.Getenv("LEAK_HEAP_PROFILE")
	if dir == "" {
		return
	}
	runtime.GC()
	f, err := os.Create(dir + "/" + name + ".prof")
	if err != nil {
		t.Fatalf("create heap profile: %v", err)
	}
	defer f.Close()
	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatalf("write heap profile: %v", err)
	}
	t.Logf("wrote heap profile %s/%s.prof", dir, name)
}

// goroutineDump returns every goroutine's stack.
func goroutineDump() string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

// diffBySignature groups both dumps by stack signature (the per-goroutine block
// without its "goroutine N [state]:" header) and reports, most-grown first, the
// signatures whose count increased between baseDump and lastDump.
func diffBySignature(baseDump, lastDump string) string {
	before := countBySignature(baseDump)
	after := countBySignature(lastDump)

	type grown struct {
		sig   string
		delta int
	}
	var grows []grown
	for sig, n := range after {
		if d := n - before[sig]; d > 0 {
			grows = append(grows, grown{sig, d})
		}
	}
	sort.Slice(grows, func(i, j int) bool { return grows[i].delta > grows[j].delta })

	var b strings.Builder
	for i, g := range grows {
		if i >= 15 {
			fmt.Fprintf(&b, "  ... and %d more\n", len(grows)-15)
			break
		}
		fmt.Fprintf(&b, "  +%d  %s\n", g.delta, firstFrame(g.sig))
	}
	return b.String()
}

var hexAddr = regexp.MustCompile(`0x[0-9a-f]+`)

func countBySignature(dump string) map[string]int {
	out := map[string]int{}
	for _, blk := range strings.Split(dump, "\n\n") {
		blk = strings.TrimSpace(blk)
		if blk == "" {
			continue
		}
		lines := strings.Split(blk, "\n")
		if len(lines) > 0 && strings.HasPrefix(lines[0], "goroutine ") {
			lines = lines[1:]
		}
		// Normalize away per-goroutine pointer args so identical stacks group.
		sig := hexAddr.ReplaceAllString(strings.Join(lines, "\n"), "0x?")
		out[sig]++
	}
	return out
}

// firstFrame returns the top-of-stack function line of a signature.
func firstFrame(sig string) string {
	if i := strings.IndexByte(sig, '\n'); i > 0 {
		return strings.TrimSpace(sig[:i])
	}
	return strings.TrimSpace(sig)
}
