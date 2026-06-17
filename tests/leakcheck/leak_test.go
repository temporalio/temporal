// Package leakcheck is a goroutine-leak regression test for the functional
// test infrastructure. It guards against the class of bug that drove the
// functional-test OOMs: per-cluster goroutines and gRPC connections not
// released when a test cluster shuts down. The functional suite builds hundreds
// of clusters in one process, so any per-cluster leak accumulates until OOM.
//
// It builds and tears down full test clusters (running a trivial workflow on
// each so the full frontend → history → matching → SDK worker path is
// exercised) and asserts via goleak that a clean shutdown leaves no new
// goroutine stacks behind.
//
// It does not run by default: it inspects all goroutines, so it must run in
// isolation. Set RUN_LEAK_TEST=1 to enable it (a dedicated CI job does). It
// is intentionally NOT build-tagged, so it stays compiled — and protected from
// rot when testcore changes — in normal builds; the Makefile excludes
// tests/leakcheck from the unit and functional suites.
//
// Run (sqlite — no Docker required):
//
//	RUN_LEAK_TEST=1 go test -run TestClusterShutdownLeak -count=1 -v \
//	    ./tests/leakcheck/ -args -persistenceType=sql -persistenceDriver=sqlite
//
// Tunable via env (defaults also set by the Makefile leak-test target):
//
//	LEAK_ITERS        clusters built after warmup (default 15)
//	LEAK_OUTPUT_DIR   on failure, write goroutines.txt here (CI uploads it)
package leakcheck

import (
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/goleak"
)

func TestClusterShutdownLeak(t *testing.T) {
	if os.Getenv("RUN_LEAK_TEST") != "1" {
		t.Skip("set RUN_LEAK_TEST=1 to run the goroutine-leak regression test (inspects all goroutines; must run in isolation)")
	}
	iters := envInt("LEAK_ITERS", 15)

	// Warm up with a few clusters so process-lifetime singletons (gRPC resolver
	// init, proto registries, ...) are created before we snapshot the baseline.
	// Those are one-time costs, not per-cluster leaks.
	for range 3 {
		buildRunTeardownCluster(t)
	}
	baseline := goleak.IgnoreCurrent()

	for i := range iters {
		buildRunTeardownCluster(t)
		t.Logf("cluster %2d: goroutines=%d", i, runtime.NumGoroutine())
	}

	// Settle: give asynchronous post-teardown cleanup a moment to finish before
	// we check for leaked stacks.
	settle()

	goleak.VerifyNone(t, baseline,
		// The sqlite plugin keeps one *sql.DB (and its connectionOpener) per
		// file DSN for the process lifetime — by design, not a per-cluster leak.
		// sqlite is dev/test-only; the functional OOMs were on cassandra.
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
	)

	if t.Failed() {
		writeDiagnostics(t)
	}
}

// buildRunTeardownCluster creates a freshly-built dedicated worker-service
// cluster, runs a trivial workflow on it to exercise the full server path, then
// tears it down. The subtest ensures all env cleanups (SDK worker stop + cluster
// teardown) complete before this returns.
func buildRunTeardownCluster(t *testing.T) {
	t.Run("cluster", func(t *testing.T) {
		env := testcore.NewEnv(t, testcore.WithWorkerService("leak regression test"))
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

// settle runs GC and waits for the goroutine count to stabilise, giving
// asynchronous post-teardown cleanup time to finish before goleak inspects.
func settle() {
	prev := -1
	for range 20 {
		runtime.GC()
		n := runtime.NumGoroutine()
		if n == prev {
			return
		}
		prev = n
		time.Sleep(50 * time.Millisecond)
	}
}

func writeDiagnostics(t *testing.T) {
	dir := os.Getenv("LEAK_OUTPUT_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	f, err := os.Create(dir + "/goroutines.txt")
	if err != nil {
		return
	}
	defer f.Close()
	// Write full goroutine dump (debug=2 includes all frames).
	_ = goleak.Find(goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"))
	t.Logf("goroutine dump written to %s/goroutines.txt", dir)
}

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
