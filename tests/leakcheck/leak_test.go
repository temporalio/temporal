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
// Tunable via env:
//
//	LEAK_ITERS        clusters built after warmup (set by the Makefile)
//	LEAK_OUTPUT_DIR   on failure, write goroutines.txt here (CI uploads it)
package leakcheck

import (
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/goleak"
)

// sqliteConnOpener is the top-of-stack for the background goroutine the sqlite
// plugin keeps alive per file DSN for the process lifetime. That is by design,
// not a per-cluster leak; sqlite is dev/test-only.
const sqliteConnOpener = "database/sql.(*DB).connectionOpener"

func TestClusterShutdownLeak(t *testing.T) {
	iters, err := strconv.Atoi(os.Getenv("LEAK_ITERS"))
	if err != nil {
		t.Fatal("LEAK_ITERS must be set to a positive integer (set by the Makefile leak-test target)")
	}

	// Warm up with a few clusters so process-lifetime singletons (gRPC resolver
	// init, proto registries, ...) are created before we snapshot the baseline.
	// Those are one-time costs, not per-cluster leaks.
	for range 3 {
		buildRunTeardownCluster(t)
	}

	// Use goleak's built-in retry (20 attempts, exponential backoff capped at
	// 100 ms) to wait for warmup goroutines to drain before snapshotting the
	// baseline. goleak.IgnoreCurrent filters by stack trace, so a still-draining
	// warmup goroutine would mask an identical per-cluster leak. We discard the
	// Find result: any persistent goroutines that survive the retry window are
	// legitimate process-lifetime singletons that IgnoreCurrent will baseline.
	_ = goleak.Find(goleak.IgnoreTopFunction(sqliteConnOpener))
	baseline := goleak.IgnoreCurrent()

	for i := range iters {
		buildRunTeardownCluster(t)
		t.Logf("cluster %2d: goroutines=%d", i, runtime.NumGoroutine())
	}

	goleak.VerifyNone(t, baseline,
		goleak.IgnoreTopFunction(sqliteConnOpener),
	)

	if t.Failed() {
		if dir := os.Getenv("LEAK_OUTPUT_DIR"); dir != "" {
			if err := os.MkdirAll(dir, 0o755); err == nil {
				if f, err := os.Create(dir + "/goroutines.txt"); err == nil {
					_ = goleak.Find(goleak.IgnoreTopFunction(sqliteConnOpener))
					f.Close()
					t.Logf("goroutine dump written to %s/goroutines.txt", dir)
				}
			}
		}
	}
}

// buildRunTeardownCluster creates a freshly-built dedicated cluster,
// runs a trivial workflow on it to exercise the full server path,
// then tears it down.
func buildRunTeardownCluster(t *testing.T) {
	// using t.Run to ensure subtest cleanups
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
