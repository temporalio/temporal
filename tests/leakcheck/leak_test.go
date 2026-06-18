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
//	LEAK_ITERS         clusters built after warmup
//	LEAK_ITERS_WARMUP  warmup clusters before snapshotting the baseline
//	LEAK_OUTPUT_DIR    directory for diagnostics on failure (CI uploads it)
package leakcheck

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/goleak"
)

var opts = []goleak.Option{
	// The sqlite plugin keeps one *sql.DB (and its connectionOpener) per
	// file DSN for the process lifetime — by design, not a per-cluster leak.
	// sqlite is dev/test-only; the functional OOMs were on cassandra.
	goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
}

func TestClusterShutdownLeak(t *testing.T) {
	iters, err := strconv.Atoi(os.Getenv("LEAK_ITERS"))
	if err != nil {
		t.Fatal("LEAK_ITERS must be set to a positive integer")
	}
	warmupIters, err := strconv.Atoi(os.Getenv("LEAK_ITERS_WARMUP"))
	if err != nil {
		t.Fatal("LEAK_ITERS_WARMUP must be set to a positive integer")
	}
	outputDir := os.Getenv("LEAK_OUTPUT_DIR")
	if outputDir == "" {
		t.Fatal("LEAK_OUTPUT_DIR must be set")
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("LEAK_OUTPUT_DIR: %v", err)
	}

	// Warm up with a few clusters so process-lifetime singletons (gRPC resolver
	// init, proto registries, ...) are created before we snapshot the baseline.
	for range warmupIters {
		buildRunTeardownCluster(t)
	}

	// Wait for warmup goroutines to drain before snapshotting the baseline.
	_ = goleak.Find(opts...)
	baseline := goleak.IgnoreCurrent()

	// Run the leak test: build, run, and tear down a cluster per iteration.
	for i := range iters {
		buildRunTeardownCluster(t)
		t.Logf("cluster %2d: goroutines=%d", i, runtime.NumGoroutine())
	}

	// Verify that no goroutines leaked beyond the baseline.
	goleak.VerifyNone(t, append(opts, baseline)...)

	// On failure, write a goroutine dump to the output directory.
	if t.Failed() {
		f, err := os.Create(filepath.Join(outputDir, "goroutines.txt"))
		if err != nil {
			t.Logf("failed to create goroutine dump: %v", err)
		} else {
			_ = goleak.Find(opts...)
			f.Close()
			t.Logf("goroutine dump written to %s/goroutines.txt", outputDir)
		}
	}
}

// buildRunTeardownCluster creates a freshly-built dedicated worker-service
// cluster, runs a trivial workflow on it to exercise the full server path,
// then tears it down.
func buildRunTeardownCluster(t *testing.T) {
	// The subtest ensures all env cleanups complete before this returns.
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
