package leakcheck

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/objectleak"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/goleak"
)

// goleakOpts are the goleak options applied to every Find/VerifyNone call.
// TODO entries are known leaks to be fixed; remove each ignore once fixed.
var goleakOpts = []goleak.Option{
	// By design: sqlite keeps one *sql.DB per file DSN for the process lifetime.
	goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
}

var objectLeakOpts = []objectleak.Option{
	objectleak.WithPruneType("google.golang.org/protobuf/internal/impl.*"),
	objectleak.WithExpected("FunctionalTestBase.Logger*"),
	objectleak.WithExpected("FunctionalTestBase.testCluster.host*"),
	objectleak.WithExpected("FunctionalTestBase.testCluster.testBase*"),
}

// TestClusterShutdownLeak is a goroutine-leak regression test for the functional
// test infrastructure. It detects per-cluster goroutines not being released when
// a test cluster shuts down.
//
// Tunable via env:
//
//	LEAK_ITERS         clusters built after warmup
//	LEAK_ITERS_WARMUP  warmup clusters before snapshotting the baseline
//	LEAK_OUTPUT_DIR    directory for diagnostics on failure
//	LEAK_GC_SETTLE_TIMEOUT maximum time to settle object leak checks
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
	gcSettleTimeout, err := time.ParseDuration(os.Getenv("LEAK_GC_SETTLE_TIMEOUT"))
	if err != nil {
		t.Fatal("LEAK_GC_SETTLE_TIMEOUT must be set to a positive duration")
	}
	if gcSettleTimeout <= 0 {
		t.Fatal("LEAK_GC_SETTLE_TIMEOUT must be set to a positive duration")
	}

	leakOpts := append([]objectleak.Option{}, objectLeakOpts...)
	leakOpts = append(leakOpts, objectleak.WithGCSettleTimeout(gcSettleTimeout))
	leakCheck, err := objectleak.NewObjectLeakCheck(leakOpts...)
	require.NoError(t, err)

	// Keep the final stopped warmup worker reachable through Check so the SDK's
	// process-wide workflow cache remains the same allocation across the baseline.
	var worker sdkworker.Worker

	// Warm up with a few clusters so process-lifetime singletons (gRPC resolver
	// init, proto registries, ...) are created before we snapshot the baseline.
	for range warmupIters {
		worker = buildRunTeardownCluster(t, &leakCheck)
	}

	// Wait for warmup goroutines to drain before snapshotting the object and goroutine baselines.
	_ = goleak.Find(goleakOpts...)
	objleakBaseline := leakCheck.IgnoreCurrent()
	goleakBaseline := goleak.IgnoreCurrent()

	// Run the leak test: build, run, and tear down a cluster per iteration.
	for i := range iters {
		buildRunTeardownCluster(t, &leakCheck)
		t.Logf("cluster %2d: goroutines=%d", i, runtime.NumGoroutine())
	}

	// Verify that no goroutines leaked beyond the baseline.
	goleakErr := goleak.Find(append(goleakOpts, goleakBaseline)...)
	goleakReport := "no unexpected goroutines\n"
	if goleakErr != nil {
		goleakReport = goleakErr.Error() + "\n"
	}
	writeReport(t, outputDir, "goleak_report.txt", goleakReport)
	writeProfile(t, outputDir, "goroutine", "goroutines_all.txt", 2)

	// Verify that no cluster references leaked.
	leakCheckStart := time.Now()
	leakReport, leakErr := leakCheck.Check(objleakBaseline)
	runtime.KeepAlive(worker) // ensure worker is kept alive until leak check completes

	t.Logf("object leak check settled in %s", time.Since(leakCheckStart).Round(time.Millisecond))
	reportPath := filepath.Join(outputDir, "objectleak_report.txt")
	writeReport(t, outputDir, "objectleak_report.txt", leakReport+"\n")
	writeProfile(t, outputDir, "heap", "heap.pb.gz", 0)
	writeProfile(t, outputDir, "heap", "heap.txt", 1)
	writeProfile(t, outputDir, "allocs", "allocs.pb.gz", 0)
	writeProfile(t, outputDir, "allocs", "allocs.txt", 1)

	if goleakErr != nil {
		t.Error(goleakErr)
	}
	if leakErr != nil {
		t.Errorf("cluster references leaked; see %s", reportPath)
	}
}

// buildRunTeardownCluster creates a dedicated cluster, runs a trivial
// workflow on it to exercise the full server path, then tears it down.
func buildRunTeardownCluster(t *testing.T, leakCheck *objectleak.ObjectLeakCheck) sdkworker.Worker {
	var worker sdkworker.Worker
	// The subtest ensures all env cleanups complete before this returns.
	t.Run("cluster", func(t *testing.T) {
		env := testcore.NewEnv(t,
			testcore.WithDedicatedCluster(),
			testcore.WithWorkerService("leak regression test"))

		worker = env.SdkWorker()
		worker.RegisterWorkflow(smokeWorkflow)
		run, err := env.SdkClient().ExecuteWorkflow(
			context.Background(),
			sdkclient.StartWorkflowOptions{TaskQueue: env.WorkerTaskQueue()},
			smokeWorkflow,
		)
		require.NoError(t, err)
		require.NoError(t, run.Get(context.Background(), nil))

		leakCheck.Track(env)
	})
	return worker
}

func smokeWorkflow(workflow.Context) error { return nil }

func writeReport(t *testing.T, outputDir string, fileName string, contents string) {
	t.Helper()
	path := filepath.Join(outputDir, fileName)
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Logf("failed to write %s: %v", fileName, err)
	} else {
		t.Logf("wrote %s", path)
	}
}

func writeProfile(t *testing.T, outputDir string, profileName string, fileName string, debug int) {
	t.Helper()
	profile := pprof.Lookup(profileName)
	if profile == nil {
		t.Logf("profile %q is unavailable", profileName)
		return
	}
	var dump bytes.Buffer
	if err := profile.WriteTo(&dump, debug); err != nil {
		t.Logf("failed to capture %s profile: %v", profileName, err)
		return
	}
	path := filepath.Join(outputDir, fileName)
	if err := os.WriteFile(path, dump.Bytes(), 0o644); err != nil {
		t.Logf("failed to write %s profile: %v", profileName, err)
	} else {
		t.Logf("%s profile written to %s", profileName, path)
	}
}
