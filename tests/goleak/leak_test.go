// Package goleak hosts a goroutine-leak regression test for the functional test
// infrastructure.
//
// It builds and tears down full test clusters (running a trivial workflow on
// each, so the whole frontend -> history -> matching -> SDK worker path is
// exercised) and asserts, via goleak, that a clean shutdown leaves no goroutines
// behind. A surviving goroutine is the signature of a shutdown leak — clients,
// connections, background loops, or SDK workers not released — the class of bug
// that drove the functional-test OOMs.
//
// It does not run by default: it inspects all goroutines, so it must run in
// isolation, and it spins up real clusters. Set RUN_GOLEAK_TEST=1 to enable it
// (a dedicated CI job does). It is intentionally NOT build-tagged, so it stays
// compiled — and protected from rot when testcore changes — in normal builds.
// The Makefile already excludes tests/goleak from the unit and functional suites.
//
// Run (sqlite => no Docker required):
//
//	RUN_GOLEAK_TEST=1 go test -run TestClusterShutdownLeak -count=1 -v \
//	    ./tests/goleak/ -args -persistenceType=sql -persistenceDriver=sqlite
//
// On failure, goleak prints the leaked goroutine stacks (CI captures them in the
// job log), which name the leaking subsystem.

package goleak

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/goleak"
)

func TestClusterShutdownLeak(t *testing.T) {
	if os.Getenv("RUN_GOLEAK_TEST") != "1" {
		t.Skip("set RUN_GOLEAK_TEST=1 to run the goroutine-leak regression test (inspects all goroutines; must run in isolation)")
	}

	// Warm up with one cluster so process-lifetime singletons (created once and
	// kept for the whole process) exist before we snapshot the baseline — they
	// must not be counted as leaks.
	buildRunTeardownCluster(t)
	baseline := goleak.IgnoreCurrent()

	// Build and tear down a few more clusters. A clean shutdown leaves no new
	// goroutines; goleak fails (printing the leaked stacks) if any survive.
	for range 3 {
		buildRunTeardownCluster(t)
	}

	goleak.VerifyNone(t, baseline,
		// The sqlite plugin intentionally keeps one *sql.DB (and its background
		// connectionOpener) per file DSN for the lifetime of the process; that is
		// not a per-shutdown leak, and sqlite is dev/test-only.
		goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
	)
}

// buildRunTeardownCluster creates a freshly-built, dedicated test cluster via the
// public testEnv API, runs a trivial workflow on it, then tears it down. Running
// it inside a subtest means the env's cleanups (SDK worker stop + cluster
// teardown) run before this returns, so the caller observes a post-teardown
// state.
//
// WithWorkerService forces a fresh, dedicated cluster that is torn down per call
// and exercises the worker-service shutdown path. (A plain pooled cluster would
// be reused, not torn down — useless for a leak check.) The workflow doubles as
// a liveness check: if the cluster weren't functional, it would fail here.
func buildRunTeardownCluster(t *testing.T) {
	t.Run("cluster", func(t *testing.T) {
		env := testcore.NewEnv(t, testcore.WithWorkerService("goleak regression test"))
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
