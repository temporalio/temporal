package scheduler_test

import (
	"compress/gzip"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/worker/scheduler"
)

// versionRelease maps a SchedulerWorkflowVersion to its respective initial server release.
// The snapshot is testdata/replay_<server>.json.gz.
type versionRelease struct {
	version scheduler.SchedulerWorkflowVersion
	server  string
}

// Versions sharing a release share a snapshot, but are kept here for completeness.
var versionReleases = []versionRelease{
	// v0. https://github.com/temporalio/temporal/blob/v1.19.1/service/worker/scheduler/workflow.go
	{scheduler.InitialVersion, "v1.19.1"},
	// v1. https://github.com/temporalio/temporal/blob/v1.20.4/service/worker/scheduler/workflow.go
	{scheduler.BatchAndCacheTimeQueries, "v1.20.4"},
	// v2. https://github.com/temporalio/temporal/blob/v1.22.0/service/worker/scheduler/workflow.go
	{scheduler.NewCacheAndJitter, "v1.22.0"},
	// v3. https://github.com/temporalio/temporal/blob/v1.23.0/service/worker/scheduler/workflow.go
	{scheduler.DontTrackOverlapping, "v1.23.0"},
	// v4, v5, v6: https://github.com/temporalio/temporal/blob/v1.24.0/service/worker/scheduler/workflow.go
	{scheduler.InclusiveBackfillStartTime, "v1.24.0"},
	{scheduler.IncrementalBackfill, "v1.24.0"},
	{scheduler.UpdateFromPrevious, "v1.24.0"},
	// v7, v8: https://github.com/temporalio/temporal/blob/v1.25.0/service/worker/scheduler/workflow.go
	{scheduler.CANAfterSignals, "v1.25.0"},
	{scheduler.UseLastAction, "v1.25.0"},
	// v9, v10: https://github.com/temporalio/temporal/blob/v1.27.0/service/worker/scheduler/workflow.go
	{scheduler.AccurateFutureActionTimes, "v1.27.0"},
	{scheduler.ActionResultIncludesStatus, "v1.27.0"},
	// v11, v12: https://github.com/temporalio/temporal/blob/v1.29.0/service/worker/scheduler/workflow.go
	{scheduler.LimitMemoSpecSize, "v1.29.0"},
	{scheduler.TriggerImmediatelyTimestamp, "v1.29.0"},
}

// TestReplays replays every recorded history under the current binary, so a history an older
// server wrote still replays after an upgrade. It covers both the per-release version snapshots
// (versionReleases) and the targeted behavior fixtures (replay_with_*, replay_recent_*) that
// exercise specific version logic.
func TestReplays(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "replay_*.json.gz"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			replay(t, loadHistory(t, f))
		})
	}
}

// TestReplaysWithDynamicConfigChange proves that changing the compute-limit dynamic config
// (Max/WarnIterations) does not break replay of an existing schedule history. The scheduling
// decisions (next times, cache-completed) live in SideEffect / MutableSideEffect markers, so
// replay returns the recorded values rather than recomputing them against the new limits.
// Each history is replayed twice: once with the "original" (effectively disabled) bound, then
// again with a much lower bound simulating an operator lowering SchedulerSpecMaxIterations.
//
// testdata/replay_compute_limit_exceeded.json.gz is the fixture that makes this meaningful (not
// a no-op): it is a V1 schedule whose spec actually tripped the hard limit while it was set to
// 1000 (an over-excluded/pathological spec: calendar matches every second, exclude blocks every
// second except a window ~1800s out, so GetNextTime scans past 1000 excluded candidates). It was
// captured from a real V1 workflow run; replaying it here at both a lower and a higher bound
// confirms the limit-exceeded decision is fixed in a SideEffect marker and cannot diverge on a
// config change.
func TestReplaysWithDynamicConfigChange(t *testing.T) {
	files, err := filepath.Glob("testdata/replay_*.json.gz")
	require.NoError(t, err)

	logger := log.NewSdkLogger(log.NewTestLogger())

	// loadHistory reads a fresh copy of the history. A fresh copy is needed per replay because
	// the replayer consumes/mutates the passed History.
	loadHistory := func(filename string) *historypb.History {
		f, err := os.Open(filename)
		require.NoError(t, err)
		defer func() { _ = f.Close() }()
		r, err := gzip.NewReader(f)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()
		history, err := client.HistoryFromJSON(r, client.HistoryJSONOptions{})
		require.NoError(t, err)
		return history
	}

	// replayWith registers a scheduler workflow whose SpecBuilder reports the given hard bound,
	// then replays a fresh copy of the history against it.
	replayWith := func(maxIterations int, filename string) error {
		b := scheduler.NewSpecBuilder(
			func() int { return scheduler.DefaultWarnIterations },
			func() int { return maxIterations },
		)
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(
			scheduler.SchedulerWorkflowWithSpecBuilder(b),
			workflow.RegisterOptions{Name: scheduler.WorkflowType},
		)
		return replayer.ReplayWorkflowHistory(logger, loadHistory(filename))
	}

	for _, filename := range files {
		logger.Info("Replaying", "file", filename)

		// Original config: hard limit effectively disabled.
		require.NoError(t, replayWith(math.MaxInt, filename),
			"replay with original config should succeed: %s", filename)

		// Simulated dynamic-config change: a much lower hard limit. Replay must still be
		// deterministic because the recorded next times come from SideEffect markers.
		require.NoError(t, replayWith(1000, filename),
			"replay after a dynamic-config change should still be deterministic: %s", filename)
	}
}

// TestEveryVersionIsMapped checks every version from InitialVersion to the highest maps to a
// server release whose snapshot is present, so a newly added version can't escape replay coverage.
func TestEveryVersionIsMapped(t *testing.T) {
	mapped := map[scheduler.SchedulerWorkflowVersion]bool{}
	for _, vr := range versionReleases {
		mapped[vr.version] = true
		_, err := os.Stat(filepath.Join("testdata", "replay_"+vr.server+".json.gz"))
		require.NoErrorf(t, err, "missing snapshot replay_%s.json.gz", vr.server)
	}
	for v := scheduler.InitialVersion; v <= scheduler.TriggerImmediatelyTimestamp; v++ {
		require.Truef(t, mapped[v], "scheduler version v%d is not mapped to a server release in versionReleases", int(v))
	}
}

// replay asserts the history replays under the current scheduler workflow binary.
func replay(t *testing.T, h *historypb.History) {
	t.Helper()
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(scheduler.SchedulerWorkflow, workflow.RegisterOptions{Name: scheduler.WorkflowType})
	require.NoError(t, replayer.ReplayWorkflowHistory(log.NewSdkLogger(log.NewTestLogger()), h))
}

// loadHistory reads a gzipped JSON history fixture.
func loadHistory(t *testing.T, path string) *historypb.History {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err, "missing history %s", path)
	defer func() { _ = f.Close() }()
	r, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	h, err := client.HistoryFromJSON(r, client.HistoryJSONOptions{})
	require.NoError(t, err)
	return h
}
