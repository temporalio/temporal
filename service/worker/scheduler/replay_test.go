package scheduler_test

import (
	"compress/gzip"
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

// versionRelease maps a SchedulerWorkflowVersion to the server release that first recorded it.
// The snapshot is testdata/replay_<server>.json.gz.
type versionRelease struct {
	version scheduler.SchedulerWorkflowVersion
	server  string
}

// versionReleases maps every SchedulerWorkflowVersion to the server release that first recorded it
// versions sharing a release share a snapshot - but are kept here for completeness
var versionReleases = []versionRelease{
	// v0. https://github.com/temporalio/temporal/blob/v1.19.1/service/worker/scheduler/workflow.go
	{scheduler.InitialVersion, "v1.19.1"},
	// v1. https://github.com/temporalio/temporal/blob/v1.20.4/service/worker/scheduler/workflow.go
	{scheduler.BatchAndCacheTimeQueries, "v1.20.4"},
	// v2. https://github.com/temporalio/temporal/blob/v1.22.0/service/worker/scheduler/workflow.go
	{scheduler.NewCacheAndJitter, "v1.22.0"},
	// v3. https://github.com/temporalio/temporal/blob/v1.23.0/service/worker/scheduler/workflow.go
	{scheduler.DontTrackOverlapping, "v1.23.0"},
	// v4, v5, v6: default jumped 3 to 6. https://github.com/temporalio/temporal/blob/v1.24.0/service/worker/scheduler/workflow.go
	{scheduler.InclusiveBackfillStartTime, "v1.24.0"},
	{scheduler.IncrementalBackfill, "v1.24.0"},
	{scheduler.UpdateFromPrevious, "v1.24.0"},
	// v7, v8: default jumped 6 to 8. https://github.com/temporalio/temporal/blob/v1.25.0/service/worker/scheduler/workflow.go
	{scheduler.CANAfterSignals, "v1.25.0"},
	{scheduler.UseLastAction, "v1.25.0"},
	// v9, v10: default jumped 8 to 10. https://github.com/temporalio/temporal/blob/v1.27.0/service/worker/scheduler/workflow.go
	{scheduler.AccurateFutureActionTimes, "v1.27.0"},
	{scheduler.ActionResultIncludesStatus, "v1.27.0"},
	// v11, v12: default jumped 10 to 12. https://github.com/temporalio/temporal/blob/v1.29.0/service/worker/scheduler/workflow.go
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

// TestEveryVersionIsMapped checks every version from LowestVersion to HighestVersion maps to a
// server release whose snapshot is present, so a newly added version can't escape replay coverage.
func TestEveryVersionIsMapped(t *testing.T) {
	mapped := map[scheduler.SchedulerWorkflowVersion]bool{}
	for _, vr := range versionReleases {
		mapped[vr.version] = true
		_, err := os.Stat(filepath.Join("testdata", "replay_"+vr.server+".json.gz"))
		require.NoErrorf(t, err, "missing snapshot replay_%s.json.gz", vr.server)
	}
	for v := scheduler.LowestVersion; v <= scheduler.HighestVersion; v++ {
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
