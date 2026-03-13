package scheduler_test

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/worker/scheduler"
)

// TestReplays tests workflow logic backwards compatibility from previous versions.
// Whenever there's a change in logic, consider capturing a new history with the
// testdata/generate_history.sh script and checking it in.
func TestReplays(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	wfFunc := func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return scheduler.SchedulerWorkflowWithDeps(ctx, args, scheduler.NewSpecBuilder(), false)
	}
	replayer.RegisterWorkflowWithOptions(wfFunc, workflow.RegisterOptions{Name: scheduler.WorkflowType})

	replayAll(t, replayer)
}

// TestReplaysWithCHASMMigration verifies that enabling CHASM migration does not
// break replay determinism for existing histories.
func TestReplaysWithCHASMMigration(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	wfFunc := func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return scheduler.SchedulerWorkflowWithDeps(ctx, args, scheduler.NewSpecBuilder(), true)
	}
	replayer.RegisterWorkflowWithOptions(wfFunc, workflow.RegisterOptions{Name: scheduler.WorkflowType})

	replayAll(t, replayer)
}

func replayAll(t *testing.T, replayer worker.WorkflowReplayer) {
	t.Helper()

	files, err := filepath.Glob("testdata/replay_*.json.gz")
	require.NoError(t, err)

	logger := log.NewSdkLogger(log.NewTestLogger())

	for _, filename := range files {
		logger.Info("Replaying", "file", filename)
		f, err := os.Open(filename)
		require.NoError(t, err)
		r, err := gzip.NewReader(f)
		require.NoError(t, err)
		history, err := client.HistoryFromJSON(r, client.HistoryJSONOptions{})
		require.NoError(t, err)
		err = replayer.ReplayWorkflowHistory(logger, history)
		require.NoError(t, err)
		_ = r.Close()
		_ = f.Close()
	}
}
