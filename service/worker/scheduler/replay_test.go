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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/worker/scheduler"
)

// TestReplays tests workflow logic backwards compatibility from previous versions.
// Whenever there's a change in logic, consider capturing a new history with the
// testdata/generate_history.sh script and checking it in.
func TestReplays(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(scheduler.SchedulerWorkflow, workflow.RegisterOptions{Name: scheduler.WorkflowType})

	files, err := filepath.Glob("testdata/replay_*.json.gz")
	require.NoError(t, err)

	logger := log.NewSdkLogger(log.NewTestLogger())

	for _, filename := range files {
		t.Run(filepath.Base(filename), func(t *testing.T) {
			f, err := os.Open(filename)
			require.NoError(t, err)
			defer f.Close()
			r, err := gzip.NewReader(f)
			require.NoError(t, err)
			defer r.Close()
			history, err := client.HistoryFromJSON(r, client.HistoryJSONOptions{})
			require.NoError(t, err)
			require.NoError(t, replayer.ReplayWorkflowHistory(logger, history))
		})
	}
}
