package replaytester

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/worker/workerdeployment"
)

// TestReplays tests workflow logic backwards compatibility from previous versions.
func TestReplays(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	deploymentWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
		maxVersionsGetter := func() int {
			return 100
		}
		return workerdeployment.Workflow(ctx, maxVersionsGetter, args)
	}
	replayer.RegisterWorkflowWithOptions(workerdeployment.VersionWorkflow, workflow.RegisterOptions{Name: workerdeployment.WorkerDeploymentVersionWorkflowType})
	replayer.RegisterWorkflowWithOptions(deploymentWorkflow, workflow.RegisterOptions{Name: workerdeployment.WorkerDeploymentWorkflowType})

	files, err := filepath.Glob("testdata/replay_*.json.gz")
	require.NoError(t, err)

	fmt.Println("Number of files to replay:", len(files))

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
