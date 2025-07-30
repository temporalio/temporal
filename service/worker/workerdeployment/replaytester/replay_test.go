package replaytester

import (
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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

	// Create version workflow wrapper to match production registration
	versionWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentVersionWorkflowArgs) error {
		refreshIntervalGetter := func() any {
			return 5 * time.Minute // default value for testing
		}
		visibilityGracePeriodGetter := func() any {
			return 3 * time.Minute // default value for testing
		}
		return workerdeployment.VersionWorkflow(ctx, refreshIntervalGetter, visibilityGracePeriodGetter, args)
	}

	// Create deployment workflow wrapper to match production registration
	deploymentWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
		maxVersionsGetter := func() int {
			return 100
		}
		return workerdeployment.Workflow(ctx, maxVersionsGetter, args)
	}

	replayer.RegisterWorkflowWithOptions(versionWorkflow, workflow.RegisterOptions{Name: workerdeployment.WorkerDeploymentVersionWorkflowType})
	replayer.RegisterWorkflowWithOptions(deploymentWorkflow, workflow.RegisterOptions{Name: workerdeployment.WorkerDeploymentWorkflowType})

	logger := log.NewSdkLogger(log.NewTestLogger())

	// Test all run directories (default behavior for comprehensive replay testing)
	testAllRunDirectories(t, replayer, logger)

}

// testAllRunDirectories tests all directories prepended with "run_" since they contain replay test data
func testAllRunDirectories(t *testing.T, replayer worker.WorkflowReplayer, logger *log.SdkLogger) {
	runDirs, err := filepath.Glob("testdata/run_*")
	require.NoError(t, err)

	if len(runDirs) == 0 {
		t.Skip("No run directories found. Run generate_history.sh first.")
	}

	fmt.Printf("Testing %d run directories\n", len(runDirs))

	for _, runDir := range runDirs {
		t.Run(filepath.Base(runDir), func(t *testing.T) {
			fmt.Printf("Testing run: %s\n", runDir)
			testRunDirectory(t, replayer, logger, runDir)
		})
	}
}

// testRunDirectory tests all workflow histories in a specific run directory
func testRunDirectory(t *testing.T, replayer worker.WorkflowReplayer, logger *log.SdkLogger, runDir string) {
	files, err := filepath.Glob(filepath.Join(runDir, "replay_*.json.gz"))
	require.NoError(t, err)

	fmt.Printf("  Found %d workflow histories to replay\n", len(files))

	for _, filename := range files {
		t.Run(filepath.Base(filename), func(t *testing.T) {
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
		})
	}
}
