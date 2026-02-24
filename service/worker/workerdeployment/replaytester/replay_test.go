package replaytester

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	// For each workflow implementation version we run all the replay tests for snapshots created by that version or older versions
	for wv := workerdeployment.InitialVersion; wv <= workerdeployment.VersionDataRevisionNumber; wv++ {
		replayer := worker.NewWorkflowReplayer()

		// Create version workflow wrapper to match production registration
		versionWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentVersionWorkflowArgs) error {
			refreshIntervalGetter := func() time.Duration {
				return 5 * time.Minute // default value for testing
			}
			visibilityGracePeriodGetter := func() time.Duration {
				return 3 * time.Minute // default value for testing
			}
			return workerdeployment.VersionWorkflow(ctx, nil, refreshIntervalGetter, visibilityGracePeriodGetter, args)
		}

		// Create deployment workflow wrapper to match production registration
		deploymentWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
			workflowVersionGetter := func() workerdeployment.DeploymentWorkflowVersion {
				return wv
			}
			maxVersionsGetter := func() int {
				return 100
			}
			return workerdeployment.Workflow(ctx, workflowVersionGetter, maxVersionsGetter, args)
		}

		replayer.RegisterWorkflowWithOptions(versionWorkflow, workflow.RegisterOptions{Name: workerdeployment.WorkerDeploymentVersionWorkflowType})
		replayer.RegisterWorkflowWithOptions(deploymentWorkflow, workflow.RegisterOptions{Name: workerdeployment.WorkerDeploymentWorkflowType})

		logger := log.NewSdkLogger(log.NewTestLogger())

		// Test all run directories (default behavior for comprehensive replay testing)
		testAllRunDirectories(t, replayer, logger, wv)
	}
}

// testAllRunDirectories tests all directories prepended with "run_" since they contain replay test data.
// For each workflow implementation version we run all the replay tests for snapshots created by that version or older versions
func testAllRunDirectories(t *testing.T, replayer worker.WorkflowReplayer, logger *log.SdkLogger, workflowImplementationVersion workerdeployment.DeploymentWorkflowVersion) {
	// Warning: the pattern here might not work if workflowImplementationVersion grow more than 9. But by then we should be in CHASM!
	runDirs, err := filepath.Glob(fmt.Sprintf("testdata/v[0-%d]/run_*", workflowImplementationVersion))
	require.NoError(t, err)

	if len(runDirs) == 0 {
		t.Skip("No run directories found. Run generate_history.sh first.")
	}

	fmt.Printf("Testing %d run directories\n", len(runDirs))

	for _, runDir := range runDirs {
		t.Run(fmt.Sprintf("%s on v%d", filepath.Base(runDir), workflowImplementationVersion), func(t *testing.T) {
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

	// Validate that workflow counts match expected values
	validateWorkflowCounts(t, files, runDir)

	// Validate that histories replay successfully
	for _, filename := range files {
		t.Run(filepath.Base(filename), func(t *testing.T) {
			replayWorkflowHistory(t, replayer, logger, filename)
		})
	}
}

// replayWorkflowHistory replays a single workflow history file and validates it
func replayWorkflowHistory(t *testing.T, replayer worker.WorkflowReplayer, logger *log.SdkLogger, filename string) {
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

// readExpectedCounts reads expected workflow counts from the expected_counts.txt file
func readExpectedCounts(runDir string) (deploymentCount, versionCount int, err error) {
	expectedCountsFile := filepath.Join(runDir, "expected_counts.txt")

	file, err := os.Open(expectedCountsFile)
	if err != nil {
		// File doesn't exist - this might be an older test data directory
		return 0, 0, fmt.Errorf("expected_counts.txt not found in %s: %w", runDir, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || line == "" {
			continue // Skip comments and empty lines
		}

		if strings.HasPrefix(line, "EXPECTED_DEPLOYMENT_WORKFLOWS=") {
			value := strings.TrimPrefix(line, "EXPECTED_DEPLOYMENT_WORKFLOWS=")
			deploymentCount, err = strconv.Atoi(value)
			if err != nil {
				return 0, 0, fmt.Errorf("invalid deployment count: %w", err)
			}
		} else if strings.HasPrefix(line, "EXPECTED_VERSION_WORKFLOWS=") {
			value := strings.TrimPrefix(line, "EXPECTED_VERSION_WORKFLOWS=")
			versionCount, err = strconv.Atoi(value)
			if err != nil {
				return 0, 0, fmt.Errorf("invalid version count: %w", err)
			}
		}
	}

	return deploymentCount, versionCount, scanner.Err()
}

// validateWorkflowCounts ensures the number of deployment and version workflows matches expectations
func validateWorkflowCounts(t *testing.T, files []string, runDir string) {
	// Read expected counts from file
	expectedDeploymentCount, expectedVersionCount, err := readExpectedCounts(runDir)
	if err != nil {
		// For backwards compatibility, skip validation if expected_counts.txt doesn't exist
		fmt.Printf("  ⚠️  Skipping workflow count validation since expected_counts.txt doesn't exist for this test data directory; this is expected for older test data directories: %v\n", err)
		return
	}

	// Count actual workflows
	actualDeploymentCount := 0
	actualVersionCount := 0

	for _, file := range files {
		filename := filepath.Base(file)
		// Only count .gz files since those are the ones used for replay testing
		if !strings.HasSuffix(filename, ".json.gz") {
			continue
		}

		if strings.Contains(filename, "replay_worker_deployment_wf_run_") {
			actualDeploymentCount++
		} else if strings.Contains(filename, "replay_worker_deployment_version_wf_run_") {
			actualVersionCount++
		}
	}

	fmt.Printf("  Workflow counts - Expected: Deployment=%d, Version=%d | Actual: Deployment=%d, Version=%d\n",
		expectedDeploymentCount, expectedVersionCount, actualDeploymentCount, actualVersionCount)

	require.Equal(t, expectedDeploymentCount, actualDeploymentCount,
		"Deployment workflow count mismatch in %s. Expected %d, got %d. "+
			"This could mean your changes caused additional workflow executions. "+
			"If this is expected, regenerate test data with: EXPECTED_DEPLOYMENT_WORKFLOWS=%d ./generate_history.sh",
		runDir, expectedDeploymentCount, actualDeploymentCount, actualDeploymentCount)

	require.Equal(t, expectedVersionCount, actualVersionCount,
		"Version workflow count mismatch in %s. Expected %d, got %d. "+
			"This could mean your changes caused additional workflow executions. "+
			"If this is expected, regenerate test data with: EXPECTED_VERSION_WORKFLOWS=%d ./generate_history.sh",
		runDir, expectedVersionCount, actualVersionCount, actualVersionCount)
}
