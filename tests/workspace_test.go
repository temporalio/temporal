package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
)

type WorkspaceSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkspaceSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkspaceSuite))
}

// TestWorkspaceRebuildAfterWorkerRestart verifies that a durable workspace
// persists across worker restarts. Activity 1 writes a file, worker dies,
// worker 2 reconstructs workspace from external storage, activity 2 reads
// the file.
func (s *WorkspaceSuite) TestWorkspaceRebuildAfterWorkerRestart() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("workspace-tq")
	workflowID := testcore.RandomizeStr("workspace-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")

	const signalProceed = "proceed"

	// Channel to notify test that step 1 completed.
	step1DoneCh := make(chan struct{}, 1)

	// --- Workflow ---
	workspaceFn := func(ctx workflow.Context) (string, error) {
		logger := workflow.GetLogger(ctx)

		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: "ws-1"},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		// Step 1: Write a file.
		err := workflow.ExecuteActivity(ctx, "WriteFile").Get(ctx, nil)
		if err != nil {
			return "", fmt.Errorf("WriteFile failed: %w", err)
		}
		logger.Info("Step 1 complete")

		// Wait for proceed signal (test sends after restarting worker).
		signalCh := workflow.GetSignalChannel(ctx, signalProceed)
		signalCh.Receive(ctx, nil)
		logger.Info("Received proceed signal")

		// Step 2: Read and uppercase.
		var result string
		err = workflow.ExecuteActivity(ctx, "ReadAndUppercase").Get(ctx, &result)
		if err != nil {
			return "", fmt.Errorf("ReadAndUppercase failed: %w", err)
		}
		return result, nil
	}

	// --- Activities: user code only touches files, no workspace manager calls ---
	writeFileActivity := func(ctx context.Context) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return fmt.Errorf("get workspace path: %w", err)
		}
		if err := os.WriteFile(filepath.Join(wsPath, "output.txt"), []byte("hello world"), 0o644); err != nil {
			return err
		}
		select {
		case step1DoneCh <- struct{}{}:
		default:
		}
		return nil
	}

	readAndUppercaseActivity := func(ctx context.Context) (string, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return "", fmt.Errorf("get workspace path: %w", err)
		}
		data, err := os.ReadFile(filepath.Join(wsPath, "output.txt"))
		if err != nil {
			return "", fmt.Errorf("read output.txt: %w", err)
		}
		return strings.ToUpper(string(data)), nil
	}

	// --- Helper: create workspace-enabled worker ---
	newWorker := func(basePath string) sdkworker.Worker {
		snapshotFS := sdkworker.NewTarDiffFS(filepath.Join(basePath, "fs"))
		driver := converter.NewLocalFileStoreDriver(storeDir)
		wsMgr := sdkworker.NewWorkspaceManager(snapshotFS, driver)

		w := sdkworker.New(s.SdkClient(), taskQueue, sdkworker.Options{
			WorkspaceManager: wsMgr,
		})
		w.RegisterWorkflowWithOptions(workspaceFn, workflow.RegisterOptions{Name: "WorkspaceWorkflow"})
		w.RegisterActivityWithOptions(writeFileActivity, activity.RegisterOptions{Name: "WriteFile"})
		w.RegisterActivityWithOptions(readAndUppercaseActivity, activity.RegisterOptions{Name: "ReadAndUppercase"})
		return w
	}

	// --- Worker 1 ---
	w1 := newWorker(filepath.Join(s.T().TempDir(), "worker1"))
	s.NoError(w1.Start())

	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "WorkspaceWorkflow")
	s.NoError(err)

	// Wait for step 1 (file written).
	select {
	case <-step1DoneCh:
	case <-ctx.Done():
		s.FailNow("timed out waiting for step 1")
	}

	w1.Stop()

	// --- Worker 2 (fresh filesystem, same external storage) ---
	w2 := newWorker(filepath.Join(s.T().TempDir(), "worker2"))
	s.NoError(w2.Start())
	defer w2.Stop()

	// Signal workflow to proceed to step 2.
	s.NoError(s.SdkClient().SignalWorkflow(ctx, workflowID, "", signalProceed, nil))

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("HELLO WORLD", result)
}
