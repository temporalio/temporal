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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

type WorkspaceV2Suite struct {
	testcore.FunctionalTestBase
}

func TestWorkspaceV2Suite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkspaceV2Suite))
}

func (s *WorkspaceV2Suite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
			dynamicconfig.EnableChasm.Key(): true,
		}),
	)
}

// TestWorkspaceV2_BasicWriteRead validates the fundamental workspace flow:
// Activity-1 writes a file, Activity-2 reads it back.
func (s *WorkspaceV2Suite) TestWorkspaceV2_BasicWriteRead() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("ws-v2-tq")
	workflowID := testcore.RandomizeStr("ws-v2-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")

	// --- Workflow ---
	workspaceFn := func(ctx workflow.Context) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: workflowID + "-ws"},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		// Step 1: Write a file.
		err := workflow.ExecuteActivity(ctx, "WriteFile").Get(ctx, nil)
		if err != nil {
			return "", fmt.Errorf("WriteFile failed: %w", err)
		}

		// Step 2: Read and uppercase.
		var result string
		err = workflow.ExecuteActivity(ctx, "ReadAndUppercase").Get(ctx, &result)
		if err != nil {
			return "", fmt.Errorf("ReadAndUppercase failed: %w", err)
		}
		return result, nil
	}

	// --- Activities ---
	writeFileActivity := func(ctx context.Context) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return fmt.Errorf("get workspace path: %w", err)
		}
		return os.WriteFile(filepath.Join(wsPath, "output.txt"), []byte("hello world"), 0o644)
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

	// --- Worker ---
	snapshotFS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "fs"))
	driver := converter.NewLocalFileStoreDriver(storeDir)
	wsMgr := sdkworker.NewWorkspaceManager(snapshotFS, driver)

	w := sdkworker.New(s.SdkClient(), taskQueue, sdkworker.Options{
		WorkspaceManager: wsMgr,
	})
	w.RegisterWorkflowWithOptions(workspaceFn, workflow.RegisterOptions{Name: "WorkspaceV2Workflow"})
	w.RegisterActivityWithOptions(writeFileActivity, activity.RegisterOptions{Name: "WriteFile"})
	w.RegisterActivityWithOptions(readAndUppercaseActivity, activity.RegisterOptions{Name: "ReadAndUppercase"})
	s.NoError(w.Start())
	defer w.Stop()

	// --- Execute workflow ---
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "WorkspaceV2Workflow")
	s.NoError(err)

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("HELLO WORLD", result)
}

// TestWorkspaceV2_WorkerRestart validates workspace durability across worker restarts.
func (s *WorkspaceV2Suite) TestWorkspaceV2_WorkerRestart() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("ws-v2-restart-tq")
	workflowID := testcore.RandomizeStr("ws-v2-restart-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")

	const signalProceed = "proceed"
	step1DoneCh := make(chan struct{}, 1)

	// --- Workflow ---
	workspaceFn := func(ctx workflow.Context) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: workflowID + "-ws"},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		err := workflow.ExecuteActivity(ctx, "WriteFile").Get(ctx, nil)
		if err != nil {
			return "", fmt.Errorf("WriteFile failed: %w", err)
		}

		signalCh := workflow.GetSignalChannel(ctx, signalProceed)
		signalCh.Receive(ctx, nil)

		var result string
		err = workflow.ExecuteActivity(ctx, "ReadAndUppercase").Get(ctx, &result)
		if err != nil {
			return "", fmt.Errorf("ReadAndUppercase failed: %w", err)
		}
		return result, nil
	}

	// --- Activities ---
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
		w.RegisterWorkflowWithOptions(workspaceFn, workflow.RegisterOptions{Name: "WorkspaceV2RestartWorkflow"})
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
	}, "WorkspaceV2RestartWorkflow")
	s.NoError(err)

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

	s.NoError(s.SdkClient().SignalWorkflow(ctx, workflowID, "", signalProceed, nil))

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("HELLO WORLD", result)
}

// TestWorkspaceV2_MultipleActivities validates workspace state accumulates
// correctly across 3+ sequential activities.
func (s *WorkspaceV2Suite) TestWorkspaceV2_MultipleActivities() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("ws-v2-multi-tq")
	workflowID := testcore.RandomizeStr("ws-v2-multi-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")

	// --- Workflow ---
	workspaceFn := func(ctx workflow.Context) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: workflowID + "-ws"},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		err := workflow.ExecuteActivity(ctx, "WriteFileA").Get(ctx, nil)
		if err != nil {
			return "", fmt.Errorf("WriteFileA failed: %w", err)
		}

		err = workflow.ExecuteActivity(ctx, "WriteFileB").Get(ctx, nil)
		if err != nil {
			return "", fmt.Errorf("WriteFileB failed: %w", err)
		}

		var result string
		err = workflow.ExecuteActivity(ctx, "ReadBothFiles").Get(ctx, &result)
		if err != nil {
			return "", fmt.Errorf("ReadBothFiles failed: %w", err)
		}
		return result, nil
	}

	// --- Activities ---
	writeFileA := func(ctx context.Context) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(wsPath, "file-a.txt"), []byte("alpha"), 0o644)
	}
	writeFileB := func(ctx context.Context) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		if _, err := os.Stat(filepath.Join(wsPath, "file-a.txt")); err != nil {
			return fmt.Errorf("file-a.txt not found: %w", err)
		}
		return os.WriteFile(filepath.Join(wsPath, "file-b.txt"), []byte("bravo"), 0o644)
	}
	readBothFiles := func(ctx context.Context) (string, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return "", err
		}
		a, err := os.ReadFile(filepath.Join(wsPath, "file-a.txt"))
		if err != nil {
			return "", err
		}
		b, err := os.ReadFile(filepath.Join(wsPath, "file-b.txt"))
		if err != nil {
			return "", err
		}
		return string(a) + "+" + string(b), nil
	}

	// --- Worker ---
	snapshotFS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "fs"))
	driver := converter.NewLocalFileStoreDriver(storeDir)
	wsMgr := sdkworker.NewWorkspaceManager(snapshotFS, driver)

	w := sdkworker.New(s.SdkClient(), taskQueue, sdkworker.Options{
		WorkspaceManager: wsMgr,
	})
	w.RegisterWorkflowWithOptions(workspaceFn, workflow.RegisterOptions{Name: "WorkspaceV2MultiWorkflow"})
	w.RegisterActivityWithOptions(writeFileA, activity.RegisterOptions{Name: "WriteFileA"})
	w.RegisterActivityWithOptions(writeFileB, activity.RegisterOptions{Name: "WriteFileB"})
	w.RegisterActivityWithOptions(readBothFiles, activity.RegisterOptions{Name: "ReadBothFiles"})
	s.NoError(w.Start())
	defer w.Stop()

	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "WorkspaceV2MultiWorkflow")
	s.NoError(err)

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("alpha+bravo", result)
}

// TestWorkspaceV2_ContinueAsNew validates that workspace state persists across
// continue-as-new boundaries. The workflow writes files in a loop, does
// continue-as-new after 2 iterations, then reads all files in the new run.
func (s *WorkspaceV2Suite) TestWorkspaceV2_ContinueAsNew() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := testcore.RandomizeStr("ws-v2-can-tq")
	workflowID := testcore.RandomizeStr("ws-v2-can-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")

	type canInput struct {
		WorkspaceID string
		Iteration   int
		MaxPerRun   int
		TotalWrites int
	}

	// --- Workflow ---
	workspaceFn := func(ctx workflow.Context, input canInput) (string, error) {
		if input.WorkspaceID == "" {
			input.WorkspaceID = workflowID + "-ws"
		}
		if input.MaxPerRun == 0 {
			input.MaxPerRun = 2
		}
		if input.TotalWrites == 0 {
			input.TotalWrites = 3
		}

		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: input.WorkspaceID},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		writesThisRun := 0
		for input.Iteration < input.TotalWrites && writesThisRun < input.MaxPerRun {
			filename := fmt.Sprintf("file-%03d.txt", input.Iteration)
			content := fmt.Sprintf("v%d", input.Iteration)
			if err := workflow.ExecuteActivity(ctx, "WriteNamedFile", filename, content).Get(ctx, nil); err != nil {
				return "", err
			}
			input.Iteration++
			writesThisRun++
		}

		if input.Iteration < input.TotalWrites {
			return "", workflow.NewContinueAsNewError(ctx, "WorkspaceV2CANWorkflow", input)
		}

		// All files written. Read them all back.
		var result string
		if err := workflow.ExecuteActivity(ctx, "ReadAllCANFiles", input.TotalWrites).Get(ctx, &result); err != nil {
			return "", err
		}
		return result, nil
	}

	writeNamedFile := func(ctx context.Context, filename, content string) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(wsPath, filename), []byte(content), 0o644)
	}

	readAllCANFiles := func(ctx context.Context, count int) (string, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return "", err
		}
		var parts []string
		for i := 0; i < count; i++ {
			data, err := os.ReadFile(filepath.Join(wsPath, fmt.Sprintf("file-%03d.txt", i)))
			if err != nil {
				return "", fmt.Errorf("read file-%03d.txt: %w", i, err)
			}
			parts = append(parts, string(data))
		}
		return strings.Join(parts, ","), nil
	}

	// --- Worker ---
	snapshotFS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "fs"))
	driver := converter.NewLocalFileStoreDriver(storeDir)
	wsMgr := sdkworker.NewWorkspaceManager(snapshotFS, driver)

	w := sdkworker.New(s.SdkClient(), taskQueue, sdkworker.Options{
		WorkspaceManager: wsMgr,
	})
	w.RegisterWorkflowWithOptions(workspaceFn, workflow.RegisterOptions{Name: "WorkspaceV2CANWorkflow"})
	w.RegisterActivityWithOptions(writeNamedFile, activity.RegisterOptions{Name: "WriteNamedFile"})
	w.RegisterActivityWithOptions(readAllCANFiles, activity.RegisterOptions{Name: "ReadAllCANFiles"})
	s.NoError(w.Start())
	defer w.Stop()

	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "WorkspaceV2CANWorkflow", canInput{
		TotalWrites: 3,
		MaxPerRun:   2, // Will CAN after 2 writes, then write 1 more + read all.
	})
	s.NoError(err)

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("v0,v1,v2", result)
}

// TestWorkspaceV2_ChildWorkflow validates that a child workflow on a DIFFERENT
// task queue (with a separate worker and separate local filesystem) can access
// the parent's workspace. This proves the child reconstructs the workspace from
// external storage via the V2 CHASM standalone, not from the parent worker's
// local filesystem.
//
// Flow:
//  1. Worker-1 (parentTQ) runs parent workflow → writes "workspace-shared-secret"
//  2. Parent starts child on childTQ with WorkspaceTransfer
//  3. Server syncs parent's workspace to standalone CHASM during child start
//  4. Worker-2 (childTQ, completely separate filesystem) runs child workflow
//  5. Child's EnsureWorkspaceForActivity reads from standalone CHASM
//  6. Child's worker reconstructs workspace from external storage diffs
//  7. Child reads the file and returns its content
func (s *WorkspaceV2Suite) TestWorkspaceV2_ChildWorkflow() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	parentTQ := testcore.RandomizeStr("ws-v2-parent-tq")
	childTQ := testcore.RandomizeStr("ws-v2-child-tq")
	workflowID := testcore.RandomizeStr("ws-v2-child-wf")
	// Shared external storage — both workers use the same store (like S3).
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")

	wsID := workflowID + "-ws"

	type childInput struct {
		WorkspaceID string
	}

	// --- Parent Workflow ---
	parentFn := func(ctx workflow.Context) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: wsID},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		if err := workflow.ExecuteActivity(ctx, "WriteSecret").Get(ctx, nil); err != nil {
			return "", fmt.Errorf("WriteSecret: %w", err)
		}

		// Start child on a DIFFERENT task queue with workspace grant.
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:                childTQ,
			WorkflowExecutionTimeout: 30 * time.Second,
			WorkspaceTransfers: []sdkworker.WorkspaceTransfer{
				{SourceWorkspaceID: wsID},
			},
		})
		var childResult string
		if err := workflow.ExecuteChildWorkflow(childCtx, "WorkspaceV2ChildReader", childInput{
			WorkspaceID: wsID,
		}).Get(ctx, &childResult); err != nil {
			return "", fmt.Errorf("child: %w", err)
		}
		return childResult, nil
	}

	// --- Child Workflow ---
	childFn := func(ctx workflow.Context, input childInput) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: input.WorkspaceID},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var result string
		if err := workflow.ExecuteActivity(ctx, "ReadSecret").Get(ctx, &result); err != nil {
			return "", fmt.Errorf("ReadSecret: %w", err)
		}
		return result, nil
	}

	// --- Activities ---
	writeSecret := func(ctx context.Context) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(wsPath, "secret.txt"), []byte("workspace-shared-secret"), 0o644)
	}

	readSecret := func(ctx context.Context) (string, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return "", err
		}
		data, err := os.ReadFile(filepath.Join(wsPath, "secret.txt"))
		if err != nil {
			return "", fmt.Errorf("read secret.txt: %w", err)
		}
		return string(data), nil
	}

	// --- Worker 1 (parent): its own isolated local filesystem ---
	w1FS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "worker1-fs"))
	w1Driver := converter.NewLocalFileStoreDriver(storeDir) // shared external store
	w1Mgr := sdkworker.NewWorkspaceManager(w1FS, w1Driver)

	w1 := sdkworker.New(s.SdkClient(), parentTQ, sdkworker.Options{
		WorkspaceManager: w1Mgr,
	})
	w1.RegisterWorkflowWithOptions(parentFn, workflow.RegisterOptions{Name: "WorkspaceV2ParentWriter"})
	w1.RegisterActivityWithOptions(writeSecret, activity.RegisterOptions{Name: "WriteSecret"})
	s.NoError(w1.Start())
	defer w1.Stop()

	// --- Worker 2 (child): completely separate local filesystem ---
	w2FS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "worker2-fs"))
	w2Driver := converter.NewLocalFileStoreDriver(storeDir) // same shared external store
	w2Mgr := sdkworker.NewWorkspaceManager(w2FS, w2Driver)

	w2 := sdkworker.New(s.SdkClient(), childTQ, sdkworker.Options{
		WorkspaceManager: w2Mgr,
	})
	w2.RegisterWorkflowWithOptions(childFn, workflow.RegisterOptions{Name: "WorkspaceV2ChildReader"})
	w2.RegisterActivityWithOptions(readSecret, activity.RegisterOptions{Name: "ReadSecret"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// --- Execute ---
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                parentTQ,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "WorkspaceV2ParentWriter")
	s.NoError(err)

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("workspace-shared-secret", result)
}

// TestWorkspaceV2_ForkWorkspace validates copy-on-fork semantics:
//  1. Parent writes file1 to workspace
//  2. Parent starts child (fork/copy mode) on a different task queue + worker
//  3. Child waits on a signal (blocked — hasn't read workspace yet)
//  4. Parent writes file2 to workspace AFTER the fork
//  5. Parent signals child to proceed
//  6. Child reads workspace — should see file1 (present at fork time) but NOT file2
//
// This proves that a forked workspace is a snapshot frozen at fork time.
// The parent and child evolve independently after the fork.
func (s *WorkspaceV2Suite) TestWorkspaceV2_ForkWorkspace() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	parentTQ := testcore.RandomizeStr("ws-v2-fork-parent-tq")
	childTQ := testcore.RandomizeStr("ws-v2-fork-child-tq")
	workflowID := testcore.RandomizeStr("ws-v2-fork-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")

	wsID := workflowID + "-ws"
	const signalProceed = "proceed"

	type forkChildInput struct {
		WorkspaceID string
	}

	type forkResult struct {
		File1Present bool   `json:"file1_present"`
		File1Content string `json:"file1_content"`
		File2Present bool   `json:"file2_present"`
	}

	// --- Parent Workflow ---
	parentFn := func(ctx workflow.Context) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: wsID},
		}
		actCtx := workflow.WithActivityOptions(ctx, ao)

		// Step 1: Write file1.
		if err := workflow.ExecuteActivity(actCtx, "WriteFile1").Get(ctx, nil); err != nil {
			return "", fmt.Errorf("WriteFile1: %w", err)
		}

		// Step 2: Start child (fork) on a different task queue.
		// The child gets a copy of the workspace at this point (file1 exists).
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:                childTQ,
			WorkflowExecutionTimeout: 30 * time.Second,
			WorkspaceTransfers: []sdkworker.WorkspaceTransfer{
				{
					SourceWorkspaceID: wsID,
					Mode:              enumspb.WORKSPACE_TRANSFER_MODE_FORK,
					TargetWorkspaceID: wsID + "/fork",
				},
			},
		})
		// Child uses the FORKED workspace ID — ForkWorkspace creates a new
		// standalone at the target ID with the parent's diffs at fork time.
		childFuture := workflow.ExecuteChildWorkflow(childCtx, "WorkspaceV2ForkChild", forkChildInput{
			WorkspaceID: wsID + "/fork",
		})

		// Step 3: Write file2 AFTER starting the child.
		// The child should NOT see this because the fork already happened.
		if err := workflow.ExecuteActivity(actCtx, "WriteFile2").Get(ctx, nil); err != nil {
			return "", fmt.Errorf("WriteFile2: %w", err)
		}

		// Step 4: Signal child to proceed and read its workspace.
		if err := childFuture.SignalChildWorkflow(ctx, signalProceed, nil).Get(ctx, nil); err != nil {
			return "", fmt.Errorf("signal child: %w", err)
		}

		// Step 5: Wait for child result.
		var childResult forkResult
		if err := childFuture.Get(ctx, &childResult); err != nil {
			return "", fmt.Errorf("child: %w", err)
		}

		// Encode result as string for easy assertion.
		if childResult.File1Present && !childResult.File2Present {
			return "fork-correct:" + childResult.File1Content, nil
		}
		return fmt.Sprintf("fork-wrong:file1=%v,file2=%v", childResult.File1Present, childResult.File2Present), nil
	}

	// --- Child Workflow (fork recipient) ---
	childFn := func(ctx workflow.Context, input forkChildInput) (forkResult, error) {
		// Wait for signal before reading — gives parent time to write file2.
		workflow.GetSignalChannel(ctx, signalProceed).Receive(ctx, nil)

		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: input.WorkspaceID},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var result forkResult
		if err := workflow.ExecuteActivity(ctx, "CheckForkFiles").Get(ctx, &result); err != nil {
			return forkResult{}, fmt.Errorf("CheckForkFiles: %w", err)
		}
		return result, nil
	}

	// --- Activities ---
	writeFile1 := func(ctx context.Context) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(wsPath, "file1.txt"), []byte("before-fork"), 0o644)
	}

	writeFile2 := func(ctx context.Context) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(wsPath, "file2.txt"), []byte("after-fork"), 0o644)
	}

	checkForkFiles := func(ctx context.Context) (forkResult, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return forkResult{}, err
		}
		var result forkResult
		if data, err := os.ReadFile(filepath.Join(wsPath, "file1.txt")); err == nil {
			result.File1Present = true
			result.File1Content = string(data)
		}
		if _, err := os.Stat(filepath.Join(wsPath, "file2.txt")); err == nil {
			result.File2Present = true
		}
		return result, nil
	}

	// --- Worker 1 (parent): own filesystem ---
	w1FS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "worker1-fs"))
	w1Mgr := sdkworker.NewWorkspaceManager(w1FS, converter.NewLocalFileStoreDriver(storeDir))

	w1 := sdkworker.New(s.SdkClient(), parentTQ, sdkworker.Options{WorkspaceManager: w1Mgr})
	w1.RegisterWorkflowWithOptions(parentFn, workflow.RegisterOptions{Name: "WorkspaceV2ForkParent"})
	w1.RegisterActivityWithOptions(writeFile1, activity.RegisterOptions{Name: "WriteFile1"})
	w1.RegisterActivityWithOptions(writeFile2, activity.RegisterOptions{Name: "WriteFile2"})
	s.NoError(w1.Start())
	defer w1.Stop()

	// --- Worker 2 (child): completely separate filesystem ---
	w2FS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "worker2-fs"))
	w2Mgr := sdkworker.NewWorkspaceManager(w2FS, converter.NewLocalFileStoreDriver(storeDir))

	w2 := sdkworker.New(s.SdkClient(), childTQ, sdkworker.Options{WorkspaceManager: w2Mgr})
	w2.RegisterWorkflowWithOptions(childFn, workflow.RegisterOptions{Name: "WorkspaceV2ForkChild"})
	w2.RegisterActivityWithOptions(checkForkFiles, activity.RegisterOptions{Name: "CheckForkFiles"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// --- Execute ---
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                parentTQ,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "WorkspaceV2ForkParent")
	s.NoError(err)

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("fork-correct:before-fork", result)
}

// TestWorkspaceV2_WriteHandoff validates passing write access to a child workflow:
//  1. Parent writes file1 to workspace
//  2. Parent starts child with write access (workspace synced to standalone)
//  3. Child writes file2 to the same workspace
//  4. Child completes (workspace synced back to standalone on close)
//  5. Parent re-acquires write access (reads latest version from standalone)
//  6. Parent reads workspace — should see BOTH file1 AND file2
func (s *WorkspaceV2Suite) TestWorkspaceV2_WriteHandoff() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	parentTQ := testcore.RandomizeStr("ws-v2-handoff-parent-tq")
	childTQ := testcore.RandomizeStr("ws-v2-handoff-child-tq")
	workflowID := testcore.RandomizeStr("ws-v2-handoff-wf")
	storeDir := filepath.Join(s.T().TempDir(), "workspace-store")

	wsID := workflowID + "-ws"

	type childInput struct {
		WorkspaceID string
	}

	// --- Parent Workflow ---
	parentFn := func(ctx workflow.Context) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: wsID},
		}
		actCtx := workflow.WithActivityOptions(ctx, ao)

		// Step 1: Write file1.
		if err := workflow.ExecuteActivity(actCtx, "WriteFile1").Get(ctx, nil); err != nil {
			return "", fmt.Errorf("WriteFile1: %w", err)
		}

		// Step 2: Start child with write access on different task queue.
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			TaskQueue:                childTQ,
			WorkflowExecutionTimeout: 30 * time.Second,
			WorkspaceTransfers: []sdkworker.WorkspaceTransfer{
				{SourceWorkspaceID: wsID, Mode: enumspb.WORKSPACE_TRANSFER_MODE_HANDOFF},
			},
		})
		if err := workflow.ExecuteChildWorkflow(childCtx, "WorkspaceV2HandoffChild", childInput{
			WorkspaceID: wsID,
		}).Get(ctx, nil); err != nil {
			return "", fmt.Errorf("child: %w", err)
		}

		// Step 3: Re-acquire write access and verify both files are visible.
		var result string
		if err := workflow.ExecuteActivity(actCtx, "ReadBothHandoffFiles").Get(ctx, &result); err != nil {
			return "", fmt.Errorf("ReadBothHandoffFiles: %w", err)
		}
		return result, nil
	}

	// --- Child Workflow (write recipient) ---
	childFn := func(ctx workflow.Context, input childInput) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			WorkspaceOptions:    &sdkworker.WorkspaceOptions{ID: input.WorkspaceID},
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		// Write file2 to the workspace.
		return workflow.ExecuteActivity(ctx, "WriteFile2").Get(ctx, nil)
	}

	// --- Activities ---
	writeFile1 := func(ctx context.Context) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(wsPath, "file1.txt"), []byte("parent-wrote"), 0o644)
	}

	writeFile2 := func(ctx context.Context) error {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return err
		}
		// Verify file1 exists (inherited from parent).
		if _, err := os.Stat(filepath.Join(wsPath, "file1.txt")); err != nil {
			return fmt.Errorf("file1.txt should exist from parent: %w", err)
		}
		return os.WriteFile(filepath.Join(wsPath, "file2.txt"), []byte("child-wrote"), 0o644)
	}

	readBothHandoffFiles := func(ctx context.Context) (string, error) {
		wsPath, err := activity.GetWorkspacePath(ctx)
		if err != nil {
			return "", err
		}
		f1, err := os.ReadFile(filepath.Join(wsPath, "file1.txt"))
		if err != nil {
			return "", fmt.Errorf("read file1.txt: %w", err)
		}
		f2, err := os.ReadFile(filepath.Join(wsPath, "file2.txt"))
		if err != nil {
			return "", fmt.Errorf("read file2.txt: %w", err)
		}
		return string(f1) + "+" + string(f2), nil
	}

	// --- Worker 1 (parent): own filesystem ---
	w1FS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "worker1-fs"))
	w1Mgr := sdkworker.NewWorkspaceManager(w1FS, converter.NewLocalFileStoreDriver(storeDir))

	w1 := sdkworker.New(s.SdkClient(), parentTQ, sdkworker.Options{WorkspaceManager: w1Mgr})
	w1.RegisterWorkflowWithOptions(parentFn, workflow.RegisterOptions{Name: "WorkspaceV2HandoffParent"})
	w1.RegisterActivityWithOptions(writeFile1, activity.RegisterOptions{Name: "WriteFile1"})
	w1.RegisterActivityWithOptions(readBothHandoffFiles, activity.RegisterOptions{Name: "ReadBothHandoffFiles"})
	s.NoError(w1.Start())
	defer w1.Stop()

	// --- Worker 2 (child): completely separate filesystem ---
	w2FS := sdkworker.NewTarDiffFS(filepath.Join(s.T().TempDir(), "worker2-fs"))
	w2Mgr := sdkworker.NewWorkspaceManager(w2FS, converter.NewLocalFileStoreDriver(storeDir))

	w2 := sdkworker.New(s.SdkClient(), childTQ, sdkworker.Options{WorkspaceManager: w2Mgr})
	w2.RegisterWorkflowWithOptions(childFn, workflow.RegisterOptions{Name: "WorkspaceV2HandoffChild"})
	w2.RegisterActivityWithOptions(writeFile2, activity.RegisterOptions{Name: "WriteFile2"})
	s.NoError(w2.Start())
	defer w2.Stop()

	// --- Execute ---
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                parentTQ,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "WorkspaceV2HandoffParent")
	s.NoError(err)

	var result string
	s.NoError(run.Get(ctx, &result))
	s.Equal("parent-wrote+child-wrote", result)
}

