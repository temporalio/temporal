package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
)

type UpdateWorkflowSDKSuite struct {
	testcore.FunctionalTestBase
}

func TestUpdateWorkflowSDKSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(UpdateWorkflowSDKSuite))
}

// NonDurableRejectWorkflow is the workflow that handles updates with validation
func NonDurableRejectWorkflow(ctx workflow.Context) error {
	// Register update handler with validator
	err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		"update-handler",
		func(ctx workflow.Context, arg string) (string, error) {
			// This is the handler - will only run if validator passes
			workflow.GetLogger(ctx).Info("Update handler executing", "arg", arg)
			return "update-result", nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, arg string) error {
				workflow.GetLogger(ctx).Info("Validating update", "arg", arg)
				// Reject updates with "reject" in the argument
				if arg == "reject-me" {
					workflow.GetLogger(ctx).Info("Rejecting update")
					return errors.New("Update rejected by validator")
				}
				workflow.GetLogger(ctx).Info("Update validated successfully")
				return nil
			},
		},
	)
	if err != nil {
		return err
	}

	// Wait for completion signal or timeout
	if err := workflow.Sleep(ctx, 30*time.Second); err != nil {
		return err
	}

	return nil
}

// TestNonDurableRejectWithSDK replicates features/update/non_durable_reject using SDK client and worker
// This is the closest match to the actual features test implementation
func (s *UpdateWorkflowSDKSuite) TestNonDurableRejectWithSDK() {
	ctx := context.Background()
	taskQueue := "update-sdk-test-tq"

	// Create SDK client
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
	defer sdkClient.Close()

	// Create and start worker
	w := worker.New(sdkClient, taskQueue, worker.Options{})
	w.RegisterWorkflow(NonDurableRejectWorkflow)
	err = w.Start()
	s.NoError(err)
	defer w.Stop()

	// Start workflow
	workflowID := "non-durable-reject-sdk-test"
	workflowRun, err := sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: 1 * time.Minute,
	}, NonDurableRejectWorkflow)
	s.NoError(err)
	s.T().Logf("Started workflow: %s, RunID: %s", workflowRun.GetID(), workflowRun.GetRunID())

	// Wait a moment for workflow to start
	time.Sleep(500 * time.Millisecond)

	// Send update that will be REJECTED by validator (non-durable rejection)
	s.T().Log("Sending update that will be rejected...")
	updateHandle, err := sdkClient.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		RunID:        workflowRun.GetRunID(),
		UpdateName:   "update-handler",
		Args:         []interface{}{"reject-me"},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})

	// The update should be rejected
	if err == nil {
		var result string
		err = updateHandle.Get(ctx, &result)
	}
	s.Error(err, "Update should be rejected by validator")
	s.Contains(err.Error(), "rejected", "Error should indicate rejection")
	s.T().Logf("Update rejected as expected: %v", err)

	// THIS IS THE CRITICAL TEST:
	// After the rejection, if we try to get the workflow history, the SDK worker internally
	// calls GetWorkflowExecutionHistory and rebuilds the replay state. If transient events
	// are missing, the SDK will encounter "premature end of stream" error.

	// Send a successful update to force the worker to process another workflow task
	// This will cause the SDK to fetch history and replay, exposing the bug if present
	s.T().Log("Sending successful update to trigger history fetch and replay...")
	updateHandle2, err := sdkClient.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		RunID:        workflowRun.GetRunID(),
		UpdateName:   "update-handler",
		Args:         []interface{}{"accept-me"},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	s.NoError(err, "Second update should be accepted")

	var result string
	err = updateHandle2.Get(ctx, &result)
	s.NoError(err, "Second update should complete successfully")
	s.Equal("update-result", result)
	s.T().Log("✅ Second update completed successfully - no 'premature end of stream' error!")

	// Terminate workflow to clean up
	err = sdkClient.TerminateWorkflow(ctx, workflowID, workflowRun.GetRunID(), "test complete")
	s.NoError(err)
}

// BasicUpdateWorkflow is a simpler workflow for basic update testing
func BasicUpdateWorkflow(ctx workflow.Context) error {
	// Register update handler
	err := workflow.SetUpdateHandler(
		ctx,
		"simple-update",
		func(ctx workflow.Context, arg string) (string, error) {
			workflow.GetLogger(ctx).Info("Processing update", "arg", arg)
			return "processed: " + arg, nil
		},
	)
	if err != nil {
		return err
	}

	// Wait
	if err := workflow.Sleep(ctx, 30*time.Second); err != nil {
		return err
	}

	return nil
}

// TestBasicUpdateWithSDK tests a basic update scenario using SDK
func (s *UpdateWorkflowSDKSuite) TestBasicUpdateWithSDK() {
	ctx := context.Background()
	taskQueue := "update-basic-sdk-test-tq"

	// Create SDK client
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
	defer sdkClient.Close()

	// Create and start worker
	w := worker.New(sdkClient, taskQueue, worker.Options{})
	w.RegisterWorkflow(BasicUpdateWorkflow)
	err = w.Start()
	s.NoError(err)
	defer w.Stop()

	// Start workflow
	workflowID := "basic-update-sdk-test"
	workflowRun, err := sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: 1 * time.Minute,
	}, BasicUpdateWorkflow)
	s.NoError(err)
	s.T().Logf("Started workflow: %s, RunID: %s", workflowRun.GetID(), workflowRun.GetRunID())

	// Wait a moment for workflow to start
	time.Sleep(500 * time.Millisecond)

	// Send update
	s.T().Log("Sending update...")
	updateHandle, err := sdkClient.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		RunID:        workflowRun.GetRunID(),
		UpdateName:   "simple-update",
		Args:         []interface{}{"test-arg"},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	s.NoError(err)

	var result string
	err = updateHandle.Get(ctx, &result)
	s.NoError(err)
	s.Equal("processed: test-arg", result)
	s.T().Log("✅ Update completed successfully")

	// Send another update to trigger cache eviction and history replay
	s.T().Log("Sending second update to trigger history replay...")
	updateHandle2, err := sdkClient.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		RunID:        workflowRun.GetRunID(),
		UpdateName:   "simple-update",
		Args:         []interface{}{"test-arg-2"},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	s.NoError(err)

	var result2 string
	err = updateHandle2.Get(ctx, &result2)
	s.NoError(err, "Second update should complete without 'premature end of stream' error")
	s.Equal("processed: test-arg-2", result2)
	s.T().Log("✅ Second update completed successfully - no replay errors!")

	// Terminate workflow
	err = sdkClient.TerminateWorkflow(ctx, workflowID, workflowRun.GetRunID(), "test complete")
	s.NoError(err)
}

// TestMultipleUpdatesWithSDK tests multiple updates in sequence
func (s *UpdateWorkflowSDKSuite) TestMultipleUpdatesWithSDK() {
	ctx := context.Background()
	taskQueue := "update-multiple-sdk-test-tq"

	// Create SDK client
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
	defer sdkClient.Close()

	// Create and start worker
	w := worker.New(sdkClient, taskQueue, worker.Options{})
	w.RegisterWorkflow(BasicUpdateWorkflow)
	err = w.Start()
	s.NoError(err)
	defer w.Stop()

	// Start workflow
	workflowID := "multiple-updates-sdk-test"
	workflowRun, err := sdkClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: 1 * time.Minute,
	}, BasicUpdateWorkflow)
	s.NoError(err)

	// Wait for workflow to start
	time.Sleep(500 * time.Millisecond)

	// Send multiple updates in sequence
	for i := 1; i <= 5; i++ {
		s.T().Logf("Sending update %d...", i)
		updateHandle, err := sdkClient.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
			WorkflowID:   workflowID,
			RunID:        workflowRun.GetRunID(),
			UpdateName:   "simple-update",
			Args:         []interface{}{string(rune('A' + i - 1))},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		})
		s.NoError(err, "Update %d should not fail", i)

		var result string
		err = updateHandle.Get(ctx, &result)
		s.NoError(err, "Update %d should complete without 'premature end of stream' error", i)
		s.T().Logf("Update %d result: %s", i, result)
	}

	s.T().Log("✅ All 5 updates completed successfully - no replay errors!")

	// Terminate workflow
	err = sdkClient.TerminateWorkflow(ctx, workflowID, workflowRun.GetRunID(), "test complete")
	s.NoError(err)
}
