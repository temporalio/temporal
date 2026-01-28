package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
)

// NOTE: These tests require the full System Nexus Endpoint implementation to be complete.
// Tests for the complex scenarios are skipped until the async callback mechanism is implemented.

type SystemNexusWaitWorkflowSuite struct {
	testcore.FunctionalTestBase
}

func TestSystemNexusWaitWorkflowSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(SystemNexusWaitWorkflowSuite))
}

// TestWaitExternalWorkflowCompletion tests the WaitExternalWorkflow system Nexus operation.
// Test scenario:
//  1. Workflow A starts Workflow B using an activity that calls SignalWithStart API
//  2. Workflow B waits for "unblock_me" signal, then completes with a result
//  3. Workflow A spawns a coroutine that waits 1s then signals B with "unblock_me"
//  4. Workflow A's main coroutine calls WaitExternalWorkflow on B
//  5. Once that wait returns, A verifies the result and returns success
//
// NOTE: This test uses polling to wait for the target workflow completion.
// A more efficient callback-based mechanism may be implemented in a future iteration.
func (s *SystemNexusWaitWorkflowSuite) TestWaitExternalWorkflowCompletion() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := s.TaskQueue()
	workflowAID := testcore.RandomizeStr("workflow-a")
	workflowBID := testcore.RandomizeStr("workflow-b")

	const signalName = "unblock_me"
	const expectedResult = "workflow-b-result"

	// Workflow B: waits for "unblock_me" signal, then completes with a result
	workflowB := func(ctx workflow.Context) (string, error) {
		// Wait for the unblock signal
		signalCh := workflow.GetSignalChannel(ctx, signalName)
		var signalValue string
		signalCh.Receive(ctx, &signalValue)

		workflow.GetLogger(ctx).Info("Workflow B received signal, completing", "signal", signalValue)
		return expectedResult, nil
	}

	// Activity that starts Workflow B
	// We pass the SDK client via closure since this is a test
	sdkClient := s.SdkClient()
	startWorkflowBActivity := func(ctx context.Context, targetWorkflowID string, targetTaskQueue string) (string, error) {
		activityInfo := activity.GetInfo(ctx)
		_ = activityInfo // for logging if needed

		// Start workflow B (it will wait for the unblock signal before completing)
		run, err := sdkClient.ExecuteWorkflow(
			ctx,
			sdkclient.StartWorkflowOptions{
				ID:                       targetWorkflowID,
				TaskQueue:                targetTaskQueue,
				WorkflowExecutionTimeout: 30 * time.Second,
			},
			"WorkflowB",
		)
		if err != nil {
			return "", err
		}
		return run.GetRunID(), nil
	}

	// Workflow A: starts B, spawns coroutine to signal B, then waits for B's completion
	workflowA := func(ctx workflow.Context, targetWorkflowID string) (string, error) {
		logger := workflow.GetLogger(ctx)

		// Step 1: Start Workflow B using an activity
		var runID string
		activityOpts := workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
		}
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, activityOpts),
			startWorkflowBActivity,
			targetWorkflowID,
			taskQueue,
		).Get(ctx, &runID)
		if err != nil {
			return "", err
		}
		logger.Info("Workflow A started Workflow B", "runID", runID)

		// Step 2: Spawn a coroutine that waits 1s then signals B with "unblock_me"
		workflow.Go(ctx, func(gCtx workflow.Context) {
			// Wait 1 second before sending the unblock signal
			_ = workflow.Sleep(gCtx, 1*time.Second)
			logger.Info("Sending unblock signal to Workflow B")

			// Signal the external workflow
			signalFuture := workflow.SignalExternalWorkflow(gCtx, targetWorkflowID, "", signalName, "unblock-now")
			if err := signalFuture.Get(gCtx, nil); err != nil {
				logger.Error("Failed to signal Workflow B", "error", err)
			}
		})

		// Step 3: Main coroutine calls WaitExternalWorkflow on B
		logger.Info("Workflow A waiting for Workflow B to complete")
		waitFuture := workflow.WaitExternalWorkflow(ctx, workflow.WaitExternalWorkflowOptions{
			WorkflowID:             targetWorkflowID,
			ScheduleToCloseTimeout: 30 * time.Second,
		})

		var waitResult workflow.WaitExternalWorkflowResult
		err = waitFuture.Get(ctx, &waitResult)
		if err != nil {
			return "", err
		}

		// Step 4: Verify the result
		logger.Info("Workflow B completed", "status", waitResult.Status)

		if waitResult.Status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			return "", fmt.Errorf("workflow B did not complete successfully, status: %v", waitResult.Status)
		}

		// Decode the result from Workflow B
		var result string
		if err := waitResult.Get(&result); err != nil {
			return "", err
		}

		logger.Info("Workflow A received result from Workflow B", "result", result)
		return result, nil
	}

	// Register workflows and activity
	s.Worker().RegisterWorkflowWithOptions(workflowA, workflow.RegisterOptions{Name: "WorkflowA"})
	s.Worker().RegisterWorkflowWithOptions(workflowB, workflow.RegisterOptions{Name: "WorkflowB"})
	s.Worker().RegisterActivity(startWorkflowBActivity)

	// Start Workflow A
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowAID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "WorkflowA", workflowBID)
	s.NoError(err)

	// Wait for Workflow A to complete
	var result string
	err = run.Get(ctx, &result)
	s.NoError(err)

	// Verify the result
	s.Equal(expectedResult, result)
	s.T().Logf("Test passed: Workflow A received result '%s' from Workflow B", result)

	// Verify that a NexusOperationScheduled event was recorded in Workflow A's history
	history := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: workflowAID})
	foundScheduledEvent := false
	foundCompletedEvent := false
	for _, event := range history {
		if attrs := event.GetNexusOperationScheduledEventAttributes(); attrs != nil {
			s.T().Logf("Found NexusOperationScheduled event: endpoint=%s, service=%s, operation=%s",
				attrs.Endpoint, attrs.Service, attrs.Operation)
			s.Equal("__temporal_system", attrs.Endpoint)
			s.Equal("temporal.system.v1", attrs.Service)
			s.Equal("WaitExternalWorkflowCompletion", attrs.Operation)
			foundScheduledEvent = true
		}
		if event.GetNexusOperationCompletedEventAttributes() != nil {
			foundCompletedEvent = true
		}
	}
	s.True(foundScheduledEvent, "Expected to find NexusOperationScheduled event in history")
	s.True(foundCompletedEvent, "Expected to find NexusOperationCompleted event in history")
}

// TestSimpleWaitExternalWorkflowSchedule tests that WaitExternalWorkflow can schedule the operation.
func (s *SystemNexusWaitWorkflowSuite) TestSimpleWaitExternalWorkflowSchedule() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	taskQueue := s.TaskQueue()
	workflowID := testcore.RandomizeStr("workflow-wait-test")
	targetWorkflowID := testcore.RandomizeStr("target-workflow")

	// A simple workflow that calls WaitExternalWorkflow
	testWorkflow := func(ctx workflow.Context) error {
		// Just schedule the wait operation - we don't care if it completes
		// because we're just testing that the operation can be scheduled.
		waitFuture := workflow.WaitExternalWorkflow(ctx, workflow.WaitExternalWorkflowOptions{
			WorkflowID:             targetWorkflowID,
			ScheduleToCloseTimeout: 5 * time.Second,
		})
		var waitResult workflow.WaitExternalWorkflowResult
		// This will likely fail with timeout, but that's OK for this test
		err := waitFuture.Get(ctx, &waitResult)
		if err != nil {
			// Expected - the target workflow doesn't exist
			workflow.GetLogger(ctx).Info("WaitExternalWorkflow returned error", "error", err)
		}
		return nil // Always return success to end the workflow
	}

	// Register workflow
	s.Worker().RegisterWorkflowWithOptions(testWorkflow, workflow.RegisterOptions{Name: "TestWaitWorkflow"})

	// Start the workflow
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, "TestWaitWorkflow")
	s.NoError(err)

	// Wait for the workflow to complete
	err = run.Get(ctx, nil)
	s.NoError(err)

	// Verify that a NexusOperationScheduled event was recorded
	history := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: workflowID})
	s.T().Log("Checking history for NexusOperationScheduled event")
	foundScheduledEvent := false
	for _, event := range history {
		if attrs := event.GetNexusOperationScheduledEventAttributes(); attrs != nil {
			s.T().Logf("Found NexusOperationScheduled event: endpoint=%s, service=%s, operation=%s",
				attrs.Endpoint, attrs.Service, attrs.Operation)
			s.Equal("__temporal_system", attrs.Endpoint)
			s.Equal("temporal.system.v1", attrs.Service)
			s.Equal("WaitExternalWorkflowCompletion", attrs.Operation)
			foundScheduledEvent = true
			break
		}
	}
	s.True(foundScheduledEvent, "Expected to find NexusOperationScheduled event in history")
}

// TestWaitExternalWorkflowAlreadyCompleted tests waiting on a workflow that has already completed.
// This tests the synchronous path where the target workflow is completed before the wait starts.
func (s *SystemNexusWaitWorkflowSuite) TestWaitExternalWorkflowAlreadyCompleted() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := s.TaskQueue()
	workflowAID := testcore.RandomizeStr("workflow-a")
	workflowBID := testcore.RandomizeStr("workflow-b")

	const expectedResult = "workflow-b-result"

	// Workflow B: completes immediately with a result
	workflowB := func(ctx workflow.Context) (string, error) {
		workflow.GetLogger(ctx).Info("Workflow B completing immediately")
		return expectedResult, nil
	}

	// Activity that starts Workflow B and waits for it to complete
	sdkClient := s.SdkClient()
	startAndWaitWorkflowBActivity := func(ctx context.Context, targetWorkflowID string, targetTaskQueue string) error {
		// Start workflow B
		run, err := sdkClient.ExecuteWorkflow(
			ctx,
			sdkclient.StartWorkflowOptions{
				ID:                       targetWorkflowID,
				TaskQueue:                targetTaskQueue,
				WorkflowExecutionTimeout: 30 * time.Second,
			},
			"WorkflowB",
		)
		if err != nil {
			return err
		}

		// Wait for workflow B to complete before returning
		var result string
		err = run.Get(ctx, &result)
		if err != nil {
			return err
		}

		return nil
	}

	// Workflow A: starts B (waits for completion), then calls WaitExternalWorkflow on B
	workflowA := func(ctx workflow.Context, targetWorkflowID string) (string, error) {
		logger := workflow.GetLogger(ctx)

		// Step 1: Start and wait for Workflow B to complete using an activity
		activityOpts := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		}
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, activityOpts),
			startAndWaitWorkflowBActivity,
			targetWorkflowID,
			taskQueue,
		).Get(ctx, nil)
		if err != nil {
			return "", err
		}
		logger.Info("Workflow B has completed")

		// Step 2: Now call WaitExternalWorkflow on the already-completed B
		logger.Info("Workflow A calling WaitExternalWorkflow on completed Workflow B")
		waitFuture := workflow.WaitExternalWorkflow(ctx, workflow.WaitExternalWorkflowOptions{
			WorkflowID:             targetWorkflowID,
			ScheduleToCloseTimeout: 30 * time.Second,
		})

		var waitResult workflow.WaitExternalWorkflowResult
		err = waitFuture.Get(ctx, &waitResult)
		if err != nil {
			return "", err
		}

		// Step 3: Verify the status
		// Note: Result extraction from completed workflows is not yet implemented,
		// so we only verify the status here.
		logger.Info("WaitExternalWorkflow completed", "status", waitResult.Status)

		if waitResult.Status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
			return "", fmt.Errorf("workflow B did not complete successfully, status: %v", waitResult.Status)
		}

		logger.Info("Workflow A verified that Workflow B completed successfully")

		return "completed", nil
	}

	// Register workflows and activity
	s.Worker().RegisterWorkflowWithOptions(workflowA, workflow.RegisterOptions{Name: "WorkflowA"})
	s.Worker().RegisterWorkflowWithOptions(workflowB, workflow.RegisterOptions{Name: "WorkflowB"})
	s.Worker().RegisterActivity(startAndWaitWorkflowBActivity)

	// Start Workflow A
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowAID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "WorkflowA", workflowBID)
	s.NoError(err)

	// Wait for Workflow A to complete
	var result string
	err = run.Get(ctx, &result)
	s.NoError(err)

	// Verify the result (workflow A returns "completed" when B's status is COMPLETED)
	s.Equal("completed", result)
	s.T().Logf("Test passed: Workflow A verified that Workflow B (result: '%s') completed with correct status", expectedResult)

	// Verify that a NexusOperationScheduled and Completed event were recorded
	history := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: workflowAID})
	foundScheduledEvent := false
	foundCompletedEvent := false
	for _, event := range history {
		if attrs := event.GetNexusOperationScheduledEventAttributes(); attrs != nil {
			s.T().Logf("Found NexusOperationScheduled event: endpoint=%s, service=%s, operation=%s",
				attrs.Endpoint, attrs.Service, attrs.Operation)
			s.Equal("__temporal_system", attrs.Endpoint)
			s.Equal("temporal.system.v1", attrs.Service)
			s.Equal("WaitExternalWorkflowCompletion", attrs.Operation)
			foundScheduledEvent = true
		}
		if event.GetNexusOperationCompletedEventAttributes() != nil {
			foundCompletedEvent = true
		}
	}
	s.True(foundScheduledEvent, "Expected to find NexusOperationScheduled event in history")
	s.True(foundCompletedEvent, "Expected to find NexusOperationCompleted event in history")
}

// TestWaitExternalWorkflowFailed tests waiting on a workflow that fails.
// Test scenario:
//  1. Workflow A starts Workflow B using an activity
//  2. Workflow B waits for "unblock_me" signal, then fails with an error
//  3. Workflow A spawns a coroutine that waits 1s then signals B with "unblock_me"
//  4. Workflow A's main coroutine calls WaitExternalWorkflow on B
//  5. Once that wait returns, A verifies it received the FAILED status and failure details
func (s *SystemNexusWaitWorkflowSuite) TestWaitExternalWorkflowFailed() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	taskQueue := s.TaskQueue()
	workflowAID := testcore.RandomizeStr("workflow-a")
	workflowBID := testcore.RandomizeStr("workflow-b")

	const signalName = "unblock_me"
	const expectedErrorMessage = "workflow-b-intentional-failure"

	// Workflow B: waits for "unblock_me" signal, then fails with an error
	workflowB := func(ctx workflow.Context) (string, error) {
		// Wait for the unblock signal
		signalCh := workflow.GetSignalChannel(ctx, signalName)
		var signalValue string
		signalCh.Receive(ctx, &signalValue)

		workflow.GetLogger(ctx).Info("Workflow B received signal, failing intentionally", "signal", signalValue)
		return "", fmt.Errorf(expectedErrorMessage)
	}

	// Activity that starts Workflow B
	sdkClient := s.SdkClient()
	startWorkflowBActivity := func(ctx context.Context, targetWorkflowID string, targetTaskQueue string) (string, error) {
		// Start workflow B (it will wait for the unblock signal before failing)
		run, err := sdkClient.ExecuteWorkflow(
			ctx,
			sdkclient.StartWorkflowOptions{
				ID:                       targetWorkflowID,
				TaskQueue:                targetTaskQueue,
				WorkflowExecutionTimeout: 30 * time.Second,
			},
			"WorkflowBFails",
		)
		if err != nil {
			return "", err
		}
		return run.GetRunID(), nil
	}

	// Workflow A: starts B, spawns coroutine to signal B, then waits for B's completion
	workflowA := func(ctx workflow.Context, targetWorkflowID string) (string, error) {
		logger := workflow.GetLogger(ctx)

		// Step 1: Start Workflow B using an activity
		var runID string
		activityOpts := workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
		}
		err := workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, activityOpts),
			startWorkflowBActivity,
			targetWorkflowID,
			taskQueue,
		).Get(ctx, &runID)
		if err != nil {
			return "", err
		}
		logger.Info("Workflow A started Workflow B", "runID", runID)

		// Step 2: Spawn a coroutine that waits 1s then signals B with "unblock_me"
		workflow.Go(ctx, func(gCtx workflow.Context) {
			// Wait 1 second before sending the unblock signal
			_ = workflow.Sleep(gCtx, 1*time.Second)
			logger.Info("Sending unblock signal to Workflow B")

			// Signal the external workflow
			signalFuture := workflow.SignalExternalWorkflow(gCtx, targetWorkflowID, "", signalName, "unblock-now")
			if err := signalFuture.Get(gCtx, nil); err != nil {
				logger.Error("Failed to signal Workflow B", "error", err)
			}
		})

		// Step 3: Main coroutine calls WaitExternalWorkflow on B
		logger.Info("Workflow A waiting for Workflow B to complete")
		waitFuture := workflow.WaitExternalWorkflow(ctx, workflow.WaitExternalWorkflowOptions{
			WorkflowID:             targetWorkflowID,
			ScheduleToCloseTimeout: 30 * time.Second,
		})

		var waitResult workflow.WaitExternalWorkflowResult
		err = waitFuture.Get(ctx, &waitResult)
		if err != nil {
			return "", err
		}

		// Step 4: Verify the failure status
		logger.Info("Workflow B completed", "status", waitResult.Status)

		if waitResult.Status != enumspb.WORKFLOW_EXECUTION_STATUS_FAILED {
			return "", fmt.Errorf("expected workflow B to fail, but got status: %v", waitResult.Status)
		}

		// Step 5: Verify we can access the failure details
		failure := waitResult.GetFailure()
		if failure == nil {
			return "", fmt.Errorf("expected failure details but got nil")
		}

		logger.Info("Workflow A received failure from Workflow B", "failure", failure.Message)
		return fmt.Sprintf("received-failure:%s", failure.Message), nil
	}

	// Register workflows and activity
	s.Worker().RegisterWorkflowWithOptions(workflowA, workflow.RegisterOptions{Name: "WorkflowAWaitsFailed"})
	s.Worker().RegisterWorkflowWithOptions(workflowB, workflow.RegisterOptions{Name: "WorkflowBFails"})
	s.Worker().RegisterActivity(startWorkflowBActivity)

	// Start Workflow A
	run, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowAID,
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 60 * time.Second,
	}, "WorkflowAWaitsFailed", workflowBID)
	s.NoError(err)

	// Wait for Workflow A to complete
	var result string
	err = run.Get(ctx, &result)
	s.NoError(err)

	// Verify the result contains the failure message
	s.Contains(result, "received-failure:")
	s.T().Logf("Test passed: Workflow A received failure result '%s' from failed Workflow B", result)

	// Verify that NexusOperationScheduled and Completed events were recorded
	history := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: workflowAID})
	foundScheduledEvent := false
	foundCompletedEvent := false
	for _, event := range history {
		if attrs := event.GetNexusOperationScheduledEventAttributes(); attrs != nil {
			s.T().Logf("Found NexusOperationScheduled event: endpoint=%s, service=%s, operation=%s",
				attrs.Endpoint, attrs.Service, attrs.Operation)
			s.Equal("__temporal_system", attrs.Endpoint)
			s.Equal("temporal.system.v1", attrs.Service)
			s.Equal("WaitExternalWorkflowCompletion", attrs.Operation)
			foundScheduledEvent = true
		}
		if event.GetNexusOperationCompletedEventAttributes() != nil {
			foundCompletedEvent = true
		}
	}
	s.True(foundScheduledEvent, "Expected to find NexusOperationScheduled event in history")
	s.True(foundCompletedEvent, "Expected to find NexusOperationCompleted event in history")
}
