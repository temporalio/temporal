package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/testcore"
)

type LostTaskTestSuite struct {
	testcore.FunctionalTestBase
}

func TestUmpireLostTaskTestSuite(t *testing.T) {
	// Enable OTEL debug mode to capture proto payloads in spans
	t.Setenv("TEMPORAL_OTEL_DEBUG", "true")
	suite.Run(t, new(LostTaskTestSuite))
}

func (s *LostTaskTestSuite) SetupSuite() {
	// Ensure OTEL debug mode is enabled
	os.Setenv("TEMPORAL_OTEL_DEBUG", "true")
	s.FunctionalTestBase.SetupSuite()
}

// TestLostTaskDetection verifies that umpire detects when a task is lost from matching.
// This test simulates a scenario where:
// 1. A workflow task is stored to persistence
// 2. The task is deleted from persistence via CompleteTasksLessThan
// 3. A worker polls the task queue and gets an empty response
// 4. Umpire detects that a task was stored but never successfully polled
func (s *LostTaskTestSuite) TestLostTaskDetection() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start a workflow without any workers
	workflowID := "umpire-lost-task-test"
	workflowType := "TestWorkflow"
	taskQueue := s.TaskQueue()

	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   workflowID,
		WorkflowType: &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
	})
	s.NoError(err)
	s.T().Logf("Workflow started: %s/%s on task queue %s", workflowID, startResp.GetRunId(), taskQueue)

	// Wait for at least one task to be created in persistence
	// Tasks may be spilled to persistence if matching service has memory pressure
	// or if there are no pollers available
	taskMgr := s.GetTestCluster().TestBase().TaskMgr
	var deleted int
	s.Eventually(func() bool {
		// Try to delete tasks from persistence
		var err error
		deleted, err = taskMgr.CompleteTasksLessThan(ctx, &persistence.CompleteTasksLessThanRequest{
			NamespaceID:        s.NamespaceID().String(),
			TaskQueueName:      taskQueue,
			TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			ExclusiveMaxTaskID: 999999999, // Delete all tasks
			Limit:              1000,      // Max number of tasks to delete
		})
		if err != nil {
			s.T().Logf("CompleteTasksLessThan error: %v", err)
			return false
		}
		s.T().Logf("Deleted %d tasks from persistence", deleted)
		return deleted > 0
	}, 10*time.Second, 100*time.Millisecond, "Expected at least one task to be stored in persistence")

	// Now poll the task queue - this should return empty since we deleted the task
	pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pollCancel()
	pollResp, err := s.FrontendClient().PollWorkflowTaskQueue(pollCtx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:  "test-worker",
	})
	s.NoError(err)
	s.T().Logf("Poll response: taskToken length = %d", len(pollResp.TaskToken))

	// Wait for umpire to detect the lost task
	// The model checks for tasks that were stored but never polled
	s.T().Logf("Waiting for umpire to detect lost task...")

	s.Eventually(func() bool {
		violations := s.GetUmpire().Check(ctx, true)
		if len(violations) > 0 {
			s.T().Logf("Umpire detected %d violation(s):", len(violations))
			for _, vi := range violations {
				if v, ok := vi.(umpire.Violation); ok {
					s.T().Logf("  [%s] %s - Tags: %v", v.Rule, v.Message, v.Tags)
				}
			}
			return true
		}
		s.T().Logf("Umpire Check returned 0 violations, waiting...")
		return false
	}, 30*time.Second, 1*time.Second, "Expected umpire to detect lost task")

	// Clean up
	_, err = s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      startResp.GetRunId(),
		},
		Reason: "test cleanup",
	})
	s.NoError(err)
}

// TestStuckWorkflowDetection verifies that umpire detects when a workflow is started but never completes.
// This test simulates a scenario where:
// 1. A workflow is started
// 2. The workflow never receives a RespondWorkflowTaskCompleted response
// 3. Umpire detects the workflow is stuck in the "started" state
func (s *LostTaskTestSuite) TestStuckWorkflowDetection() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start a workflow without any workers and without completing it
	workflowID := "umpire-stuck-workflow-test"
	workflowType := "TestWorkflow"
	taskQueue := s.TaskQueue()

	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   workflowID,
		WorkflowType: &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
	})
	s.NoError(err)
	s.T().Logf("Workflow started: %s/%s on task queue %s", workflowID, startResp.GetRunId(), taskQueue)

	// Wait for umpire to detect the stuck workflow
	// The model checks for workflows that were started but never completed
	s.T().Logf("Waiting for umpire to detect stuck workflow...")

	s.Eventually(func() bool {
		violations := s.GetUmpire().Check(ctx, true)
		if len(violations) > 0 {
			s.T().Logf("Umpire detected %d violation(s):", len(violations))
			for _, vi := range violations {
				if v, ok := vi.(umpire.Violation); ok {
					s.T().Logf("  [%s] %s - Tags: %v", v.Rule, v.Message, v.Tags)
					if v.Rule == "WorkflowTaskStarvationRule" {
						return true
					}
				}
			}
		}
		s.T().Logf("Umpire Check returned 0 stuck workflow violations, waiting...")
		return false
	}, 60*time.Second, 1*time.Second, "Expected umpire to detect stuck workflow")

	// Clean up
	_, err = s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      startResp.GetRunId(),
		},
		Reason: "test cleanup",
	})
	s.NoError(err)
}

// stuckAwaitWorkflow is a workflow that uses workflow.Await with a condition that never becomes true,
// causing it to get stuck and never complete.
func stuckAwaitWorkflow(ctx workflow.Context) error {
	// Await a condition that will never become true
	err := workflow.Await(ctx, func() bool {
		return false // This condition will never be satisfied
	})
	return err
}

// TestStuckWorkflowDetectionWithSDK verifies that umpire detects a workflow that gets stuck.
// This test creates a workflow that uses workflow.Await with a condition that never becomes true.
// To simulate a stuck workflow task (which umpire detects), we:
// 1. Create a separate task queue and worker
// 2. Register a workflow that uses workflow.Await with a condition that never becomes true
// 3. Start the workflow
// 4. Stop the worker immediately to prevent workflow task completion
// 5. Wait for umpire to detect the workflow is stuck (no RespondWorkflowTaskCompleted)
func (s *LostTaskTestSuite) TestStuckWorkflowDetectionWithSDK() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Create a separate SDK client and worker for this test
	// We need to be able to stop the worker without affecting other tests
	taskQueue := "umpire-stuck-workflow-test-tq"
	sdkClient := s.SdkClient()

	// Create a worker on a dedicated task queue
	testWorker := worker.New(sdkClient, taskQueue, worker.Options{})
	testWorker.RegisterWorkflow(stuckAwaitWorkflow)

	// Start the worker
	err := testWorker.Start()
	s.NoError(err)
	s.T().Logf("Started test worker on task queue: %s", taskQueue)

	// Start the workflow
	workflowID := "umpire-stuck-await-workflow-test"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}

	workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, stuckAwaitWorkflow)
	s.NoError(err)
	s.T().Logf("Workflow started: %s/%s on task queue %s", workflowID, workflowRun.GetRunID(), taskQueue)

	// Stop the worker immediately to prevent it from completing the workflow task
	// This creates a scenario where the workflow task is dispatched but never completed
	testWorker.Stop()
	s.T().Logf("Stopped test worker - workflow task will not be completed")

	// Wait for umpire to detect the stuck workflow
	// The workflow should now be stuck with no RespondWorkflowTaskCompleted
	s.T().Logf("Waiting for umpire to detect stuck workflow...")

	s.Eventually(func() bool {
		violations := s.GetUmpire().Check(ctx, true)
		if len(violations) > 0 {
			s.T().Logf("Umpire detected %d violation(s):", len(violations))
			for _, vi := range violations {
				if v, ok := vi.(umpire.Violation); ok {
					s.T().Logf("  [%s] %s - Tags: %v", v.Rule, v.Message, v.Tags)
					if v.Rule == "WorkflowTaskStarvationRule" && v.Tags["workflowID"] == workflowID {
						return true
					}
				}
			}
		}
		s.T().Logf("Umpire Check returned 0 stuck workflow violations for %s, waiting...", workflowID)
		return false
	}, 70*time.Second, 2*time.Second, "Expected umpire to detect stuck workflow with Await")

	// Clean up
	err = sdkClient.CancelWorkflow(ctx, workflowID, workflowRun.GetRunID())
	s.NoError(err)
}
