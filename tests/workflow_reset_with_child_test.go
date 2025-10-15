package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
)

// Tests workflow reset feature. This suite executes the following scenarios:
//  1. Reset point is before the child init and child is not running (i.e. child already completed)
//     a: If the parent uses random ChildIDs (i.e. SDK generated): Expect it to make progress by starting a completely new
//     child.
//     b: If the parent used static ChildIDs (keeping default Child WorkflowIDReusePolicy): Expect it to make progress by
//     starting another instance of the child.
//     c: If the parent used static ChildIDs and set WorkflowIDReusePolicy=WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE :
//     Expect the parent to make progress by overriding the WorkflowIDReusePolicy to allow duplicate.
//  2. Reset point is before the child init and child is running at the time of reset
//     a: If the parent uses random ChildIDs: Expect the parent to make progress by starting a completely new child.
//     Additionally assert that the ParentClosePolicy is applied to the current running child (Note: Currently this
//     doesn’t work because of the bug mentioned above)
//     b: If the parent uses static ChildID (keeping default Child WorkflowIDReusePolicy): Expect the parent to make
//     progress by terminating the old child and starting another instance of the child.
//     c: If the parent used static ChildIDs and set WorkflowIDReusePolicy=WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE :
//     Expect the parent to make progress by overriding the WorkflowIDReusePolicy to allow duplicate and
//     WorkflowIdConflictPolicy to terminate the old child and start another instance of the child.
//  3. Reset point is after the child init and the child is running at the time of reset.
//     a: If the child was already started but not yet completed: Expect the parent to make progress by receiving the
//     completion event from the running child.
//     b: If the child was already completed: Expect the parent to make progress by retaining the results from the
//     completed child.
//     c: If the child was terminated/failed at the time of reset: Expect the parent to make progress by retaining the
//     termination/failure from the child.
type WorkflowResetWithChildSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowResetWithChildTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowResetWithChildSuite))
}

func (s *WorkflowResetWithChildSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.Worker().RegisterWorkflow(s.WorkflowWithChildren)
	s.Worker().RegisterWorkflow(s.WorkflowWithWaitingChild)
	s.Worker().RegisterWorkflow(child)
	s.Worker().RegisterWorkflow(s.waitingChild)
	s.Worker().RegisterActivity(simpleActivity)
	err := s.Worker().Start()
	s.NoError(err)
}

// Case 1.a Reset point is before the child init and child is not running (i.e. child already completed), with random ChildIDs
// This test will create 3 child workflows and resets between each child.
func (s *WorkflowResetWithChildSuite) TestResetWithChild() {
	// TODO: Enable this test when reset phase 2 is enabled.
	s.T().Skip("Skipping until reset phase 2 is enabled")
	wfID := "reset-workflow-with-children"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start a workflow with 3 children.
	options := s.startWorkflowOptions(wfID)
	run, err := s.SdkClient().ExecuteWorkflow(ctx, options, s.WorkflowWithChildren, false, false)
	s.NoError(err)
	var originalResult string
	err = run.Get(ctx, &originalResult)
	s.NoError(err)

	// save child init childIDs for later comparison.
	childIDs := s.getChildWFIDsFromHistory(ctx, wfID, run.GetRunID())
	s.Len(childIDs, 3)
	child1IDBeforeReset := childIDs[0]
	child2IDBeforeReset := childIDs[1]
	child3IDBeforeReset := childIDs[2]

	resetRequest := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfID,
			RunId:      run.GetRunID(),
		},
		Reason: "integration test",
	}
	// (reset #1) - resetting the workflow execution before both child workflows are started.
	resetRequest.RequestId = "reset-request-1"
	resetRequest.WorkflowTaskFinishEventId = 4
	resp, err := s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset1 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset1)
	s.NoError(err)
	s.Equal(originalResult, resultAfterReset1)

	childIDsAfterReset1 := s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
	s.Len(childIDsAfterReset1, 3)
	// All 3 child workflow IDs should be different after reset.
	s.NotEqual(child1IDBeforeReset, childIDsAfterReset1[0])
	s.NotEqual(child2IDBeforeReset, childIDsAfterReset1[1])
	s.NotEqual(child3IDBeforeReset, childIDsAfterReset1[2])

	// (reset #2) - resetting the new workflow execution after child-1 but before child-2
	resetRequest.RequestId = "reset-request-2"
	resetRequest.WorkflowExecution.RunId = resp.GetRunId()
	resetRequest.WorkflowTaskFinishEventId = s.getWorkflowTaskFinishEventIdAfterChild(ctx, wfID, resp.GetRunId(), childIDsAfterReset1[0].WorkflowId)
	resp, err = s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset2 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset2)
	s.NoError(err)
	s.Equal(originalResult, resultAfterReset2)

	childIDsAfterReset2 := s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
	s.Len(childIDsAfterReset2, 3)
	s.Equal(childIDsAfterReset1[0], childIDsAfterReset2[0])    // child-1 should be the same as before reset.
	s.NotEqual(childIDsAfterReset1[1], childIDsAfterReset2[1]) // child-2 should be different after reset.
	s.NotEqual(childIDsAfterReset1[2], childIDsAfterReset2[2]) // Child-3 should be different after reset.

	// (reset #3) - resetting the new workflow execution after child-2 but before child-3
	resetRequest.RequestId = "reset-request-3"
	resetRequest.WorkflowExecution.RunId = resp.GetRunId()
	resetRequest.WorkflowTaskFinishEventId = s.getWorkflowTaskFinishEventIdAfterChild(ctx, wfID, resp.GetRunId(), childIDsAfterReset2[1].WorkflowId)
	resp, err = s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset3 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset3)
	s.NoError(err)
	s.Equal(originalResult, resultAfterReset3)

	childIDsAfterReset3 := s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
	s.Len(childIDsAfterReset3, 3)
	// child-1 & child-2 workflow IDs should be the same as before reset. Child-3 should be different.
	s.Equal(childIDsAfterReset2[0], childIDsAfterReset3[0])
	s.Equal(childIDsAfterReset2[1], childIDsAfterReset3[1])
	s.NotEqual(childIDsAfterReset2[2], childIDsAfterReset3[2])

	// (reset #3) - resetting the new workflow execution one last time after child-3
	// This should successfully replay all child events and not change the child workflow IDs from previous run.
	resetRequest.RequestId = "reset-request-4"
	resetRequest.WorkflowExecution.RunId = resp.GetRunId()
	resetRequest.WorkflowTaskFinishEventId = s.getWorkflowTaskFinishEventIdAfterChild(ctx, wfID, resp.GetRunId(), childIDsAfterReset3[2].WorkflowId)
	resp, err = s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)
	childIDsFinal := s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
	s.Len(childIDsFinal, 3)
	s.Equal(childIDsAfterReset3[0], childIDsFinal[0])
	s.Equal(childIDsAfterReset3[1], childIDsFinal[1])
	s.Equal(childIDsAfterReset3[2], childIDsFinal[2])
}

// Case 1.b Reset point is before the child init and child is not running (i.e. child already completed), with specified ChildIDs
// This test will create 3 child workflows and resets between each child.
func (s *WorkflowResetWithChildSuite) TestResetWithChild_WithChildID() {
	// TODO: Enable this test when reset phase 2 is enabled.
	s.T().Skip("Skipping until reset phase 2 is enabled")
	wfID := "reset-workflow-with-children_with_child_id"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start a workflow with 3 children.
	options := s.startWorkflowOptions(wfID)
	// Executing WorkflowWithChildren with true for input fixedWID, and false for input rejectDuplicate.
	run, err := s.SdkClient().ExecuteWorkflow(ctx, options, s.WorkflowWithChildren, true, false)
	s.NoError(err)
	var originalResult string
	err = run.Get(ctx, &originalResult)
	s.NoError(err)

	// save child init initialChildExecutions for later comparison.
	initialChildExecutions := s.getChildWFIDsFromHistory(ctx, wfID, run.GetRunID())
	s.Len(initialChildExecutions, 3)

	resetRequest := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfID,
			RunId:      run.GetRunID(),
		},
		Reason: "integration test",
	}
	// (reset #1) - resetting the workflow execution before all 3 child workflows are started.
	resetRequest.RequestId = "reset-request-1"
	resetRequest.WorkflowTaskFinishEventId = 4
	resp, err := s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)
	// Wait for the new run to complete.
	var resultAfterReset1 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset1)
	s.NoError(err)
	s.Equal(originalResult, resultAfterReset1)

	childExecutionsAfterReset1 := s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
	s.Len(childExecutionsAfterReset1, 3)
	// All 3 child workflow IDs should be same after reset.
	s.Equal(initialChildExecutions[0].WorkflowId, childExecutionsAfterReset1[0].WorkflowId)
	s.Equal(initialChildExecutions[1].WorkflowId, childExecutionsAfterReset1[1].WorkflowId)
	s.Equal(initialChildExecutions[2].WorkflowId, childExecutionsAfterReset1[2].WorkflowId)

	// All 3 child rus IDs should be different after reset.
	s.NotEqual(initialChildExecutions[0].RunId, childExecutionsAfterReset1[0].RunId)
	s.NotEqual(initialChildExecutions[1].RunId, childExecutionsAfterReset1[1].RunId)
	s.NotEqual(initialChildExecutions[2].RunId, childExecutionsAfterReset1[2].RunId)

	// (reset #2) - resetting the new workflow execution after child-1 but before child-2
	resetRequest.RequestId = "reset-request-2"
	resetRequest.WorkflowExecution.RunId = resp.GetRunId()
	resetRequest.WorkflowTaskFinishEventId = s.getWorkflowTaskFinishEventIdAfterChild(ctx, wfID, resp.GetRunId(), childExecutionsAfterReset1[0].WorkflowId)
	resp, err = s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset2 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset2)
	s.NoError(err)
	s.Equal(originalResult, resultAfterReset2)

	childExecutionsAfterReset2 := s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
	s.Len(childExecutionsAfterReset2, 3)
	// All 3 child workflow IDs should be same after reset.
	s.Equal(childExecutionsAfterReset1[0].WorkflowId, childExecutionsAfterReset2[0].WorkflowId)
	s.Equal(childExecutionsAfterReset1[1].WorkflowId, childExecutionsAfterReset2[1].WorkflowId)
	s.Equal(childExecutionsAfterReset1[2].WorkflowId, childExecutionsAfterReset2[2].WorkflowId)

	// First child run ID should be same. But the other 2 should be different.
	s.Equal(childExecutionsAfterReset1[0].RunId, childExecutionsAfterReset2[0].RunId)
	s.NotEqual(childExecutionsAfterReset1[1].RunId, childExecutionsAfterReset2[1].RunId)
	s.NotEqual(childExecutionsAfterReset1[2].RunId, childExecutionsAfterReset2[2].RunId)

	// (reset #3) - resetting the new workflow execution after child-2 but before child-3
	resetRequest.RequestId = "reset-request-3"
	resetRequest.WorkflowExecution.RunId = resp.GetRunId()
	resetRequest.WorkflowTaskFinishEventId = s.getWorkflowTaskFinishEventIdAfterChild(ctx, wfID, resp.GetRunId(), childExecutionsAfterReset2[1].WorkflowId)
	resp, err = s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)
	// Wait for the new run to complete.
	var resultAfterReset3 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset3)
	s.NoError(err)
	s.Equal(originalResult, resultAfterReset3)

	childExecutionsAfterReset3 := s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
	s.Len(childExecutionsAfterReset3, 3)
	// All 3 child workflow IDs should be same after reset.
	s.Equal(childExecutionsAfterReset2[0].WorkflowId, childExecutionsAfterReset3[0].WorkflowId)
	s.Equal(childExecutionsAfterReset2[1].WorkflowId, childExecutionsAfterReset3[1].WorkflowId)
	s.Equal(childExecutionsAfterReset2[2].WorkflowId, childExecutionsAfterReset3[2].WorkflowId)

	// First two child run ID should be same. Third one should be different.
	s.Equal(childExecutionsAfterReset2[0].RunId, childExecutionsAfterReset3[0].RunId)
	s.Equal(childExecutionsAfterReset2[1].RunId, childExecutionsAfterReset3[1].RunId)
	s.NotEqual(childExecutionsAfterReset2[2].RunId, childExecutionsAfterReset3[2].RunId)

	// (reset #3) - resetting the new workflow execution one last time after child-3
	// This should successfully replay all child events and not change the child workflow IDs from previous run.
	resetRequest.RequestId = "reset-request-4"
	resetRequest.WorkflowExecution.RunId = resp.GetRunId()
	resetRequest.WorkflowTaskFinishEventId = s.getWorkflowTaskFinishEventIdAfterChild(ctx, wfID, resp.GetRunId(), childExecutionsAfterReset3[2].WorkflowId)
	resp, err = s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset4 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset4)
	s.NoError(err)
	s.Equal(originalResult, resultAfterReset4)

	childIDsFinal := s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
	s.Len(childIDsFinal, 3)
	s.Equal(childExecutionsAfterReset3[0].WorkflowId, childIDsFinal[0].WorkflowId)
	s.Equal(childExecutionsAfterReset3[1].WorkflowId, childIDsFinal[1].WorkflowId)
	s.Equal(childExecutionsAfterReset3[2].WorkflowId, childIDsFinal[2].WorkflowId)

	// All 3 child run IDs should be same after reset.
	s.Equal(childExecutionsAfterReset3[0].RunId, childIDsFinal[0].RunId)
	s.Equal(childExecutionsAfterReset3[1].RunId, childIDsFinal[1].RunId)
	s.Equal(childExecutionsAfterReset3[2].RunId, childIDsFinal[2].RunId)
}

// Case 1.c Reset point is before the child init and child is not running (i.e. child already completed), with specified
// ChildIDs and WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE. This test will create 3 child workflows and resets between each child.
func (s *WorkflowResetWithChildSuite) TestResetWithChild_WithChildID_WithRejectDuplicate() {
	// TODO: Enable this test when reset phase 2 is enabled.
	s.T().Skip("Skipping until reset phase 2 is enabled")
	wfID := "reset-workflow-with-children-with-child-workflow-id-with-reject-duplicate"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start a workflow with 3 children.
	options := s.startWorkflowOptions(wfID)
	// Executing WorkflowWithChildren with true for input fixedWID, and rejectDuplicate.
	run, err := s.SdkClient().ExecuteWorkflow(ctx, options, s.WorkflowWithChildren, true, true)
	s.NoError(err)
	var originalResult string
	err = run.Get(ctx, &originalResult)
	s.NoError(err)

	s.verifyReusePolicyIsSetForAllChild(ctx, wfID, run.GetRunID(), enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)

	// save child init initialChildExecutions for later comparison.
	initialChildExecutions := s.getChildWFIDsFromHistory(ctx, wfID, run.GetRunID())
	s.Len(initialChildExecutions, 3)

	resetRequest := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfID,
			RunId:      run.GetRunID(),
		},
		Reason: "integration test",
	}
	// (reset #1) - resetting the workflow execution before all 3 child workflows are started.
	resetRequest.RequestId = "reset-request-1"
	resetRequest.WorkflowTaskFinishEventId = 4
	resp, err := s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)
	// Wait for the new run to complete.
	var resultAfterReset1 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset1)
	s.NoError(err)
	s.Equal(originalResult, resultAfterReset1)

	childExecutionsAfterReset1 := s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
	s.Len(childExecutionsAfterReset1, 3)
	// All 3 child workflow IDs should be same after reset.
	s.Equal(initialChildExecutions[0].WorkflowId, childExecutionsAfterReset1[0].WorkflowId)
	s.Equal(initialChildExecutions[1].WorkflowId, childExecutionsAfterReset1[1].WorkflowId)
	s.Equal(initialChildExecutions[2].WorkflowId, childExecutionsAfterReset1[2].WorkflowId)

	// All 3 child rus IDs should be different after reset.
	s.NotEqual(initialChildExecutions[0].RunId, childExecutionsAfterReset1[0].RunId)
	s.NotEqual(initialChildExecutions[1].RunId, childExecutionsAfterReset1[1].RunId)
	s.NotEqual(initialChildExecutions[2].RunId, childExecutionsAfterReset1[2].RunId)

	// resetting the new workflow execution after child-3
	// This should successfully replay all child events and not change the child workflow IDs from previous run.
	resetRequest.RequestId = "reset-request-4"
	resetRequest.WorkflowExecution.RunId = resp.GetRunId()
	resetRequest.WorkflowTaskFinishEventId = s.getWorkflowTaskFinishEventIdAfterChild(ctx, wfID, resp.GetRunId(), initialChildExecutions[2].WorkflowId)
	resp, err = s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset4 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset4)
	s.NoError(err)
	s.Equal(originalResult, resultAfterReset4)

	childIDsFinal := s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
	s.Len(childIDsFinal, 3)
	s.Equal(childExecutionsAfterReset1[0].WorkflowId, childIDsFinal[0].WorkflowId)
	s.Equal(childExecutionsAfterReset1[1].WorkflowId, childIDsFinal[1].WorkflowId)
	s.Equal(childExecutionsAfterReset1[2].WorkflowId, childIDsFinal[2].WorkflowId)

	// All 3 child run IDs should be same after reset.
	s.Equal(childExecutionsAfterReset1[0].RunId, childIDsFinal[0].RunId)
	s.Equal(childExecutionsAfterReset1[1].RunId, childIDsFinal[1].RunId)
	s.Equal(childExecutionsAfterReset1[2].RunId, childIDsFinal[2].RunId)
}

// 2.a Reset point is before the child init and child is running at the time of reset. Child uses random WorkflowID.
func (s *WorkflowResetWithChildSuite) TestResetWithChild_RunningChild_RandomWID() {
	// TODO: Enable this test when reset phase 2 is enabled.
	s.T().Skip("Skipping until reset phase 2 is enabled")
	wfID := "reset-workflow-with-running-child"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	options := s.startWorkflowOptions(wfID)
	firstRun, err := s.SdkClient().ExecuteWorkflow(ctx, options, s.WorkflowWithWaitingChild, false, false)
	s.NoError(err)

	// save child init initialChildExecutions for later comparison.
	var initialChildExecutions []*commonpb.WorkflowExecution
	s.Eventually(func() bool {
		initialChildExecutions = s.getChildWFIDsFromHistory(ctx, wfID, firstRun.GetRunID())
		return len(initialChildExecutions) == 1

	}, 5*time.Second, 100*time.Millisecond)

	resetRequest := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfID,
			RunId:      firstRun.GetRunID(),
		},
		Reason: "integration test",
	}
	// resetting the workflow execution before child workflow starts.
	resetRequest.RequestId = "reset-request-1"
	resetRequest.WorkflowTaskFinishEventId = 4
	resp, err := s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	var childExecutionsAfterReset1 []*commonpb.WorkflowExecution
	s.Eventually(func() bool {
		childExecutionsAfterReset1 = s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
		return len(childExecutionsAfterReset1) == 1
	}, 5*time.Second, 100*time.Millisecond)

	// Let the second child finish by sending a signal.
	err = s.SdkClient().SignalWorkflow(context.Background(), childExecutionsAfterReset1[0].WorkflowId, childExecutionsAfterReset1[0].RunId, "continue", "")
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset1 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset1)
	s.NoError(err)

	s.NotEqual(initialChildExecutions[0].WorkflowId, childExecutionsAfterReset1[0].WorkflowId)
	s.NotEqual(initialChildExecutions[0].RunId, childExecutionsAfterReset1[0].RunId)

	// Verify that the first child is still running.
	descResp, err := s.SdkClient().DescribeWorkflowExecution(ctx, initialChildExecutions[0].WorkflowId, initialChildExecutions[0].RunId)
	s.NoError(err)

	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.GetWorkflowExecutionInfo().GetStatus(),
		"Child workflow should be running")

	// Let the first child finish execution by sending a signal.
	err = s.SdkClient().SignalWorkflow(context.Background(), initialChildExecutions[0].WorkflowId, initialChildExecutions[0].RunId, "continue", "")
	s.NoError(err)

	err = s.SdkClient().GetWorkflow(context.Background(), initialChildExecutions[0].WorkflowId, initialChildExecutions[0].RunId).Get(ctx, &resultAfterReset1)
	s.NoError(err)
}

// 2.b Reset point is before the child init and child is running at the time of reset. Child uses fixed WorkflowID.
func (s *WorkflowResetWithChildSuite) TestResetWithChild_RunningChild_SetWID() {
	// TODO: Enable this test when reset phase 2 is enabled.
	s.T().Skip("Skipping until reset phase 2 is enabled")
	wfID := "reset-workflow-with-running-child-with-set-wid"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	options := s.startWorkflowOptions(wfID)
	firstRun, err := s.SdkClient().ExecuteWorkflow(ctx, options, s.WorkflowWithWaitingChild, true, false)
	s.NoError(err)

	// save child init initialChildExecutions for later comparison.
	var initialChildExecutions []*commonpb.WorkflowExecution
	s.Eventually(func() bool {
		initialChildExecutions = s.getChildWFIDsFromHistory(ctx, wfID, firstRun.GetRunID())
		return len(initialChildExecutions) == 1

	}, 5*time.Second, 100*time.Millisecond)

	resetRequest := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfID,
			RunId:      firstRun.GetRunID(),
		},
		Reason: "integration test",
	}
	// resetting the workflow execution before all 3 child workflows are started.
	resetRequest.RequestId = "reset-request-1"
	resetRequest.WorkflowTaskFinishEventId = 4
	resp, err := s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Let the second child finish
	var childExecutionsAfterReset1 []*commonpb.WorkflowExecution
	s.Eventually(func() bool {
		childExecutionsAfterReset1 = s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
		return len(childExecutionsAfterReset1) == 1
	}, 5*time.Second, 100*time.Millisecond)

	err = s.SdkClient().SignalWorkflow(context.Background(), childExecutionsAfterReset1[0].WorkflowId, childExecutionsAfterReset1[0].RunId, "continue", "")
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset1 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset1)
	s.NoError(err)

	s.Equal(initialChildExecutions[0].WorkflowId, childExecutionsAfterReset1[0].WorkflowId)
	s.NotEqual(initialChildExecutions[0].RunId, childExecutionsAfterReset1[0].RunId)

	// Verify that the first child was terminated.
	descResp, err := s.SdkClient().DescribeWorkflowExecution(ctx, initialChildExecutions[0].WorkflowId, initialChildExecutions[0].RunId)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.GetWorkflowExecutionInfo().GetStatus(),
		"Child workflow should have status TERMINATED")
}

// 2.c Reset point is before the child init and child is running at the time of reset. Child uses fixed WorkflowID and POLICY_REJECT_DUPLICATE.
func (s *WorkflowResetWithChildSuite) TestResetWithChild_RunningChild_SetWID_WithRejectDuplicate() {
	// TODO: Enable this test when reset phase 2 is enabled.
	s.T().Skip("Skipping until reset phase 2 is enabled")
	wfID := "reset-workflow-with-running-child-with-set-wid-with-reject-duplicate"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	options := s.startWorkflowOptions(wfID)
	firstRun, err := s.SdkClient().ExecuteWorkflow(ctx, options, s.WorkflowWithWaitingChild, true, true)
	s.NoError(err)

	// save child init initialChildExecutions for later comparison.
	var initialChildExecutions []*commonpb.WorkflowExecution
	s.Eventually(func() bool {
		initialChildExecutions = s.getChildWFIDsFromHistory(ctx, wfID, firstRun.GetRunID())
		return len(initialChildExecutions) == 1

	}, 5*time.Second, 100*time.Millisecond)

	s.verifyReusePolicyIsSetForAllChild(ctx, wfID, firstRun.GetRunID(), enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)

	resetRequest := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfID,
			RunId:      firstRun.GetRunID(),
		},
		Reason: "integration test",
	}
	//  resetting the workflow execution before all 3 child workflows are started.
	resetRequest.RequestId = "reset-request-1"
	resetRequest.WorkflowTaskFinishEventId = 4
	resp, err := s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Let the second child finish
	var childExecutionsAfterReset1 []*commonpb.WorkflowExecution
	s.Eventually(func() bool {
		childExecutionsAfterReset1 = s.getChildWFIDsFromHistory(ctx, wfID, resp.GetRunId())
		return len(childExecutionsAfterReset1) == 1
	}, 5*time.Second, 100*time.Millisecond)

	err = s.SdkClient().SignalWorkflow(context.Background(), childExecutionsAfterReset1[0].WorkflowId, childExecutionsAfterReset1[0].RunId, "continue", "")
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset1 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset1)
	s.NoError(err)

	s.Equal(initialChildExecutions[0].WorkflowId, childExecutionsAfterReset1[0].WorkflowId)
	s.NotEqual(initialChildExecutions[0].RunId, childExecutionsAfterReset1[0].RunId)

	// Verify that the first child was terminated.
	descResp, err := s.SdkClient().DescribeWorkflowExecution(ctx, initialChildExecutions[0].WorkflowId, initialChildExecutions[0].RunId)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.GetWorkflowExecutionInfo().GetStatus(),
		"Child workflow should have status TERMINATED")
}

// 3.a Reset point is after the child init and the child is running at the time of reset. Child started but not completed.
func (s *WorkflowResetWithChildSuite) TestResetWithChild_AfterStartingChild() {
	wfID := "reset-workflow-with-running-child-after-starting-child"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	options := s.startWorkflowOptions(wfID)
	firstRun, err := s.SdkClient().ExecuteWorkflow(ctx, options, s.WorkflowWithWaitingChild, false, false)
	s.NoError(err)

	resetRequest := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfID,
			RunId:      firstRun.GetRunID(),
		},
		Reason: "integration test",
	}

	// save child init initialChildExecutions for later comparison.
	var initialChildExecutions []*commonpb.WorkflowExecution
	s.Eventually(func() bool {
		initialChildExecutions = s.getChildWFIDsFromHistory(ctx, wfID, firstRun.GetRunID())
		if len(initialChildExecutions) == 0 {
			return false
		}
		resetRequest.WorkflowTaskFinishEventId = s.getWorkflowTaskFinishEventIdAfterChildInit(ctx, wfID, firstRun.GetRunID(), initialChildExecutions[0].WorkflowId)
		return resetRequest.WorkflowTaskFinishEventId != 0
	}, 5*time.Second, 100*time.Millisecond)

	// resetting the new workflow execution after child-1 while child-1 is still running
	resetRequest.RequestId = "reset-request-2"
	resetRequest.WorkflowExecution.RunId = firstRun.GetRunID()
	resp, err := s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Let the second child finish now
	err = s.SdkClient().SignalWorkflow(context.Background(), initialChildExecutions[0].WorkflowId, initialChildExecutions[0].RunId, "continue", "")
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset1 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset1)
	s.NoError(err)

	// verify that the child completed.
	descResp, err := s.SdkClient().DescribeWorkflowExecution(ctx, initialChildExecutions[0].WorkflowId, initialChildExecutions[0].RunId)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.GetWorkflowExecutionInfo().GetStatus(),
		"Child workflow should have status COMPLETED")
}

// 3.b Reset point is after the child init and the child finished at the time of reset.
func (s *WorkflowResetWithChildSuite) TestResetWithChild_AfterChildCompletes() {
	wfID := "reset-workflow-with-running-child-after-completes"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	options := s.startWorkflowOptions(wfID)
	firstRun, err := s.SdkClient().ExecuteWorkflow(ctx, options, s.WorkflowWithWaitingChild, false, false)
	s.NoError(err)

	// save child init initialChildExecutions for later comparison.
	var initialChildExecutions []*commonpb.WorkflowExecution
	s.Eventually(func() bool {
		initialChildExecutions = s.getChildWFIDsFromHistory(ctx, wfID, firstRun.GetRunID())
		return len(initialChildExecutions) == 1

	}, 5*time.Second, 100*time.Millisecond)

	resetRequest := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfID,
			RunId:      firstRun.GetRunID(),
		},
		Reason: "integration test",
	}

	// Let the child finish now
	err = s.SdkClient().SignalWorkflow(context.Background(), initialChildExecutions[0].WorkflowId, initialChildExecutions[0].RunId, "continue", "")
	s.NoError(err)

	// Wait for the child to complete.
	s.Eventually(func() bool {
		return s.getWorkflowTaskFinishEventIdAfterChild(ctx, wfID, firstRun.GetRunID(), initialChildExecutions[0].WorkflowId) != 0
	}, 5*time.Second, 100*time.Millisecond)

	// resetting the new workflow execution after child initiation.
	resetRequest.RequestId = "reset-request-2"
	resetRequest.WorkflowExecution.RunId = firstRun.GetRunID()
	resetRequest.WorkflowTaskFinishEventId = s.getWorkflowTaskFinishEventIdAfterChildInit(ctx, wfID, firstRun.GetRunID(), initialChildExecutions[0].WorkflowId)
	resp, err := s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset1 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset1)
	s.NoError(err)
}

// 3.c Reset point is after the child init and the child terminated at the time of reset.
func (s *WorkflowResetWithChildSuite) TestResetWithChild_AfterChildTerminated() {
	wfID := "reset-workflow-with-child-after-child-terminates"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	options := s.startWorkflowOptions(wfID)
	firstRun, err := s.SdkClient().ExecuteWorkflow(ctx, options, s.WorkflowWithWaitingChild, false, false)
	s.NoError(err)

	// save child init initialChildExecutions for later comparison.
	var initialChildExecutions []*commonpb.WorkflowExecution
	s.Eventually(func() bool {
		initialChildExecutions = s.getChildWFIDsFromHistory(ctx, wfID, firstRun.GetRunID())
		return len(initialChildExecutions) == 1

	}, 5*time.Second, 100*time.Millisecond)

	resetRequest := &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfID,
			RunId:      firstRun.GetRunID(),
		},
		Reason: "integration test",
	}

	// Terminate child
	err = s.SdkClient().TerminateWorkflow(context.Background(), initialChildExecutions[0].WorkflowId, initialChildExecutions[0].RunId, "test")
	s.NoError(err)

	// Wait until the parent has recorded the child's terminated event and a subsequent WFT completed.
	var wftAfterChildTerminated int64
	s.Eventually(func() bool {
		wftAfterChildTerminated = s.getWorkflowTaskFinishEventIdAfterChild(ctx, wfID, firstRun.GetRunID(), initialChildExecutions[0].WorkflowId)
		return wftAfterChildTerminated != 0
	}, 5*time.Second, 200*time.Millisecond)

	// resetting the new workflow execution after child initiation.
	resetRequest.RequestId = "reset-request-2"
	resetRequest.WorkflowExecution.RunId = firstRun.GetRunID()
	resetRequest.WorkflowTaskFinishEventId = wftAfterChildTerminated
	resp, err := s.SdkClient().ResetWorkflowExecution(context.Background(), resetRequest)
	s.NoError(err)

	// Wait for the new run to complete.
	var resultAfterReset1 string
	err = s.SdkClient().GetWorkflow(context.Background(), wfID, resp.GetRunId()).Get(ctx, &resultAfterReset1)
	s.ErrorContains(err, "child workflow execution error")
	s.ErrorContains(err, "terminated")

	// verify that the child is terminated.
	descResp, err := s.SdkClient().DescribeWorkflowExecution(ctx, initialChildExecutions[0].WorkflowId, initialChildExecutions[0].RunId)
	s.NoError(err)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, descResp.GetWorkflowExecutionInfo().GetStatus(),
		"Child workflow should have status TERMINATED")
}

func (s *WorkflowResetWithChildSuite) startWorkflowOptions(wfID string) sdkclient.StartWorkflowOptions {
	var wfOptions = sdkclient.StartWorkflowOptions{
		ID:                       wfID,
		TaskQueue:                s.TaskQueue(),
		WorkflowExecutionTimeout: 15 * time.Second,
		WorkflowTaskTimeout:      time.Second,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}
	return wfOptions
}

// WorkflowWithChildren starts two child workflows and waits for them to complete in sequence.
func (s *WorkflowResetWithChildSuite) WorkflowWithChildren(ctx workflow.Context, fixedWID bool, rejectDuplicatePolicy bool) (string, error) {
	wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
	opt := workflow.ChildWorkflowOptions{}
	if rejectDuplicatePolicy {
		opt.WorkflowIDReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	}
	if fixedWID {
		opt.WorkflowID = wfID + "/test-child-workflow-1"
	}
	childCtx := workflow.WithChildOptions(ctx, opt)
	var result string
	err := workflow.ExecuteChildWorkflow(childCtx, child, "hello child-1").Get(ctx, &result)
	if err != nil {
		return "", err
	}

	if fixedWID {
		opt.WorkflowID = wfID + "/test-child-workflow-2"
	}
	childCtx = workflow.WithChildOptions(ctx, opt)
	var result2 string
	err = workflow.ExecuteChildWorkflow(childCtx, child, "hello child-2").Get(ctx, &result2)
	if err != nil {
		return "", err
	}

	if fixedWID {
		opt.WorkflowID = wfID + "/test-child-workflow-3"
	}
	childCtx = workflow.WithChildOptions(ctx, opt)
	var result3 string
	err = workflow.ExecuteChildWorkflow(childCtx, child, "hello child-2").Get(ctx, &result3)
	if err != nil {
		return "", err
	}

	return "Parent Workflow Complete", nil
}

// WorkflowWithChildren starts three child workflows and waits for them to complete in sequence.
func (s *WorkflowResetWithChildSuite) WorkflowWithWaitingChild(ctx workflow.Context, fixedWID bool, rejectDuplicatePolicy bool) (string, error) {
	wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
	var result string
	opt := workflow.ChildWorkflowOptions{}
	if rejectDuplicatePolicy {
		opt.WorkflowIDReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	}
	if fixedWID {
		opt.WorkflowID = wfID + "/test-child-workflow-1"
	}
	childCtx := workflow.WithChildOptions(ctx, opt)
	err := workflow.ExecuteChildWorkflow(childCtx, s.waitingChild, "hello child").Get(ctx, &result)
	if err != nil {
		return "", err
	}
	return result, err
}

func child(ctx workflow.Context, arg string, mustFail bool) (string, error) {
	var result string
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions())
	err := workflow.ExecuteActivity(ctx, simpleActivity, arg).Get(ctx, &result)
	return result, err
}

func (s *WorkflowResetWithChildSuite) waitingChild(ctx workflow.Context, arg string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, defaultActivityOptions())
	workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
	return arg, nil
}

// getWorkflowTaskFinishEventIdAfterChild gets the event ID of the first WFT completed after the child completed event.
// It does so by scanning the history of runID for any child completed events (completed, failed, canceled, timed out, terminated) and then the first WFT completed after that.
func (s *WorkflowResetWithChildSuite) getWorkflowTaskFinishEventIdAfterChild(ctx context.Context, wfID string, runID string, childID string) int64 {
	iter := s.SdkClient().GetWorkflowHistory(ctx, wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	childClosedSeen := false
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			break
		}
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
			if event.GetChildWorkflowExecutionCompletedEventAttributes().GetWorkflowExecution().GetWorkflowId() == childID {
				childClosedSeen = true
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:
			if event.GetChildWorkflowExecutionFailedEventAttributes().GetWorkflowExecution().GetWorkflowId() == childID {
				childClosedSeen = true
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:
			if event.GetChildWorkflowExecutionCanceledEventAttributes().GetWorkflowExecution().GetWorkflowId() == childID {
				childClosedSeen = true
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:
			if event.GetChildWorkflowExecutionTimedOutEventAttributes().GetWorkflowExecution().GetWorkflowId() == childID {
				childClosedSeen = true
			}
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
			if event.GetChildWorkflowExecutionTerminatedEventAttributes().GetWorkflowExecution().GetWorkflowId() == childID {
				childClosedSeen = true
			}
		default:
			// Do nothing and fall through.
		}
		if !childClosedSeen {
			continue
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			return event.GetEventId()
		}
	}
	return 0
}

func (s *WorkflowResetWithChildSuite) getWorkflowTaskFinishEventIdAfterChildInit(ctx context.Context, wfID string, runID string, childID string) int64 {
	iter := s.SdkClient().GetWorkflowHistory(ctx, wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	childFound := false
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			break
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED {
			if event.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowId() == childID {
				childFound = true
			}
		}
		if !childFound {
			continue
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			return event.GetEventId()
		}
	}
	return 0
}

func (s *WorkflowResetWithChildSuite) getChildWFIDsFromHistory(ctx context.Context, wfID string, runID string) []*commonpb.WorkflowExecution {
	iter := s.SdkClient().GetWorkflowHistory(ctx, wfID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var childExecutions []*commonpb.WorkflowExecution
	for iter.HasNext() {
		event, err1 := iter.Next()
		if err1 != nil {
			break
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED {
			childExecutions = append(childExecutions, event.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution())
		}
	}
	return childExecutions
}

// verifyReusePolicy checks if a given workflow was started with REJECT_DUPLICATE
func (s *WorkflowResetWithChildSuite) verifyReusePolicyIsSetForAllChild(ctx context.Context, workflowID, runID string, expected enumspb.WorkflowIdReusePolicy) {
	iter := s.SdkClient().GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	if !iter.HasNext() {
		s.Fail("No events found")
	}
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			s.Fail("Failed to get event")
			return
		}
		if event.GetEventType() == enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED {
			policy := event.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowIdReusePolicy()
			s.Equal(expected, policy)
		}
	}
}

func simpleActivity(ctx context.Context) error {
	return nil
}

func defaultActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		ScheduleToStartTimeout: 5 * time.Second,
		ScheduleToCloseTimeout: 5 * time.Second,
		StartToCloseTimeout:    9 * time.Second,
	}
}
