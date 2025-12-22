package tests

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

type PauseWorkflowExecutionSuite struct {
	testcore.FunctionalTestBase

	testEndSignal string
	pauseIdentity string
	pauseReason   string

	workflowFn      func(ctx workflow.Context) (string, error)
	childWorkflowFn func(ctx workflow.Context) (string, error)
	activityFn      func(ctx context.Context) (string, error)

	activityCompletedCh   chan struct{}
	activityCompletedOnce sync.Once
}

func TestPauseWorkflowExecutionSuite(t *testing.T) {
	s := new(PauseWorkflowExecutionSuite)
	suite.Run(t, s)
}

func (s *PauseWorkflowExecutionSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.OverrideDynamicConfig(dynamicconfig.WorkflowPauseEnabled, true)

	s.testEndSignal = "test-end"
	s.pauseIdentity = "functional-test"
	s.pauseReason = "pausing workflow for acceptance test"
	s.activityCompletedCh = make(chan struct{}, 1)
	s.activityCompletedOnce = sync.Once{}

	s.workflowFn = func(ctx workflow.Context) (string, error) {
		s.T().Log("workflow started")
		ao := workflow.ActivityOptions{
			StartToCloseTimeout:    5 * time.Second,
			ScheduleToCloseTimeout: 10 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var activityResult string
		s.T().Log("executing activity")
		if err := workflow.ExecuteActivity(ctx, s.activityFn).Get(ctx, &activityResult); err != nil {
			return "", err
		}

		var childResult string
		s.T().Log("executing child workflow")
		if err := workflow.ExecuteChildWorkflow(ctx, s.childWorkflowFn).Get(ctx, &childResult); err != nil {
			return "", err
		}

		s.T().Log("waiting to receive signal to complete the workflow")
		signalCh := workflow.GetSignalChannel(ctx, s.testEndSignal)
		var signalPayload string
		signalCh.Receive(ctx, &signalPayload)
		s.T().Log("signal received to complete the workflow")
		return signalPayload + activityResult + childResult, nil
	}

	s.childWorkflowFn = func(ctx workflow.Context) (string, error) {
		return "child-workflow", nil
	}

	s.activityFn = func(ctx context.Context) (string, error) {
		s.T().Log("activity started")
		s.activityCompletedOnce.Do(func() {
			// blocks until the test case unblocks the activity.
			<-s.activityCompletedCh
		})
		s.T().Log("activity completed")
		return "activity", nil
	}

	s.Worker().RegisterWorkflow(s.workflowFn)
	s.Worker().RegisterWorkflow(s.childWorkflowFn)
	s.Worker().RegisterActivity(s.activityFn)
}

// TestPauseUnpauseWorkflowExecution tests that the pause and unpause workflow execution APIs work as expected.
// Test sequence:
// 1. Start the workflow.
// 2. Pause the workflow. Assert that the workflow is paused.
// 3. Send signal to the workflow. Assert that the workflow is still paused.
// 4. Unpause the workflow. Assert that the workflow is now running.
// 5. Unblock the activity to complete the workflow.
// 6. Assert that the workflow is completed.
func (s *PauseWorkflowExecutionSuite) TestPauseUnpauseWorkflowExecution() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
	}, 5*time.Second, 100*time.Millisecond)

	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  uuid.NewString(),
	}

	pauseResp, err := s.FrontendClient().PauseWorkflowExecution(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// ensure that the workflow is paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		s.assertWorkflowIsPaused(ctx, t, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Send signal to the workflow to complete the workflow. Since the workflow is paused, it should stay paused.
	err = s.SdkClient().SignalWorkflow(ctx, workflowID, runID, s.testEndSignal, "signal to complete the workflow")
	s.NoError(err)
	s.EventuallyWithT(func(t *assert.CollectT) {
		s.assertWorkflowIsPaused(ctx, t, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Unpause the workflow.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := s.FrontendClient().UnpauseWorkflowExecution(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// Workflow status should be running after unpausing. The workflow won't complete until the activity is unblocked.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus(), "workflow is not running. Status: %s", info.GetStatus())
	}, 5*time.Second, 200*time.Millisecond)

	// Unblock the activity to complete the workflow.
	s.activityCompletedCh <- struct{}{}

	// assert that the workflow completes now.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus(), "workflow is not completed. Status: %s", info.GetStatus())
	}, 10*time.Second, 200*time.Millisecond)
}

func (s *PauseWorkflowExecutionSuite) TestQueryWorkflowWhenPaused() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
	}, 5*time.Second, 100*time.Millisecond)

	// Pause the workflow.
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseResp, err := s.FrontendClient().PauseWorkflowExecution(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// Wait until paused.
	s.EventuallyWithT(func(t *assert.CollectT) {
		s.assertWorkflowIsPaused(ctx, t, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Issue a query to the paused workflow. It should return QueryRejected with WORKFLOW_EXECUTION_STATUS_PAUSED status.
	queryReq := &workflowservice.QueryWorkflowRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Query: &querypb.WorkflowQuery{
			QueryType: "__stack_trace",
		},
	}
	queryResp, err := s.FrontendClient().QueryWorkflow(ctx, queryReq)
	s.NoError(err)
	s.NotNil(queryResp)
	s.NotNil(queryResp.GetQueryRejected())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, queryResp.GetQueryRejected().GetStatus())

	// Unpause the workflow.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := s.FrontendClient().UnpauseWorkflowExecution(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// Unblock the activity and send the signal to complete the workflow.
	s.activityCompletedCh <- struct{}{}
	err = s.SdkClient().SignalWorkflow(ctx, workflowID, runID, s.testEndSignal, "test end signal")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus())
	}, 5*time.Second, 200*time.Millisecond)
}

// TestPauseWorkflowExecutionRequestValidation tests that pause workflow execution request validation. We don't really need a valid workflow to test this.
// - fails when the identity is too long.
// - fails when the reason is too long.
// - fails when the request id is too long.
// - fails when the dynamic config is disabled.
func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowExecutionRequestValidation() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	namespaceName := s.Namespace().String()

	// fails when the identity is too long.
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   strings.Repeat("x", 2000),
		Reason:     s.pauseReason,
		RequestId:  uuid.NewString(),
	}
	resp, err := s.FrontendClient().PauseWorkflowExecution(ctx, pauseRequest)
	s.Error(err)
	s.Nil(resp)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "identity is too long.")

	// fails when the reason is too long.
	pauseRequest = &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   s.pauseIdentity,
		Reason:     strings.Repeat("x", 2000),
		RequestId:  uuid.NewString(),
	}
	resp, err = s.FrontendClient().PauseWorkflowExecution(ctx, pauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "reason is too long.")

	// fails when the request id is too long.
	pauseRequest = &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  strings.Repeat("x", 2000),
	}
	resp, err = s.FrontendClient().PauseWorkflowExecution(ctx, pauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "request id is too long.")

	// fails when the dynamic config is disabled.
	s.OverrideDynamicConfig(dynamicconfig.WorkflowPauseEnabled, false)
	pauseRequest = &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  uuid.NewString(),
	}
	resp, err = s.FrontendClient().PauseWorkflowExecution(ctx, pauseRequest)
	s.Error(err)
	s.Nil(resp)
	var unimplementedErr *serviceerror.Unimplemented
	s.ErrorAs(err, &unimplementedErr)
	s.NotNil(unimplementedErr)
	s.Contains(unimplementedErr.Error(), namespaceName)
}

// TestUnpauseWorkflowExecutionRequestValidation tests unpause workflow execution request validation. We don't need a valid workflow.
// - fails when the identity is too long.
// - fails when the reason is too long.
// - fails when the request id is too long.
func (s *PauseWorkflowExecutionSuite) TestUnpauseWorkflowExecutionRequestValidation() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	namespaceName := s.Namespace().String()

	// fails when the identity is too long.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   strings.Repeat("x", 2000),
		Reason:     s.pauseReason,
		RequestId:  uuid.NewString(),
	}
	resp, err := s.FrontendClient().UnpauseWorkflowExecution(ctx, unpauseRequest)
	s.Error(err)
	s.Nil(resp)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "identity is too long.")

	// fails when the reason is too long.
	unpauseRequest = &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   s.pauseIdentity,
		Reason:     strings.Repeat("x", 2000),
		RequestId:  uuid.NewString(),
	}
	resp, err = s.FrontendClient().UnpauseWorkflowExecution(ctx, unpauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "reason is too long.")

	// fails when the request id is too long.
	unpauseRequest = &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  strings.Repeat("x", 2000),
	}
	resp, err = s.FrontendClient().UnpauseWorkflowExecution(ctx, unpauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "request id is too long.")
}

func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowExecutionAlreadyPaused() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
	}, 5*time.Second, 100*time.Millisecond)

	// 1st pause request should succeed.
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseResp, err := s.FrontendClient().PauseWorkflowExecution(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// Wait until paused.
	s.EventuallyWithT(func(t *assert.CollectT) {
		s.assertWorkflowIsPaused(ctx, t, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// 2nd pause request should fail with failed precondition error.
	pauseRequest.RequestId = uuid.NewString()
	pauseResp, err = s.FrontendClient().PauseWorkflowExecution(ctx, pauseRequest)
	s.Error(err)
	s.Nil(pauseResp)
	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionErr)
	s.NotNil(failedPreconditionErr)
	s.Contains(failedPreconditionErr.Error(), "workflow is already paused.")

	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   s.pauseIdentity,
		Reason:     "cleanup after paused workflow test",
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := s.FrontendClient().UnpauseWorkflowExecution(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		require.Nil(t, desc.GetWorkflowExtendedInfo().GetPauseInfo())
	}, 5*time.Second, 200*time.Millisecond)

	// Unblock the activity and send the signal to complete the workflow.
	s.activityCompletedCh <- struct{}{}
	err = s.SdkClient().SignalWorkflow(ctx, workflowID, runID, s.testEndSignal, "test end signal")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus())
	}, 5*time.Second, 200*time.Millisecond)
}

// assertWorkflowIsPaused is a helper method which asserts that,
// - the workflow status is paused.
// - the workflow has the correct pause info.
// - there is no workflow task scheduled event inbetween pause and unpause events.
func (s *PauseWorkflowExecutionSuite) assertWorkflowIsPaused(ctx context.Context, t *assert.CollectT, workflowID string, runID string) {
	desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
	require.NoError(t, err)
	info := desc.GetWorkflowExecutionInfo()
	require.NotNil(t, info)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, info.GetStatus(), "workflow is not paused. Status: %s", info.GetStatus())
	if pauseInfo := desc.GetWorkflowExtendedInfo().GetPauseInfo(); pauseInfo != nil {
		require.Equal(t, s.pauseIdentity, pauseInfo.GetIdentity(), "pause identity is not correct")
		require.Equal(t, s.pauseReason, pauseInfo.GetReason(), "pause reason is not correct")
	}

	// Also assert that there is no workflow task scheduled event after the pause event.
	hist := s.SdkClient().GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	isPaused := false
	for hist.HasNext() {
		event, err := hist.Next()
		require.NoError(t, err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED {
			isPaused = true
			continue
		}
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED {
			isPaused = false
			continue
		}
		if isPaused {
			// These are all events after the pause event. None of these should be workflow task scheduled events.
			require.NotEqual(t, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, event.EventType)
		}
	}
}
