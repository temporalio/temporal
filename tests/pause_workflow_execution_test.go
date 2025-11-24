package tests

import (
	"context"
	"strings"
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

	s.workflowFn = func(ctx workflow.Context) (string, error) {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout:    5 * time.Second,
			ScheduleToCloseTimeout: 10 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var activityResult string
		if err := workflow.ExecuteActivity(ctx, s.activityFn).Get(ctx, &activityResult); err != nil {
			return "", err
		}

		var childResult string
		if err := workflow.ExecuteChildWorkflow(ctx, s.childWorkflowFn).Get(ctx, &childResult); err != nil {
			return "", err
		}

		signalCh := workflow.GetSignalChannel(ctx, s.testEndSignal)
		var signalPayload string
		signalCh.Receive(ctx, &signalPayload)
		return signalPayload + activityResult + childResult, nil
	}

	s.childWorkflowFn = func(ctx workflow.Context) (string, error) {
		return "child-workflow", nil
	}

	s.activityFn = func(ctx context.Context) (string, error) {
		return "activity", nil
	}
}

// TestPauseUnpauseWorkflowExecution tests that the pause and unpause workflow execution APIs work as expected.
func (s *PauseWorkflowExecutionSuite) TestPauseUnpauseWorkflowExecution() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.Worker().RegisterWorkflow(s.workflowFn)
	s.Worker().RegisterWorkflow(s.childWorkflowFn)
	s.Worker().RegisterActivity(s.activityFn)

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

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, info.GetStatus())
		if pauseInfo := desc.GetWorkflowExtendedInfo().GetPauseInfo(); pauseInfo != nil {
			require.Equal(t, s.pauseIdentity, pauseInfo.GetIdentity())
			require.Equal(t, s.pauseReason, pauseInfo.GetReason())
		}
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

	// Wait until unpaused (running again).
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		require.Nil(t, desc.GetWorkflowExtendedInfo().GetPauseInfo())
	}, 5*time.Second, 200*time.Millisecond)

	// TODO: currently pause workflow execution does not intercept workflow creation. Fix the reset of this test when that is implemented.
	// For now sending this signal will complete the workflow and finish the test.
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

func (s *PauseWorkflowExecutionSuite) TestQueryWorkflowWhenPaused() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.Worker().RegisterWorkflow(s.workflowFn)

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
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, info.GetStatus())
		if pauseInfo := desc.GetWorkflowExtendedInfo().GetPauseInfo(); pauseInfo != nil {
			require.Equal(t, s.pauseIdentity, pauseInfo.GetIdentity())
			require.Equal(t, s.pauseReason, pauseInfo.GetReason())
		}
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

	// Complete the workflow to finish the test.
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

	s.Worker().RegisterWorkflow(s.workflowFn)
	s.Worker().RegisterWorkflow(s.childWorkflowFn)
	s.Worker().RegisterActivity(s.activityFn)

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

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, info.GetStatus())
		if pauseInfo := desc.GetWorkflowExtendedInfo().GetPauseInfo(); pauseInfo != nil {
			require.Equal(t, s.pauseIdentity, pauseInfo.GetIdentity())
			require.Equal(t, s.pauseReason, pauseInfo.GetReason())
		}
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
		RequestId:  uuid.New(),
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

	// For now sending this signal will complete the workflow and finish the test.
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
