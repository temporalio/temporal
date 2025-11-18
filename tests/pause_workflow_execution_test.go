package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
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

	workflowFn func(ctx workflow.Context) (string, error)
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
		signalCh := workflow.GetSignalChannel(ctx, s.testEndSignal)
		var signalPayload string
		signalCh.Receive(ctx, &signalPayload)
		return signalPayload, nil
	}
}

func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowExecution() {
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

	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  uuid.New(),
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
		RunId:      uuid.New(),
		Identity:   strings.Repeat("x", 2000),
		Reason:     s.pauseReason,
		RequestId:  uuid.New(),
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
		RunId:      uuid.New(),
		Identity:   s.pauseIdentity,
		Reason:     strings.Repeat("x", 2000),
		RequestId:  uuid.New(),
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
		RunId:      uuid.New(),
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
		RunId:      uuid.New(),
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  uuid.New(),
	}
	resp, err = s.FrontendClient().PauseWorkflowExecution(ctx, pauseRequest)
	s.Error(err)
	s.Nil(resp)
	var unavailableErr *serviceerror.Unavailable
	s.ErrorAs(err, &unavailableErr)
	s.NotNil(unavailableErr)
	s.Contains(unavailableErr.Error(), namespaceName)
}

func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowExecutionAlreadyPaused() {
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

	// 1st pause request should succeed.
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   s.pauseIdentity,
		Reason:     s.pauseReason,
		RequestId:  uuid.New(),
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
		if pauseInfo := info.GetPauseInfo(); pauseInfo != nil {
			require.Equal(t, s.pauseIdentity, pauseInfo.GetIdentity())
			require.Equal(t, s.pauseReason, pauseInfo.GetReason())
		}
	}, 5*time.Second, 200*time.Millisecond)

	// 2nd pause request should fail with failed precondition error.
	pauseRequest.RequestId = uuid.New()
	pauseResp, err = s.FrontendClient().PauseWorkflowExecution(ctx, pauseRequest)
	s.Error(err)
	s.Nil(pauseResp)
	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionErr)
	s.NotNil(failedPreconditionErr)
	s.Contains(failedPreconditionErr.Error(), "workflow is already paused.")

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
