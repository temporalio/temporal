package tests

import (
	"context"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowUpdateBaseSuite struct {
	testcore.FunctionalTestBase
}

type updateResponseErr struct {
	response *workflowservice.UpdateWorkflowExecutionResponse
	err      error
}

func (s *WorkflowUpdateBaseSuite) sendUpdateNoErrorWaitPolicyAccepted(tv *testvars.TestVars) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	return s.sendUpdateNoErrorInternal(tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
}

func (s *WorkflowUpdateBaseSuite) sendUpdateNoErrorInternal(tv *testvars.TestVars, waitPolicy *updatepb.WaitPolicy) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	retCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	syncCh := make(chan struct{})
	go func() {
		urCh := s.sendUpdateInternal(testcore.NewContext(), tv, waitPolicy, true)
		// Unblock return only after the server admits update.
		syncCh <- struct{}{}
		// Unblocked when an update result is ready.
		retCh <- (<-urCh).response
	}()
	<-syncCh
	return retCh
}

func (s *WorkflowUpdateBaseSuite) sendUpdateInternal(
	ctx context.Context,
	tv *testvars.TestVars,
	waitPolicy *updatepb.WaitPolicy,
	requireNoError bool,
) <-chan updateResponseErr {

	s.T().Helper()

	updateResultCh := make(chan updateResponseErr)
	go func() {
		updateResp, updateErr := s.FrontendClient().UpdateWorkflowExecution(ctx, s.updateWorkflowRequest(tv, waitPolicy))
		// It is important to do assert here (before writing to channel which doesn't have readers yet)
		// to fail fast without trying to process update in wtHandler.
		if requireNoError {
			require.NoError(s.T(), updateErr)
		}
		updateResultCh <- updateResponseErr{response: updateResp, err: updateErr}
	}()
	s.waitUpdateAdmitted(tv)
	return updateResultCh
}

func (s *WorkflowUpdateBaseSuite) updateWorkflowRequest(
	tv *testvars.TestVars,
	waitPolicy *updatepb.WaitPolicy,
) *workflowservice.UpdateWorkflowExecutionRequest {
	return &workflowservice.UpdateWorkflowExecutionRequest{
		Namespace:         s.Namespace().String(),
		WorkflowExecution: tv.WorkflowExecution(),
		WaitPolicy:        waitPolicy,
		Request: &updatepb.Request{
			Meta: &updatepb.Meta{UpdateId: tv.UpdateID()},
			Input: &updatepb.Input{
				Name: tv.HandlerName(),
				Args: payloads.EncodeString("args-value-of-" + tv.UpdateID()),
			},
		},
	}
}

func (s *WorkflowUpdateBaseSuite) waitUpdateAdmitted(tv *testvars.TestVars) {
	s.T().Helper()
	s.EventuallyWithTf(func(collect *assert.CollectT) {
		pollResp, pollErr := s.FrontendClient().PollWorkflowExecutionUpdate(testcore.NewContext(), &workflowservice.PollWorkflowExecutionUpdateRequest{
			Namespace:  s.Namespace().String(),
			UpdateRef:  tv.UpdateRef(),
			WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED},
		})

		require.NoError(collect, pollErr)
		require.GreaterOrEqual(collect, pollResp.GetStage(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED)
	}, 5*time.Second, 10*time.Millisecond, "update %s did not reach Admitted stage", tv.UpdateID())
}

func (s *WorkflowUpdateBaseSuite) startWorkflow(tv *testvars.TestVars) string {
	s.T().Helper()
	startResp, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), s.startWorkflowRequest(tv))
	s.NoError(err)

	return startResp.GetRunId()
}

func (s *WorkflowUpdateBaseSuite) startWorkflowRequest(tv *testvars.TestVars) *workflowservice.StartWorkflowExecutionRequest {
	return &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    tv.Any().String(),
		Namespace:    s.Namespace().String(),
		WorkflowId:   tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(),
		TaskQueue:    tv.TaskQueue(),
	}
}
