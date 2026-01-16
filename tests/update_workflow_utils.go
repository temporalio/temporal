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

type updateResponseErr struct {
	response *workflowservice.UpdateWorkflowExecutionResponse
	err      error
}

func sendUpdate(ctx context.Context, s testEnv, tv *testvars.TestVars) <-chan updateResponseErr {
	s.T().Helper()
	return sendUpdateInternal(ctx, s, tv, nil, false)
}

func sendUpdateNoError(s testEnv, tv *testvars.TestVars) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	return sendUpdateNoErrorInternal(s, tv, nil)
}

func sendUpdateNoErrorWaitPolicyAccepted(s testEnv, tv *testvars.TestVars) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	return sendUpdateNoErrorInternal(s, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
}

func pollUpdate(s testEnv, tv *testvars.TestVars, waitPolicy *updatepb.WaitPolicy) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
	s.T().Helper()
	return s.FrontendClient().PollWorkflowExecutionUpdate(testcore.NewContext(), &workflowservice.PollWorkflowExecutionUpdateRequest{
		Namespace: s.Namespace().String(),
		UpdateRef: &updatepb.UpdateRef{
			WorkflowExecution: tv.WorkflowExecution(),
			UpdateId:          tv.UpdateID(),
		},
		WaitPolicy: waitPolicy,
	})
}

func updateWorkflowRequest(
	s testEnv,
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

func sendUpdateNoErrorInternal(s testEnv, tv *testvars.TestVars, waitPolicy *updatepb.WaitPolicy) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	retCh := make(chan *workflowservice.UpdateWorkflowExecutionResponse)
	syncCh := make(chan struct{})
	go func() {
		urCh := sendUpdateInternal(testcore.NewContext(), s, tv, waitPolicy, true)
		syncCh <- struct{}{}
		retCh <- (<-urCh).response
	}()
	<-syncCh
	return retCh
}

func sendUpdateInternal(
	ctx context.Context,
	s testEnv,
	tv *testvars.TestVars,
	waitPolicy *updatepb.WaitPolicy,
	requireNoError bool,
) <-chan updateResponseErr {
	s.T().Helper()

	updateResultCh := make(chan updateResponseErr)
	go func() {
		updateResp, updateErr := s.FrontendClient().UpdateWorkflowExecution(ctx, updateWorkflowRequest(s, tv, waitPolicy))
		if requireNoError && updateErr != nil {
			s.T().Errorf("Update failed: %v", updateErr)
		}
		updateResultCh <- updateResponseErr{response: updateResp, err: updateErr}
	}()
	waitUpdateAdmitted(s, tv)
	return updateResultCh
}

func waitUpdateAdmitted(s testEnv, tv *testvars.TestVars) {
	s.T().Helper()
	require.EventuallyWithTf(s.T(), func(collect *assert.CollectT) {
		pollResp, pollErr := s.FrontendClient().PollWorkflowExecutionUpdate(testcore.NewContext(), &workflowservice.PollWorkflowExecutionUpdateRequest{
			Namespace:  s.Namespace().String(),
			UpdateRef:  tv.UpdateRef(),
			WaitPolicy: &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED},
		})

		require.NoError(collect, pollErr)
		require.GreaterOrEqual(collect, pollResp.GetStage(), enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED)
	}, 5*time.Second, 10*time.Millisecond, "update %s did not reach Admitted stage", tv.UpdateID())
}
