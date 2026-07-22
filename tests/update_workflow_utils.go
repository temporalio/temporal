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

func sendUpdate(ctx context.Context, s testcore.Env, tv *testvars.TestVars) <-chan updateResponseErr {
	s.T().Helper()
	return sendUpdateInternal(ctx, s, tv, nil, false)
}

func sendUpdateNoError(s testcore.Env, tv *testvars.TestVars) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	return sendUpdateNoErrorInternal(s, tv, nil)
}

func sendUpdateNoErrorWaitPolicyAccepted(s testcore.Env, tv *testvars.TestVars) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
	s.T().Helper()
	return sendUpdateNoErrorInternal(s, tv, &updatepb.WaitPolicy{LifecycleStage: enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED})
}

func pollUpdate(s testcore.Env, tv *testvars.TestVars, waitPolicy *updatepb.WaitPolicy) (*workflowservice.PollWorkflowExecutionUpdateResponse, error) {
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
	s testcore.Env,
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

func sendUpdateNoErrorInternal(s testcore.Env, tv *testvars.TestVars, waitPolicy *updatepb.WaitPolicy) <-chan *workflowservice.UpdateWorkflowExecutionResponse {
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
	s testcore.Env,
	tv *testvars.TestVars,
	waitPolicy *updatepb.WaitPolicy,
	requireNoError bool,
) <-chan updateResponseErr {
	s.T().Helper()

	// updateResultCh is buffered so the goroutine below never blocks on sending to it,
	// regardless of whether (or when) the caller reads it.
	updateResultCh := make(chan updateResponseErr, 1)
	done := make(chan struct{})
	// Block this test's cleanup phase until the goroutine below has finished, so its
	// s.T().Errorf call (if any) always happens while the test is still active. Without this,
	// a slow UpdateWorkflowExecution call can still be in flight when the test itself returns,
	// and failing from a goroutine after the test has completed panics the whole process -
	// this used to be masked by how long namespace deletion took to become fully effective,
	// but isn't safe to rely on.
	s.T().Cleanup(func() { //nolint:staticcheck // SA1019: s is a generic testcore.Env here, not a suite, so T() is the only way to get *testing.T.
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			s.T().Logf("sendUpdateInternal: background UpdateWorkflowExecution for update %s did not finish before cleanup timeout", tv.UpdateID()) //nolint:staticcheck // SA1019: same as above.
		}
	})
	go func() {
		updateResp, updateErr := s.FrontendClient().UpdateWorkflowExecution(ctx, updateWorkflowRequest(s, tv, waitPolicy))
		if requireNoError && updateErr != nil {
			s.T().Errorf("Update failed: %v", updateErr)
		}
		updateResultCh <- updateResponseErr{response: updateResp, err: updateErr}
		close(done)
	}()
	waitUpdateAdmitted(s, tv)
	return updateResultCh
}

func waitUpdateAdmitted(s testcore.Env, tv *testvars.TestVars) {
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
