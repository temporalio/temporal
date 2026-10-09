package tests

import (
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/tests/testcore"
)

func (s *WorkflowResetSuite) TestResetRequestIDDoesNotCarryToNextRun() {
	env := testcore.NewEnv(s.T())
	workflowID := env.Tv().WorkflowID()
	baseRunID := s.prepareSingleRun(env, workflowID, true)
	reset := func(runID, requestID string) string {
		response, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
			Namespace:                 env.Namespace().String(),
			WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(env, workflowID, runID),
			RequestId:                 requestID,
			Reason:                    "reset request identity belongs to one run",
		})
		s.Require().NoError(err)
		return response.GetRunId()
	}
	firstRequestID := uuid.NewString()
	firstRunID := reset(baseRunID, firstRequestID)
	workflowTask, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(), TaskQueue: env.Tv().TaskQueue(),
	})
	s.Require().NoError(err)
	s.Require().Equal(firstRunID, workflowTask.GetWorkflowExecution().GetRunId())
	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: workflowTask.GetTaskToken(),
	})
	s.Require().NoError(err)

	secondRequestID := uuid.NewString()
	secondRunID := reset(firstRunID, secondRequestID)
	s.Require().NotEqual(firstRunID, secondRunID)
	execution := &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: secondRunID}
	for _, rebuild := range []bool{false, true} {
		if rebuild {
			_, err = env.AdminClient().RebuildMutableState(s.Context(), &adminservice.RebuildMutableStateRequest{
				Namespace: env.Namespace().String(), Execution: execution,
			})
			s.Require().NoError(err)
		}
		state, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: env.Namespace().String(), Execution: execution,
		})
		s.Require().NoError(err)
		requestIDs := state.GetDatabaseMutableState().GetExecutionState().GetRequestIds()
		s.Require().Contains(requestIDs, secondRequestID)
		s.Require().NotContains(requestIDs, firstRequestID)
		s.Require().Equal(secondRunID, reset(firstRunID, secondRequestID))
		s.assertMutableStateStatus(env, workflowID, secondRunID, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	}
}
