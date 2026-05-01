package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

const (
	// Internal task processing for DeleteExecutionTask checks if there is no pending CloseExecutionTask
	// comparing ack levels of the last processed task and DeleteExecutionTask TaskID.
	// Queue states/ack levels are updated with delay from "history.transferProcessorUpdateAckInterval"
	// which default 30s, but it is overridden in this suite to 1s (see SetupSuite below).
	// With few executions closed and deleted in parallel, it is hard to predict time needed for every DeleteExecutionTask
	// to process. Set it to 20s here, as a minimum sufficient interval. Increase it, if tests in this file are failing with
	// "Condition never satisfied" error.
	waitForTaskProcessing = 20 * time.Second
)

type WorkflowDeleteExecutionSuite struct {
	parallelsuite.Suite[*WorkflowDeleteExecutionSuite]
}

func TestWorkflowDeleteExecutionSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowDeleteExecutionSuite{})
}

func (s *WorkflowDeleteExecutionSuite) TestDeleteWorkflowExecutionCompletedWorkflow() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.TransferProcessorUpdateAckInterval, 1*time.Second),
		testcore.WithDynamicConfig(dynamicconfig.VisibilityProcessorUpdateAckInterval, 1*time.Second),
	)

	tv := testvars.New(s.T())

	const numExecutions = 5

	var wes []*commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := range numExecutions {
		we, err := env.FrontendClient().StartWorkflowExecution(env.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    env.Namespace().String(),
			WorkflowId:   tv.WithWorkflowIDNumber(i).WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			Identity:     tv.WorkerIdentity(),
		})
		s.NoError(err)
		wes = append(wes, &commonpb.WorkflowExecution{
			WorkflowId: tv.WithWorkflowIDNumber(i).WorkflowID(),
			RunId:      we.RunId,
		})
	}

	// Complete workflow.
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
		}}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              env.FrontendClient(),
		Namespace:           env.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              env.Logger,
		T:                   s.T(),
	}

	for range wes {
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
	}

	// Verify that workflow is completed and visibility is updated.
	for _, we := range wes {
		s.EventuallyWithT(func(t *assert.CollectT) {
			visibilityResponse, err := env.FrontendClient().ListWorkflowExecutions(
				env.Context(),
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace:     env.Namespace().String(),
					PageSize:      1,
					NextPageToken: nil,
					Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
				},
			)
			s.NoError(err)
			if len(visibilityResponse.Executions) == 1 &&
				visibilityResponse.Executions[0].Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {

				return
			}
			require.Fail(t, "condition was false")

		},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}

	// Delete workflow executions.
	for _, we := range wes {
		_, err := env.FrontendClient().DeleteWorkflowExecution(env.Context(), &workflowservice.DeleteWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
	}

	for i, we := range wes {
		s.EventuallyWithT(func(t *assert.CollectT) {
			// Check execution is deleted.
			describeResponse, err := env.FrontendClient().DescribeWorkflowExecution(
				env.Context(),
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: env.Namespace().String(),
					Execution: we,
				},
			)
			if err == nil {
				env.Logger.Warn("Execution is not deleted yet", tag.Int("number", i), tag.WorkflowID(we.WorkflowId), tag.WorkflowRunID(we.RunId))
				require.Fail(t, "condition was false")

				return
			}
			var notFoundErr *serviceerror.NotFound
			s.ErrorAs(err, &notFoundErr)
			s.Nil(describeResponse)

		},
			waitForTaskProcessing,
			100*time.Millisecond,
		)

		// Check history is deleted.
		historyResponse, err := env.FrontendClient().GetWorkflowExecutionHistory(
			env.Context(),
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: env.Namespace().String(),
				Execution: we,
			},
		)
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.Nil(historyResponse)

		s.EventuallyWithT(func(t *assert.CollectT) {
			// Check visibility is updated.
			visibilityResponse, err := env.FrontendClient().ListWorkflowExecutions(
				env.Context(),
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace:     env.Namespace().String(),
					PageSize:      1,
					NextPageToken: nil,
					Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
				},
			)
			s.NoError(err)
			if len(visibilityResponse.Executions) != 0 {
				env.Logger.Warn("Visibility is not deleted yet")
				require.Fail(t, "condition was false")

				return
			}

		},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}
}

func (s *WorkflowDeleteExecutionSuite) TestDeleteWorkflowExecutionRunningWorkflow() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.TransferProcessorUpdateAckInterval, 1*time.Second),
		testcore.WithDynamicConfig(dynamicconfig.VisibilityProcessorUpdateAckInterval, 1*time.Second),
	)

	tv := testvars.New(s.T())

	const numExecutions = 5

	var wes []*commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := range numExecutions {
		we, err := env.FrontendClient().StartWorkflowExecution(env.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    env.Namespace().String(),
			WorkflowId:   tv.WithWorkflowIDNumber(i).WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			Identity:     tv.WorkerIdentity(),
		})
		s.NoError(err)
		wes = append(wes, &commonpb.WorkflowExecution{
			WorkflowId: tv.WithWorkflowIDNumber(i).WorkflowID(),
			RunId:      we.RunId,
		})
	}

	// Verify that workflow is running and visibility is updated.
	for _, we := range wes {
		s.EventuallyWithT(func(t *assert.CollectT) {
			visibilityResponse, err := env.FrontendClient().ListWorkflowExecutions(
				env.Context(),
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace:     env.Namespace().String(),
					PageSize:      1,
					NextPageToken: nil,
					Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
				},
			)
			s.NoError(err)
			require.Equal(t, 1, len(visibilityResponse.Executions))
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, visibilityResponse.Executions[0].Status)
		},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}

	// Delete workflow executions.
	for _, we := range wes {
		_, err := env.FrontendClient().DeleteWorkflowExecution(env.Context(), &workflowservice.DeleteWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: we,
		})
		s.NoError(err)
	}

	for i, we := range wes {
		s.EventuallyWithT(func(t *assert.CollectT) {
			// Check execution is deleted.
			describeResponse, err := env.FrontendClient().DescribeWorkflowExecution(
				env.Context(),
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: env.Namespace().String(),
					Execution: we,
				},
			)
			if err == nil {
				env.Logger.Warn("Execution is not deleted yet", tag.Int("number", i), tag.WorkflowID(we.WorkflowId), tag.WorkflowRunID(we.RunId))
				require.Fail(t, "condition was false")

				return
			}
			var notFoundErr *serviceerror.NotFound
			s.ErrorAs(err, &notFoundErr)
			s.Nil(describeResponse)

		},
			waitForTaskProcessing,
			100*time.Millisecond,
		)

		// Check history is deleted.
		historyResponse, err := env.FrontendClient().GetWorkflowExecutionHistory(
			env.Context(),
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: env.Namespace().String(),
				Execution: we,
			},
		)
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.Nil(historyResponse)

		s.EventuallyWithT(func(t *assert.CollectT) {
			// Check visibility is updated.
			visibilityResponse, err := env.FrontendClient().ListWorkflowExecutions(
				env.Context(),
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace:     env.Namespace().String(),
					PageSize:      1,
					NextPageToken: nil,
					Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
				},
			)
			s.NoError(err)
			if len(visibilityResponse.Executions) != 0 {
				env.Logger.Warn("Visibility is not deleted yet")
				require.Fail(t, "condition was false")

				return
			}

		},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}
}

func (s *WorkflowDeleteExecutionSuite) TestDeleteWorkflowExecutionJustTerminatedWorkflow() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.TransferProcessorUpdateAckInterval, 1*time.Second),
		testcore.WithDynamicConfig(dynamicconfig.VisibilityProcessorUpdateAckInterval, 1*time.Second),
	)

	tv := testvars.New(s.T())

	const numExecutions = 3

	var wes []*commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := range numExecutions {
		we, err := env.FrontendClient().StartWorkflowExecution(env.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    env.Namespace().String(),
			WorkflowId:   tv.WithWorkflowIDNumber(i).WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			Identity:     tv.WorkerIdentity(),
		})
		s.NoError(err)
		wes = append(wes, &commonpb.WorkflowExecution{
			WorkflowId: tv.WithWorkflowIDNumber(i).WorkflowID(),
			RunId:      we.RunId,
		})
	}

	// Verify that workflow is running and visibility is updated.
	for _, we := range wes {
		s.EventuallyWithT(func(t *assert.CollectT) {
			visibilityResponse, err := env.FrontendClient().ListWorkflowExecutions(
				env.Context(),
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace:     env.Namespace().String(),
					PageSize:      1,
					NextPageToken: nil,
					Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
				},
			)
			s.NoError(err)
			require.Equal(t, 1, len(visibilityResponse.Executions))
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, visibilityResponse.Executions[0].Status)
		},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}

	// Internal task processing for DeleteExecutionTask checks if there is no pending CloseExecutionTask
	// to make sure that the workflow is closed successfully before deleting it. Otherwise, the mutable state
	// might be deleted before the close task is executed, and so the close task will be dropped.
	// See comment about ensureNoPendingCloseTask in transferQueueTaskExecutorBase.deleteExecution() for more details.

	// This is why this test _terminates_ and _immediately deletes_ workflow execution to simulate race between
	// two types of tasks and make sure that they are executed in correct order.

	for i, we := range wes {
		_, err := env.FrontendClient().TerminateWorkflowExecution(env.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: we,
		})
		s.NoError(err)
		env.Logger.Warn("Execution is terminated", tag.Int("number", i), tag.WorkflowID(we.WorkflowId), tag.WorkflowRunID(we.RunId))
		_, err = env.FrontendClient().DeleteWorkflowExecution(env.Context(), &workflowservice.DeleteWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: we,
		})
		s.NoError(err)
		env.Logger.Warn("Execution is scheduled for deletion", tag.Int("number", i), tag.WorkflowID(we.WorkflowId), tag.WorkflowRunID(we.RunId))
	}

	for i, we := range wes {
		s.EventuallyWithT(func(t *assert.CollectT) {
			// Check execution is deleted.
			describeResponse, err := env.FrontendClient().DescribeWorkflowExecution(
				env.Context(),
				&workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: env.Namespace().String(),
					Execution: we,
				},
			)
			if err == nil {
				env.Logger.Warn("Execution is not deleted yet", tag.Int("number", i), tag.WorkflowID(we.WorkflowId), tag.WorkflowRunID(we.RunId))
				require.Fail(t, "condition was false")

				return
			}
			var notFoundErr *serviceerror.NotFound
			s.ErrorAs(err, &notFoundErr)
			s.Nil(describeResponse)

		},
			waitForTaskProcessing,
			1*time.Second,
		)

		// Check history is deleted.
		historyResponse, err := env.FrontendClient().GetWorkflowExecutionHistory(
			env.Context(),
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: env.Namespace().String(),
				Execution: we,
			},
		)
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.Nil(historyResponse)

		s.EventuallyWithT(func(t *assert.CollectT) {
			// Check visibility is updated.
			visibilityResponse, err := env.FrontendClient().ListWorkflowExecutions(
				env.Context(),
				&workflowservice.ListWorkflowExecutionsRequest{
					Namespace:     env.Namespace().String(),
					PageSize:      1,
					NextPageToken: nil,
					Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
				},
			)
			s.NoError(err)
			if len(visibilityResponse.Executions) != 0 {
				env.Logger.Warn("Visibility is not deleted yet", tag.Int("number", i), tag.WorkflowID(we.WorkflowId), tag.WorkflowRunID(we.RunId))
				require.Fail(t, "condition was false")

				return
			}

		},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}
}
