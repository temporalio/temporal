package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log/tag"
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
	testcore.FunctionalTestBase
}

func TestWorkflowDeleteExecutionSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowDeleteExecutionSuite))
}

func (s *WorkflowDeleteExecutionSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		dynamicconfig.TransferProcessorUpdateAckInterval.Key():   1 * time.Second,
		dynamicconfig.VisibilityProcessorUpdateAckInterval.Key(): 1 * time.Second,
	}
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *WorkflowDeleteExecutionSuite) TestDeleteWorkflowExecution_CompetedWorkflow() {
	tv := testvars.New(s.T())

	const numExecutions = 5

	var wes []*commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := 0; i < numExecutions; i++ {
		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), workflowservice.StartWorkflowExecutionRequest_builder{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   tv.WithWorkflowIDNumber(i).WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			Identity:     tv.WorkerIdentity(),
		}.Build())
		s.NoError(err)
		wes = append(wes, commonpb.WorkflowExecution_builder{
			WorkflowId: tv.WithWorkflowIDNumber(i).WorkflowID(),
			RunId:      we.GetRunId(),
		}.Build())
	}

	// Complete workflow.
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		}.Build()}, nil
	}

	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           tv.TaskQueue(),
		Identity:            tv.WorkerIdentity(),
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	for range wes {
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
	}

	// Verify that workflow is completed and visibility is updated.
	for _, we := range wes {
		s.Eventually(
			func() bool {
				visibilityResponse, err := s.FrontendClient().ListWorkflowExecutions(
					testcore.NewContext(),
					workflowservice.ListWorkflowExecutionsRequest_builder{
						Namespace:     s.Namespace().String(),
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.GetWorkflowId()),
					}.Build(),
				)
				s.NoError(err)
				if len(visibilityResponse.GetExecutions()) == 1 &&
					visibilityResponse.GetExecutions()[0].GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
					return true
				}
				return false
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}

	// Delete workflow executions.
	for _, we := range wes {
		_, err := s.FrontendClient().DeleteWorkflowExecution(testcore.NewContext(), workflowservice.DeleteWorkflowExecutionRequest_builder{
			Namespace: s.Namespace().String(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: we.GetWorkflowId(),
				RunId:      we.GetRunId(),
			}.Build(),
		}.Build())
		s.NoError(err)
	}

	for i, we := range wes {
		s.Eventually(
			func() bool {
				// Check execution is deleted.
				describeResponse, err := s.FrontendClient().DescribeWorkflowExecution(
					testcore.NewContext(),
					workflowservice.DescribeWorkflowExecutionRequest_builder{
						Namespace: s.Namespace().String(),
						Execution: we,
					}.Build(),
				)
				if err == nil {
					s.Logger.Warn("Execution is not deleted yet", tag.NewInt("number", i), tag.WorkflowID(we.GetWorkflowId()), tag.WorkflowRunID(we.GetRunId()))
					return false
				}
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
				s.Nil(describeResponse)
				return true
			},
			waitForTaskProcessing,
			100*time.Millisecond,
		)

		// Check history is deleted.
		historyResponse, err := s.FrontendClient().GetWorkflowExecutionHistory(
			testcore.NewContext(),
			workflowservice.GetWorkflowExecutionHistoryRequest_builder{
				Namespace: s.Namespace().String(),
				Execution: we,
			}.Build(),
		)
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.Nil(historyResponse)

		s.Eventually(
			func() bool {
				// Check visibility is updated.
				visibilityResponse, err := s.FrontendClient().ListWorkflowExecutions(
					testcore.NewContext(),
					workflowservice.ListWorkflowExecutionsRequest_builder{
						Namespace:     s.Namespace().String(),
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.GetWorkflowId()),
					}.Build(),
				)
				s.NoError(err)
				if len(visibilityResponse.GetExecutions()) != 0 {
					s.Logger.Warn("Visibility is not deleted yet")
					return false
				}
				return true
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}
}

func (s *WorkflowDeleteExecutionSuite) TestDeleteWorkflowExecution_RunningWorkflow() {
	tv := testvars.New(s.T())

	const numExecutions = 5

	var wes []*commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := 0; i < numExecutions; i++ {
		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), workflowservice.StartWorkflowExecutionRequest_builder{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   tv.WithWorkflowIDNumber(i).WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			Identity:     tv.WorkerIdentity(),
		}.Build())
		s.NoError(err)
		wes = append(wes, commonpb.WorkflowExecution_builder{
			WorkflowId: tv.WithWorkflowIDNumber(i).WorkflowID(),
			RunId:      we.GetRunId(),
		}.Build())
	}

	// Verify that workflow is running and visibility is updated.
	for _, we := range wes {
		s.Eventually(
			func() bool {
				visibilityResponse, err := s.FrontendClient().ListWorkflowExecutions(
					testcore.NewContext(),
					workflowservice.ListWorkflowExecutionsRequest_builder{
						Namespace:     s.Namespace().String(),
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.GetWorkflowId()),
					}.Build(),
				)
				s.NoError(err)
				return len(visibilityResponse.GetExecutions()) == 1 &&
					visibilityResponse.GetExecutions()[0].GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}

	// Delete workflow executions.
	for _, we := range wes {
		_, err := s.FrontendClient().DeleteWorkflowExecution(testcore.NewContext(), workflowservice.DeleteWorkflowExecutionRequest_builder{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: we,
		}.Build())
		s.NoError(err)
	}

	for i, we := range wes {
		s.Eventually(
			func() bool {
				// Check execution is deleted.
				describeResponse, err := s.FrontendClient().DescribeWorkflowExecution(
					testcore.NewContext(),
					workflowservice.DescribeWorkflowExecutionRequest_builder{
						Namespace: s.Namespace().String(),
						Execution: we,
					}.Build(),
				)
				if err == nil {
					s.Logger.Warn("Execution is not deleted yet", tag.NewInt("number", i), tag.WorkflowID(we.GetWorkflowId()), tag.WorkflowRunID(we.GetRunId()))
					return false
				}
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
				s.Nil(describeResponse)
				return true
			},
			waitForTaskProcessing,
			100*time.Millisecond,
		)

		// Check history is deleted.
		historyResponse, err := s.FrontendClient().GetWorkflowExecutionHistory(
			testcore.NewContext(),
			workflowservice.GetWorkflowExecutionHistoryRequest_builder{
				Namespace: s.Namespace().String(),
				Execution: we,
			}.Build(),
		)
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.Nil(historyResponse)

		s.Eventually(
			func() bool {
				// Check visibility is updated.
				visibilityResponse, err := s.FrontendClient().ListWorkflowExecutions(
					testcore.NewContext(),
					workflowservice.ListWorkflowExecutionsRequest_builder{
						Namespace:     s.Namespace().String(),
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.GetWorkflowId()),
					}.Build(),
				)
				s.NoError(err)
				if len(visibilityResponse.GetExecutions()) != 0 {
					s.Logger.Warn("Visibility is not deleted yet")
					return false
				}
				return true
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}
}

func (s *WorkflowDeleteExecutionSuite) TestDeleteWorkflowExecution_JustTerminatedWorkflow() {
	tv := testvars.New(s.T())

	const numExecutions = 3

	var wes []*commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := 0; i < numExecutions; i++ {
		we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), workflowservice.StartWorkflowExecutionRequest_builder{
			RequestId:    uuid.NewString(),
			Namespace:    s.Namespace().String(),
			WorkflowId:   tv.WithWorkflowIDNumber(i).WorkflowID(),
			WorkflowType: tv.WorkflowType(),
			TaskQueue:    tv.TaskQueue(),
			Identity:     tv.WorkerIdentity(),
		}.Build())
		s.NoError(err)
		wes = append(wes, commonpb.WorkflowExecution_builder{
			WorkflowId: tv.WithWorkflowIDNumber(i).WorkflowID(),
			RunId:      we.GetRunId(),
		}.Build())
	}

	// Verify that workflow is running and visibility is updated.
	for _, we := range wes {
		s.Eventually(
			func() bool {
				visibilityResponse, err := s.FrontendClient().ListWorkflowExecutions(
					testcore.NewContext(),
					workflowservice.ListWorkflowExecutionsRequest_builder{
						Namespace:     s.Namespace().String(),
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.GetWorkflowId()),
					}.Build(),
				)
				s.NoError(err)
				return len(visibilityResponse.GetExecutions()) == 1 &&
					visibilityResponse.GetExecutions()[0].GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
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
		_, err := s.FrontendClient().TerminateWorkflowExecution(testcore.NewContext(), workflowservice.TerminateWorkflowExecutionRequest_builder{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: we,
		}.Build())
		s.NoError(err)
		s.Logger.Warn("Execution is terminated", tag.NewInt("number", i), tag.WorkflowID(we.GetWorkflowId()), tag.WorkflowRunID(we.GetRunId()))
		_, err = s.FrontendClient().DeleteWorkflowExecution(testcore.NewContext(), workflowservice.DeleteWorkflowExecutionRequest_builder{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: we,
		}.Build())
		s.NoError(err)
		s.Logger.Warn("Execution is scheduled for deletion", tag.NewInt("number", i), tag.WorkflowID(we.GetWorkflowId()), tag.WorkflowRunID(we.GetRunId()))
	}

	for i, we := range wes {
		s.Eventually(
			func() bool {
				// Check execution is deleted.
				describeResponse, err := s.FrontendClient().DescribeWorkflowExecution(
					testcore.NewContext(),
					workflowservice.DescribeWorkflowExecutionRequest_builder{
						Namespace: s.Namespace().String(),
						Execution: we,
					}.Build(),
				)
				if err == nil {
					s.Logger.Warn("Execution is not deleted yet", tag.NewInt("number", i), tag.WorkflowID(we.GetWorkflowId()), tag.WorkflowRunID(we.GetRunId()))
					return false
				}
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
				s.Nil(describeResponse)
				return true
			},
			waitForTaskProcessing,
			1*time.Second,
		)

		// Check history is deleted.
		historyResponse, err := s.FrontendClient().GetWorkflowExecutionHistory(
			testcore.NewContext(),
			workflowservice.GetWorkflowExecutionHistoryRequest_builder{
				Namespace: s.Namespace().String(),
				Execution: we,
			}.Build(),
		)
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.Nil(historyResponse)

		s.Eventually(
			func() bool {
				// Check visibility is updated.
				visibilityResponse, err := s.FrontendClient().ListWorkflowExecutions(
					testcore.NewContext(),
					workflowservice.ListWorkflowExecutionsRequest_builder{
						Namespace:     s.Namespace().String(),
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.GetWorkflowId()),
					}.Build(),
				)
				s.NoError(err)
				if len(visibilityResponse.GetExecutions()) != 0 {
					s.Logger.Warn("Visibility is not deleted yet", tag.NewInt("number", i), tag.WorkflowID(we.GetWorkflowId()), tag.WorkflowRunID(we.GetRunId()))
					return false
				}
				return true
			},
			testcore.WaitForESToSettle,
			100*time.Millisecond,
		)
	}
}
