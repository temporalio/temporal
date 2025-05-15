package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Tests workflow reset feature
type WorkflowResetSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowResetTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowResetSuite))
}

// No explicit base run provided. current run is still running.
func (s *WorkflowResetSuite) TestNoBaseCurrentRunning() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 1, true)
	currentRunID := runs[0]

	// Reset the current run (i.e don't give an explicit runID)
	resp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, workflowID, currentRunID),
	})
	s.NoError(err)
	newRunID := resp.RunId

	// Current run is the assumed base run. The new run should be linked to this one.
	currentMutableState, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: currentRunID},
	})
	s.NoError(err)
	s.Equal(currentMutableState.GetDatabaseMutableState().ExecutionInfo.ResetRunId, newRunID)
	s.Equal(currentMutableState.GetDatabaseMutableState().ExecutionState.Status, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)
}

// No explicit base run provided. current run is closed.
func (s *WorkflowResetSuite) TestNoBaseCurrentClosed() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 1, false)
	currentRunID := runs[0]

	// Reset the current run (i.e don't give an explicit runID)
	resp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, workflowID, currentRunID),
	})
	s.NoError(err)
	newRunID := resp.RunId

	// Current run is the assumed base run. The new run should be linked to this one.
	currentMutableState, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: currentRunID},
	})
	s.NoError(err)
	s.Equal(currentMutableState.GetDatabaseMutableState().ExecutionInfo.ResetRunId, newRunID)
	s.Equal(currentMutableState.GetDatabaseMutableState().ExecutionState.Status, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
}

// Explicit base run is provided to be reset and its the same as currently running execution.
func (s *WorkflowResetSuite) TestSameBaseCurrentRunning() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 1, true)
	baseRunID := runs[0]
	currentRunID := runs[0]

	newRunID := s.performReset(ctx, workflowID, baseRunID)

	s.assertResetWorkflowLink(ctx, workflowID, baseRunID, newRunID)
	s.assertMutableStateStatus(ctx, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)
}

// Explicit base run is provided. Its the same as current and is in closed state.
func (s *WorkflowResetSuite) TestSameBaseCurrentClosed() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 1, false)
	baseRunID := runs[0]
	currentRunID := runs[0]

	newRunID := s.performReset(ctx, workflowID, baseRunID)
	s.assertResetWorkflowLink(ctx, workflowID, baseRunID, newRunID)
	s.assertMutableStateStatus(ctx, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
}

// Explicit base run is provided. It is different from the currently running execution.
func (s *WorkflowResetSuite) TestDifferentBaseCurrentRunning() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 2, true)
	baseRunID := runs[0]
	currentRunID := runs[1]

	newRunID := s.performReset(ctx, workflowID, baseRunID)
	s.assertResetWorkflowLink(ctx, workflowID, baseRunID, newRunID)
	s.assertMutableStateStatus(ctx, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)
}

// Explicit base run is provided. It is different from the current run which in closed state.
func (s *WorkflowResetSuite) TestDifferentBaseCurrentClosed() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 2, false)
	baseRunID := runs[0]
	currentRunID := runs[1]

	newRunID := s.performReset(ctx, workflowID, baseRunID)
	s.assertResetWorkflowLink(ctx, workflowID, baseRunID, newRunID)
	s.assertMutableStateStatus(ctx, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
}

// Base is reset multuple times. Assert that each time it point to the new run.
func (s *WorkflowResetSuite) TestRepeatedResets() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 2, false)
	baseRunID := runs[0]
	currentRunID := runs[1]

	newRunID1 := s.performReset(ctx, workflowID, baseRunID)
	s.assertResetWorkflowLink(ctx, workflowID, baseRunID, newRunID1)
	s.assertMutableStateStatus(ctx, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)

	// reset again and ensure the pointer in base is also updated.
	newRunID2 := s.performReset(ctx, workflowID, baseRunID)
	s.assertResetWorkflowLink(ctx, workflowID, baseRunID, newRunID2)                                     // base -> newRunID2
	s.assertMutableStateStatus(ctx, workflowID, newRunID1, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED) // newRunID1 was the current run.
}

// Explicit base run is provided. There are more closed runs between base and the current run. Asserts that no other runs apart from base & current are mutated.
func (s *WorkflowResetSuite) TestWithMoreClosedRuns() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 5, false)
	baseRunID := runs[0]
	currentRunID := runs[4]
	noChangeRuns := runs[1:4]

	newRunID := s.performReset(ctx, workflowID, baseRunID)
	s.assertResetWorkflowLink(ctx, workflowID, baseRunID, newRunID)
	s.assertMutableStateStatus(ctx, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)

	// assert that these runs don't have any links and their status remains completed.
	for _, noChangeRunID := range noChangeRuns {
		s.assertResetWorkflowLink(ctx, workflowID, noChangeRunID, "") // empty link
		s.assertMutableStateStatus(ctx, workflowID, noChangeRunID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	}
}

func (s *WorkflowResetSuite) TestOriginalExecutionRunId() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 1, true)
	baseRunID := runs[0]
	// Reset the current run repeatedly. Verify that each time the new run points to the original baseRunID
	for i := 0; i < 5; i++ {
		currentRunID := s.performReset(ctx, workflowID, baseRunID)
		baseMutableState, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: currentRunID},
		})
		s.NoError(err)
		s.Equal(baseRunID, baseMutableState.GetDatabaseMutableState().ExecutionInfo.OriginalExecutionRunId)
	}
}

// Test that the workflow options are updated when the workflow is reset.
func (s *WorkflowResetSuite) TestResetWorkflowWithOptionsUpdate() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 1, true)
	currentRunID := runs[0]

	// Reset the workflow by providing the explicit runID (base run) to reset.
	resp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: currentRunID},
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, workflowID, currentRunID),
		PostResetOperations: []*workflowpb.PostResetOperation{
			{
				Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
					UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
						WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
							VersioningOverride: &workflowpb.VersioningOverride{
								Behavior:      enumspb.VERSIONING_BEHAVIOR_PINNED,
								PinnedVersion: "testing.v.123",
							},
						},
						UpdateMask: &fieldmaskpb.FieldMask{
							Paths: []string{
								"versioning_override",
							},
						},
					},
				},
			},
		},
	})
	s.NoError(err)
	newRunID := resp.RunId

	// assert that the new run has the updated workflow options
	var optionsUpdatedEvent *historypb.HistoryEvent
	hist := s.SdkClient().GetWorkflowHistory(ctx, workflowID, newRunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			optionsUpdatedEvent = event
			break
		}
	}
	s.NotNil(optionsUpdatedEvent)
	s.Equal(optionsUpdatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetVersioningOverride().GetBehavior(), enumspb.VERSIONING_BEHAVIOR_PINNED)
	s.Equal(optionsUpdatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetVersioningOverride().GetPinnedVersion(), "testing.v.123")
}

// Helper methods

// getFirstWFTaskCompleteEventID finds the first event corresponding to workflow task completion. This can be used as a good reset point for tests in this suite.
func (s *WorkflowResetSuite) getFirstWFTaskCompleteEventID(ctx context.Context, workflowID string, runID string) int64 {
	hist := s.SdkClient().GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			return event.EventId
		}
	}
	s.FailNow("Couldn't find a workflow task complete event for workflowID:[%s], runID:[%s]", workflowID, runID)
	return 0
}

// performReset is a helper method to reset the given workflow run and assert that it is successful.
func (s *WorkflowResetSuite) performReset(ctx context.Context, workflowID string, runID string) string {
	// Reset the workflow by providing the explicit runID (base run) to reset.
	resp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, workflowID, runID),
	})
	s.NoError(err)
	return resp.RunId
}

// assertMutableStateStatus asserts that the mutable state for the given run matches the expected status.
func (s *WorkflowResetSuite) assertMutableStateStatus(ctx context.Context, workflowID string, runID string, expectedStatus enumspb.WorkflowExecutionStatus) {
	ms, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	s.NoError(err)
	s.Equal(expectedStatus, ms.GetDatabaseMutableState().ExecutionState.Status)
}

// assertResetWorkflowLink asserts that the reset runID is properly recorded in the given run.
func (s *WorkflowResetSuite) assertResetWorkflowLink(ctx context.Context, workflowID string, runID string, expectedLinkRunID string) {
	baseMutableState, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	s.NoError(err)
	s.Equal(expectedLinkRunID, baseMutableState.GetDatabaseMutableState().ExecutionInfo.ResetRunId)
}

// helper method to setup the test run in the required configuration. It creates a total of n runs. If isCurrentRunning is true then the last run is kept open.
func (s *WorkflowResetSuite) setupRuns(ctx context.Context, workflowID string, n int, isCurrentRunning bool) []string {
	runs := []string{}
	for i := 0; i < n-1; i++ {
		runs = append(runs, s.prepareSingleRun(ctx, workflowID, false))
	}
	runs = append(runs, s.prepareSingleRun(ctx, workflowID, isCurrentRunning))
	return runs
}

func (s *WorkflowResetSuite) prepareSingleRun(ctx context.Context, workflowID string, isRunning bool) string {
	identity := "worker-identity"
	taskQueueName := testcore.RandomizeStr(s.T().Name())
	taskQueue := &taskqueuepb.TaskQueue{
		Name: taskQueueName,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueueName,
		ID:        workflowID,
	}, "test-workflow-arg")
	s.NoError(err)

	pollWTResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  "test",
	})
	s.NoError(err)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: pollWTResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId: "test-activity-id",
					TaskQueue:  taskQueue,

					ActivityType:        &commonpb.ActivityType{Name: "test-activity-name"},
					Input:               payloads.EncodeBytes([]byte{}),
					StartToCloseTimeout: durationpb.New(10 * time.Second),
				},
			},
		}},
	})
	s.NoError(err)

	// keep the workflow running if requested.
	if isRunning {
		return run.GetRunID()
	}

	pollATResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	})
	s.NoError(err)
	_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		TaskToken: pollATResp.TaskToken,
	})
	s.NoError(err)

	pollWTResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  "test",
	})
	s.NoError(err)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: pollWTResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
			},
		}},
	})
	s.NoError(err)
	return run.GetRunID()
}
