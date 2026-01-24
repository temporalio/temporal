package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Tests workflow reset feature
type WorkflowResetSuite struct {
	testcore.FunctionalTestBase
}

// versioningConfig contains configuration for setting up versioned pollers.
type versioningConfig struct {
	// Required indicates whether a versioned poller should be started for a particular test.
	Required bool
	// DeploymentName is the deployment name for versioning.
	DeploymentName string
	// BuildID is the build ID for versioning.
	BuildID string
}

func TestWorkflowResetTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(WorkflowResetSuite))
}

// No explicit base run provided. current run is still running.
func (s *WorkflowResetSuite) TestNoBaseCurrentRunning() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 1, true, versioningConfig{})
	currentRunID := runs[0]

	// Reset the current run (i.e don't give an explicit runID)
	resp, err := s.FrontendClient().ResetWorkflowExecution(ctx, workflowservice.ResetWorkflowExecutionRequest_builder{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         commonpb.WorkflowExecution_builder{WorkflowId: workflowID}.Build(),
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, workflowID, currentRunID),
	}.Build())
	s.NoError(err)
	newRunID := resp.GetRunId()

	// Current run is the assumed base run. The new run should be linked to this one.
	currentMutableState, err := s.AdminClient().DescribeMutableState(ctx, adminservice.DescribeMutableStateRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{WorkflowId: workflowID, RunId: currentRunID}.Build(),
		Archetype: chasm.WorkflowArchetype,
	}.Build())
	s.NoError(err)
	s.Equal(currentMutableState.GetDatabaseMutableState().GetExecutionInfo().GetResetRunId(), newRunID)
	s.Equal(currentMutableState.GetDatabaseMutableState().GetExecutionState().GetStatus(), enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)
}

// No explicit base run provided. current run is closed.
func (s *WorkflowResetSuite) TestNoBaseCurrentClosed() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 1, false, versioningConfig{})
	currentRunID := runs[0]

	// Reset the current run (i.e don't give an explicit runID)
	resp, err := s.FrontendClient().ResetWorkflowExecution(ctx, workflowservice.ResetWorkflowExecutionRequest_builder{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         commonpb.WorkflowExecution_builder{WorkflowId: workflowID}.Build(),
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, workflowID, currentRunID),
	}.Build())
	s.NoError(err)
	newRunID := resp.GetRunId()

	// Current run is the assumed base run. The new run should be linked to this one.
	currentMutableState, err := s.AdminClient().DescribeMutableState(ctx, adminservice.DescribeMutableStateRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{WorkflowId: workflowID, RunId: currentRunID}.Build(),
		Archetype: chasm.WorkflowArchetype,
	}.Build())
	s.NoError(err)
	s.Equal(currentMutableState.GetDatabaseMutableState().GetExecutionInfo().GetResetRunId(), newRunID)
	s.Equal(currentMutableState.GetDatabaseMutableState().GetExecutionState().GetStatus(), enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
}

// Explicit base run is provided to be reset and its the same as currently running execution.
func (s *WorkflowResetSuite) TestSameBaseCurrentRunning() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	runs := s.setupRuns(ctx, workflowID, 1, true, versioningConfig{})
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
	runs := s.setupRuns(ctx, workflowID, 1, false, versioningConfig{})
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
	runs := s.setupRuns(ctx, workflowID, 2, true, versioningConfig{})
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
	runs := s.setupRuns(ctx, workflowID, 2, false, versioningConfig{})
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
	runs := s.setupRuns(ctx, workflowID, 2, false, versioningConfig{})
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
	runs := s.setupRuns(ctx, workflowID, 5, false, versioningConfig{})
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
	runs := s.setupRuns(ctx, workflowID, 1, true, versioningConfig{})
	baseRunID := runs[0]
	// Reset the current run repeatedly. Verify that each time the new run points to the original baseRunID
	for i := 0; i < 5; i++ {
		currentRunID := s.performReset(ctx, workflowID, baseRunID)
		baseMutableState, err := s.AdminClient().DescribeMutableState(ctx, adminservice.DescribeMutableStateRequest_builder{
			Namespace: s.Namespace().String(),
			Execution: commonpb.WorkflowExecution_builder{WorkflowId: workflowID, RunId: currentRunID}.Build(),
			Archetype: chasm.WorkflowArchetype,
		}.Build())
		s.NoError(err)
		s.Equal(baseRunID, baseMutableState.GetDatabaseMutableState().GetExecutionInfo().GetOriginalExecutionRunId())
	}
}

// Test that the workflow options are updated when the workflow is reset.
func (s *WorkflowResetSuite) TestResetWorkflowWithOptionsUpdate() {
	workflowID := "test-reset" + uuid.NewString()
	ctx := testcore.NewContext()
	deploymentName := "testing"
	buildID := "v.123"

	// Setup runs with versioning enabled so that the version is present in the task queue before the
	// versioning override is set.
	runs := s.setupRuns(ctx, workflowID, 1, true, versioningConfig{
		Required:       true,
		DeploymentName: deploymentName,
		BuildID:        buildID,
	})
	currentRunID := runs[0]

	override := workflowpb.VersioningOverride_builder{
		Pinned: workflowpb.VersioningOverride_PinnedOverride_builder{
			Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
			Version: deploymentpb.WorkerDeploymentVersion_builder{
				DeploymentName: deploymentName,
				BuildId:        buildID,
			}.Build(),
		}.Build(),
	}.Build()

	// Reset the workflow by providing the explicit runID (base run) to reset.
	resp, err := s.FrontendClient().ResetWorkflowExecution(ctx, workflowservice.ResetWorkflowExecutionRequest_builder{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         commonpb.WorkflowExecution_builder{WorkflowId: workflowID, RunId: currentRunID}.Build(),
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, workflowID, currentRunID),
		PostResetOperations: []*workflowpb.PostResetOperation{
			workflowpb.PostResetOperation_builder{
				UpdateWorkflowOptions: workflowpb.PostResetOperation_UpdateWorkflowOptions_builder{
					WorkflowExecutionOptions: workflowpb.WorkflowExecutionOptions_builder{
						VersioningOverride: override,
					}.Build(),
					UpdateMask: &fieldmaskpb.FieldMask{
						Paths: []string{
							"versioning_override",
						},
					},
				}.Build(),
			}.Build(),
		},
	}.Build())
	s.NoError(err)
	newRunID := resp.GetRunId()

	// assert that the new run has the updated workflow options
	var optionsUpdatedEvent *historypb.HistoryEvent
	hist := s.SdkClient().GetWorkflowHistory(ctx, workflowID, newRunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			optionsUpdatedEvent = event
			break
		}
	}
	s.NotNil(optionsUpdatedEvent)
	s.ProtoEqual(override, optionsUpdatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetVersioningOverride())

	info, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, newRunID)
	s.NoError(err)

	// TODO (Carly): remove deprecated values from verification once we stop populating them
	override.SetBehavior(enumspb.VERSIONING_BEHAVIOR_PINNED)  //nolint:staticcheck
	override.SetPinnedVersion(deploymentName + "." + buildID) //nolint:staticcheck
	s.ProtoEqual(override, info.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersioningOverride())
}

// Test batch reset operation with version update as post reset operation
func (s *WorkflowResetSuite) TestBatchResetWithOptionsUpdate() {
	ctx := testcore.NewContext()
	deploymentName := "batch-testing"
	buildID := "v.456"

	// Create 2 workflows for batch reset
	workflowID1 := "test-batch-reset-1-" + uuid.NewString()
	workflowID2 := "test-batch-reset-2-" + uuid.NewString()

	// Setup runs
	versioningConfig := versioningConfig{Required: true, DeploymentName: deploymentName, BuildID: buildID}
	runs1 := s.setupRuns(ctx, workflowID1, 1, true, versioningConfig)
	runs2 := s.setupRuns(ctx, workflowID2, 1, true, versioningConfig)

	// Create versioning override for post-reset operations
	override := workflowpb.VersioningOverride_builder{
		Pinned: workflowpb.VersioningOverride_PinnedOverride_builder{
			Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
			Version: deploymentpb.WorkerDeploymentVersion_builder{
				DeploymentName: deploymentName,
				BuildId:        buildID,
			}.Build(),
		}.Build(),
	}.Build()

	// Start batch reset operation
	batchJobID := "batch-reset-job-" + uuid.NewString()
	_, err := s.FrontendClient().StartBatchOperation(ctx, workflowservice.StartBatchOperationRequest_builder{
		Namespace: s.Namespace().String(),
		JobId:     batchJobID,
		Reason:    "testing-batch-reset-with-options",
		Executions: []*commonpb.WorkflowExecution{
			commonpb.WorkflowExecution_builder{WorkflowId: workflowID1, RunId: runs1[0]}.Build(),
			commonpb.WorkflowExecution_builder{WorkflowId: workflowID2, RunId: runs2[0]}.Build(),
		},
		ResetOperation: batchpb.BatchOperationReset_builder{
			Identity: "test-batch-reset",
			Options: commonpb.ResetOptions_builder{
				WorkflowTaskId: proto.Int64(s.getFirstWFTaskCompleteEventID(ctx, workflowID1, runs1[0])),
			}.Build(),
			PostResetOperations: []*workflowpb.PostResetOperation{
				workflowpb.PostResetOperation_builder{
					UpdateWorkflowOptions: workflowpb.PostResetOperation_UpdateWorkflowOptions_builder{
						WorkflowExecutionOptions: workflowpb.WorkflowExecutionOptions_builder{
							VersioningOverride: override,
						}.Build(),
						UpdateMask: &fieldmaskpb.FieldMask{
							Paths: []string{
								"versioning_override",
							},
						},
					}.Build(),
				}.Build(),
			},
		}.Build(),
	}.Build())
	s.NoError(err)

	// Wait for batch operation to complete
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().DescribeBatchOperation(ctx, workflowservice.DescribeBatchOperationRequest_builder{
			Namespace: s.Namespace().String(),
			JobId:     batchJobID,
		}.Build())
		if err != nil {
			return false
		}
		return resp.GetState() == enumspb.BATCH_OPERATION_STATE_COMPLETED
	}, 20*time.Second, 1*time.Second, "Batch operation should complete")

	// Get the new run IDs after reset
	// The workflows should be terminated and new runs started
	newWorkflows := s.getLatestRunsForWorkflows(ctx, []string{workflowID1, workflowID2})
	s.Len(newWorkflows, 2)

	// Verify both workflows have the options updated event and correct versioning override
	for i, workflowID := range []string{workflowID1, workflowID2} {
		newRunID := newWorkflows[i]

		// Find the options updated event in history
		var optionsUpdatedEvent *historypb.HistoryEvent
		hist := s.SdkClient().GetWorkflowHistory(ctx, workflowID, newRunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for hist.HasNext() {
			event, err := hist.Next()
			s.NoError(err)
			if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
				optionsUpdatedEvent = event
				break
			}
		}
		s.NotNil(optionsUpdatedEvent, "Workflow %s should have options updated event", workflowID)
		s.ProtoEqual(override, optionsUpdatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetVersioningOverride())

		// Verify the workflow execution info has the correct versioning override
		info, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, newRunID)
		s.NoError(err)

		expectedOverride := workflowpb.VersioningOverride_builder{
			Pinned: workflowpb.VersioningOverride_PinnedOverride_builder{
				Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
				Version: deploymentpb.WorkerDeploymentVersion_builder{
					DeploymentName: deploymentName,
					BuildId:        buildID,
				}.Build(),
			}.Build(),
		}.Build()
		s.ProtoEqual(expectedOverride.GetPinned().GetVersion(), info.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersioningOverride().GetPinned().GetVersion())
	}
}

// Helper methods

// getFirstWFTaskCompleteEventID finds the first event corresponding to workflow task completion. This can be used as a good reset point for tests in this suite.
func (s *WorkflowResetSuite) getFirstWFTaskCompleteEventID(ctx context.Context, workflowID string, runID string) int64 {
	hist := s.SdkClient().GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			return event.GetEventId()
		}
	}
	s.FailNow("Couldn't find a workflow task complete event for workflowID:[%s], runID:[%s]", workflowID, runID)
	return 0
}

// performReset is a helper method to reset the given workflow run and assert that it is successful.
func (s *WorkflowResetSuite) performReset(ctx context.Context, workflowID string, runID string) string {
	// Reset the workflow by providing the explicit runID (base run) to reset.
	resp, err := s.FrontendClient().ResetWorkflowExecution(ctx, workflowservice.ResetWorkflowExecutionRequest_builder{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         commonpb.WorkflowExecution_builder{WorkflowId: workflowID, RunId: runID}.Build(),
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(ctx, workflowID, runID),
	}.Build())
	s.NoError(err)
	return resp.GetRunId()
}

// assertMutableStateStatus asserts that the mutable state for the given run matches the expected status.
func (s *WorkflowResetSuite) assertMutableStateStatus(ctx context.Context, workflowID string, runID string, expectedStatus enumspb.WorkflowExecutionStatus) {
	ms, err := s.AdminClient().DescribeMutableState(ctx, adminservice.DescribeMutableStateRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{WorkflowId: workflowID, RunId: runID}.Build(),
		Archetype: chasm.WorkflowArchetype,
	}.Build())
	s.NoError(err)
	s.Equal(expectedStatus, ms.GetDatabaseMutableState().GetExecutionState().GetStatus())
}

// assertResetWorkflowLink asserts that the reset runID is properly recorded in the given run.
func (s *WorkflowResetSuite) assertResetWorkflowLink(ctx context.Context, workflowID string, runID string, expectedLinkRunID string) {
	baseMutableState, err := s.AdminClient().DescribeMutableState(ctx, adminservice.DescribeMutableStateRequest_builder{
		Namespace: s.Namespace().String(),
		Execution: commonpb.WorkflowExecution_builder{WorkflowId: workflowID, RunId: runID}.Build(),
		Archetype: chasm.WorkflowArchetype,
	}.Build())
	s.NoError(err)
	s.Equal(expectedLinkRunID, baseMutableState.GetDatabaseMutableState().GetExecutionInfo().GetResetRunId())
}

// helper method to setup the test run in the required configuration. It creates a total of n runs. If isCurrentRunning is true then the last run is kept open.
func (s *WorkflowResetSuite) setupRuns(ctx context.Context, workflowID string, n int, isCurrentRunning bool, versioningConfig versioningConfig) []string {
	taskQueueName := testcore.RandomizeStr(s.T().Name())

	// If versioning is requested, start a versioned poller and validate version membership
	if versioningConfig.Required {
		s.startVersionedPollerAndValidate(ctx, taskQueueName, versioningConfig.DeploymentName, versioningConfig.BuildID)
	}

	runs := []string{}
	for i := 0; i < n-1; i++ {
		runs = append(runs, s.prepareSingleRun(ctx, workflowID, taskQueueName, false))
	}
	runs = append(runs, s.prepareSingleRun(ctx, workflowID, taskQueueName, isCurrentRunning))
	return runs
}

func (s *WorkflowResetSuite) prepareSingleRun(ctx context.Context, workflowID string, taskQueueName string, isRunning bool) string {
	identity := "worker-identity"
	taskQueue := taskqueuepb.TaskQueue_builder{
		Name: taskQueueName,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}.Build()

	run, err := s.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue: taskQueueName,
		ID:        workflowID,
	}, "test-workflow-arg")
	s.NoError(err)

	pollWTResp, err := s.FrontendClient().PollWorkflowTaskQueue(ctx, workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  "test",
	}.Build())
	s.NoError(err)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, workflowservice.RespondWorkflowTaskCompletedRequest_builder{
		TaskToken: pollWTResp.GetTaskToken(),
		Commands: []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			ScheduleActivityTaskCommandAttributes: commandpb.ScheduleActivityTaskCommandAttributes_builder{
				ActivityId: "test-activity-id",
				TaskQueue:  taskQueue,

				ActivityType:        commonpb.ActivityType_builder{Name: "test-activity-name"}.Build(),
				Input:               payloads.EncodeBytes([]byte{}),
				StartToCloseTimeout: durationpb.New(10 * time.Second),
			}.Build(),
		}.Build()},
	}.Build())
	s.NoError(err)

	// keep the workflow running if requested.
	if isRunning {
		return run.GetRunID()
	}

	pollATResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, workflowservice.PollActivityTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  identity,
	}.Build())
	s.NoError(err)
	_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, workflowservice.RespondActivityTaskCompletedRequest_builder{
		TaskToken: pollATResp.GetTaskToken(),
	}.Build())
	s.NoError(err)

	pollWTResp, err = s.FrontendClient().PollWorkflowTaskQueue(ctx, workflowservice.PollWorkflowTaskQueueRequest_builder{
		Namespace: s.Namespace().String(),
		TaskQueue: taskQueue,
		Identity:  "test",
	}.Build())
	s.NoError(err)

	_, err = s.FrontendClient().RespondWorkflowTaskCompleted(ctx, workflowservice.RespondWorkflowTaskCompletedRequest_builder{
		TaskToken: pollWTResp.GetTaskToken(),
		Commands: []*commandpb.Command{commandpb.Command_builder{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		}.Build()},
	}.Build())
	s.NoError(err)
	return run.GetRunID()
}

// startVersionedPollerAndValidate starts a versioned poller for the given task queue
// and validates that the version is present in the task queue via matching RPC.
func (s *WorkflowResetSuite) startVersionedPollerAndValidate(
	ctx context.Context,
	taskQueueName string,
	deploymentName string,
	buildID string,
) {
	taskQueue := taskqueuepb.TaskQueue_builder{
		Name: taskQueueName,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}.Build()

	// Start versioned poller in background
	go func() {
		_, _ = s.FrontendClient().PollWorkflowTaskQueue(ctx, workflowservice.PollWorkflowTaskQueueRequest_builder{
			Namespace: s.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  "versioned-poller",
			DeploymentOptions: deploymentpb.WorkerDeploymentOptions_builder{
				DeploymentName:       deploymentName,
				BuildId:              buildID,
				WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
			}.Build(),
		}.Build())
	}()

	// Validate version is present via matching RPC
	version := deploymentspb.WorkerDeploymentVersion_builder{
		DeploymentName: deploymentName,
		BuildId:        buildID,
	}.Build()
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := s.GetTestCluster().MatchingClient().CheckTaskQueueVersionMembership(
			ctx,
			matchingservice.CheckTaskQueueVersionMembershipRequest_builder{
				NamespaceId:   s.NamespaceID().String(),
				TaskQueue:     taskQueueName,
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				Version:       version,
			}.Build(),
		)
		a.NoError(err)
		a.True(resp.GetIsMember())
	}, 10*time.Second, 100*time.Millisecond)
}

// getLatestRunsForWorkflows gets the latest run IDs for the given workflow IDs
func (s *WorkflowResetSuite) getLatestRunsForWorkflows(ctx context.Context, workflowIDs []string) []string {
	var runIDs []string
	for _, workflowID := range workflowIDs {
		// Describe the workflow to get the latest run
		info, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowID, "")
		s.NoError(err)
		runIDs = append(runIDs, info.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	}
	return runIDs
}
