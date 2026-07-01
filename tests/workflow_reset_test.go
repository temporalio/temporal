package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Tests workflow reset feature
type WorkflowResetSuite struct {
	parallelsuite.Suite[*WorkflowResetSuite]
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
	parallelsuite.Run(t, &WorkflowResetSuite{})
}

// No explicit base run provided. current run is still running.
func (s *WorkflowResetSuite) TestNoBaseCurrentRunning() {
	env := testcore.NewEnv(s.T())
	workflowID := env.Tv().WorkflowID()
	runIDs := s.prepareWorkflowRuns(env, workflowID, 1, true, versioningConfig{})
	currentRunID := runIDs[0]

	// Reset the current run (i.e don't give an explicit runID)
	resp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(env, workflowID, currentRunID),
	})
	s.NoError(err)
	newRunID := resp.RunId

	// Current run is the assumed base run. The new run should be linked to this one.
	currentMutableState, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: currentRunID},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.Equal(currentMutableState.GetDatabaseMutableState().ExecutionInfo.ResetRunId, newRunID)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, currentMutableState.GetDatabaseMutableState().ExecutionState.Status)
}

// No explicit base run provided. current run is closed.
func (s *WorkflowResetSuite) TestNoBaseCurrentClosed() {
	env := testcore.NewEnv(s.T())
	workflowID := env.Tv().WorkflowID()
	runIDs := s.prepareWorkflowRuns(env, workflowID, 1, false, versioningConfig{})
	currentRunID := runIDs[0]

	// Reset the current run (i.e don't give an explicit runID)
	resp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(env, workflowID, currentRunID),
	})
	s.NoError(err)
	newRunID := resp.RunId

	// Current run is the assumed base run. The new run should be linked to this one.
	currentMutableState, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: currentRunID},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.Equal(currentMutableState.GetDatabaseMutableState().ExecutionInfo.ResetRunId, newRunID)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, currentMutableState.GetDatabaseMutableState().ExecutionState.Status)
}

// Explicit base run is provided to be reset and its the same as currently running execution.
func (s *WorkflowResetSuite) TestSameBaseCurrentRunning() {
	env := testcore.NewEnv(s.T())
	workflowID := env.Tv().WorkflowID()
	runIDs := s.prepareWorkflowRuns(env, workflowID, 1, true, versioningConfig{})
	baseRunID := runIDs[0]
	currentRunID := runIDs[0]

	newRunID := s.performReset(env, workflowID, baseRunID)

	s.assertResetWorkflowLink(env, workflowID, baseRunID, newRunID)
	s.assertMutableStateStatus(env, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)
}

// Explicit base run is provided. Its the same as current and is in closed state.
func (s *WorkflowResetSuite) TestSameBaseCurrentClosed() {
	env := testcore.NewEnv(s.T())
	workflowID := env.Tv().WorkflowID()
	runIDs := s.prepareWorkflowRuns(env, workflowID, 1, false, versioningConfig{})
	baseRunID := runIDs[0]
	currentRunID := runIDs[0]

	newRunID := s.performReset(env, workflowID, baseRunID)
	s.assertResetWorkflowLink(env, workflowID, baseRunID, newRunID)
	s.assertMutableStateStatus(env, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
}

// Explicit base run is provided. It is different from the currently running execution.
func (s *WorkflowResetSuite) TestDifferentBaseCurrentRunning() {
	env := testcore.NewEnv(s.T())
	workflowID := env.Tv().WorkflowID()
	runIDs := s.prepareWorkflowRuns(env, workflowID, 2, true, versioningConfig{})
	baseRunID := runIDs[0]
	currentRunID := runIDs[1]

	newRunID := s.performReset(env, workflowID, baseRunID)
	s.assertResetWorkflowLink(env, workflowID, baseRunID, newRunID)
	s.assertMutableStateStatus(env, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)
}

// Explicit base run is provided. It is different from the current run which in closed state.
func (s *WorkflowResetSuite) TestDifferentBaseCurrentClosed() {
	env := testcore.NewEnv(s.T())
	workflowID := env.Tv().WorkflowID()
	runIDs := s.prepareWorkflowRuns(env, workflowID, 2, false, versioningConfig{})
	baseRunID := runIDs[0]
	currentRunID := runIDs[1]

	newRunID := s.performReset(env, workflowID, baseRunID)
	s.assertResetWorkflowLink(env, workflowID, baseRunID, newRunID)
	s.assertMutableStateStatus(env, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
}

// Base is reset multuple times. Assert that each time it point to the new run.
func (s *WorkflowResetSuite) TestRepeatedResets() {
	env := testcore.NewEnv(s.T())
	workflowID := env.Tv().WorkflowID()
	runIDs := s.prepareWorkflowRuns(env, workflowID, 2, false, versioningConfig{})
	baseRunID := runIDs[0]
	currentRunID := runIDs[1]

	newRunID1 := s.performReset(env, workflowID, baseRunID)
	s.assertResetWorkflowLink(env, workflowID, baseRunID, newRunID1)
	s.assertMutableStateStatus(env, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)

	// reset again and ensure the pointer in base is also updated.
	newRunID2 := s.performReset(env, workflowID, baseRunID)
	s.assertResetWorkflowLink(env, workflowID, baseRunID, newRunID2)                                     // base -> newRunID2
	s.assertMutableStateStatus(env, workflowID, newRunID1, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED) // newRunID1 was the current run.
}

// Explicit base run is provided. There are more closed runs between base and the current run. Asserts that no other runs apart from base & current are mutated.
func (s *WorkflowResetSuite) TestWithMoreClosedRuns() {
	env := testcore.NewEnv(s.T())
	workflowID := env.Tv().WorkflowID()
	runIDs := s.prepareWorkflowRuns(env, workflowID, 5, false, versioningConfig{})
	baseRunID := runIDs[0]
	currentRunID := runIDs[4]
	noChangeRuns := runIDs[1:4]

	newRunID := s.performReset(env, workflowID, baseRunID)
	s.assertResetWorkflowLink(env, workflowID, baseRunID, newRunID)
	s.assertMutableStateStatus(env, workflowID, currentRunID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)

	// assert that these runs don't have any links and their status remains completed.
	for _, noChangeRunID := range noChangeRuns {
		s.assertResetWorkflowLink(env, workflowID, noChangeRunID, "") // empty link
		s.assertMutableStateStatus(env, workflowID, noChangeRunID, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	}
}

func (s *WorkflowResetSuite) TestOriginalExecutionRunId() {
	env := testcore.NewEnv(s.T())
	workflowID := env.Tv().WorkflowID()
	runIDs := s.prepareWorkflowRuns(env, workflowID, 1, true, versioningConfig{})
	baseRunID := runIDs[0]
	// Reset the current run repeatedly. Verify that each time the new run points to the original baseRunID
	for range 5 {
		currentRunID := s.performReset(env, workflowID, baseRunID)
		baseMutableState, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: currentRunID},
			Archetype: chasm.WorkflowArchetype,
		})
		s.NoError(err)
		s.Equal(baseRunID, baseMutableState.GetDatabaseMutableState().ExecutionInfo.OriginalExecutionRunId)
	}
}

// Test that the workflow options are updated when the workflow is reset.
func (s *WorkflowResetSuite) TestResetWorkflowWithOptionsUpdate() {
	env := testcore.NewEnv(s.T(), testcore.WithWorkerService())
	deploymentName := "testing"
	buildID := "v.123"

	// Setup runs with versioning enabled so that the version is present in the task queue before the
	// versioning override is set.
	workflowID := env.Tv().WorkflowID()
	runIDs := s.prepareWorkflowRuns(env, workflowID, 1, true, versioningConfig{
		Required:       true,
		DeploymentName: deploymentName,
		BuildID:        buildID,
	})
	currentRunID := runIDs[0]

	override := &workflowpb.VersioningOverride{
		Override: &workflowpb.VersioningOverride_Pinned{
			Pinned: &workflowpb.VersioningOverride_PinnedOverride{
				Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
				Version: &deploymentpb.WorkerDeploymentVersion{
					DeploymentName: deploymentName,
					BuildId:        buildID,
				},
			},
		},
	}

	// Reset the workflow by providing the explicit runID (base run) to reset.
	resp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: currentRunID},
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(env, workflowID, currentRunID),
		PostResetOperations: []*workflowpb.PostResetOperation{
			{
				Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
					UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
						WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
							VersioningOverride: override,
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
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, newRunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			optionsUpdatedEvent = event
			break
		}
	}
	s.NotNil(optionsUpdatedEvent)
	s.ProtoEqual(override, optionsUpdatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetVersioningOverride())

	info, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, newRunID)
	s.NoError(err)

	// TODO (Carly): remove deprecated values from verification once we stop populating them
	override.Behavior = enumspb.VERSIONING_BEHAVIOR_PINNED  //nolint:staticcheck
	override.PinnedVersion = deploymentName + "." + buildID //nolint:staticcheck
	s.ProtoEqual(override, info.WorkflowExecutionInfo.GetVersioningInfo().GetVersioningOverride())
}

// Test batch reset operation with version update as post reset operation
func (s *WorkflowResetSuite) TestBatchResetWithOptionsUpdate() {
	env := testcore.NewEnv(s.T(), testcore.WithWorkerService())
	deploymentName := "batch-testing"
	buildID := "v.456"

	// Setup runs
	versioningConfig := versioningConfig{Required: true, DeploymentName: deploymentName, BuildID: buildID}
	workflowID1 := env.Tv().Sub("workflow-1").WorkflowID()
	runIDs1 := s.prepareWorkflowRuns(env, workflowID1, 1, true, versioningConfig)
	workflowID2 := env.Tv().Sub("workflow-2").WorkflowID()
	runIDs2 := s.prepareWorkflowRuns(env, workflowID2, 1, true, versioningConfig)

	// Create versioning override for post-reset operations
	override := &workflowpb.VersioningOverride{
		Override: &workflowpb.VersioningOverride_Pinned{
			Pinned: &workflowpb.VersioningOverride_PinnedOverride{
				Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
				Version: &deploymentpb.WorkerDeploymentVersion{
					DeploymentName: deploymentName,
					BuildId:        buildID,
				},
			},
		},
	}

	// Start batch reset operation
	batchJobID := "batch-reset-job-" + uuid.NewString()
	_, err := env.FrontendClient().StartBatchOperation(s.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		JobId:     batchJobID,
		Reason:    "testing-batch-reset-with-options",
		Executions: []*commonpb.WorkflowExecution{
			{WorkflowId: workflowID1, RunId: runIDs1[0]},
			{WorkflowId: workflowID2, RunId: runIDs2[0]},
		},
		Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
			ResetOperation: &batchpb.BatchOperationReset{
				Identity: "test-batch-reset",
				Options: &commonpb.ResetOptions{
					Target: &commonpb.ResetOptions_WorkflowTaskId{
						WorkflowTaskId: s.getFirstWFTaskCompleteEventID(env, workflowID1, runIDs1[0]),
					},
				},
				PostResetOperations: []*workflowpb.PostResetOperation{
					{
						Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
							UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
								WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
									VersioningOverride: override,
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
			},
		},
	})
	s.NoError(err)

	// Wait for batch operation to complete
	s.Eventually(func() bool {
		resp, err := env.FrontendClient().DescribeBatchOperation(s.Context(), &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     batchJobID,
		})
		if err != nil {
			return false
		}
		return resp.State == enumspb.BATCH_OPERATION_STATE_COMPLETED
	}, 20*time.Second, 1*time.Second, "Batch operation should complete")

	// Get the new run IDs after reset
	// The workflows should be terminated and new runs started
	newWorkflows := s.getLatestRunsForWorkflows(env, []string{workflowID1, workflowID2})
	s.Len(newWorkflows, 2)

	// Verify both workflows have the options updated event and correct versioning override
	for i, workflowID := range []string{workflowID1, workflowID2} {
		newRunID := newWorkflows[i]

		// Find the options updated event in history
		var optionsUpdatedEvent *historypb.HistoryEvent
		hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, newRunID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for hist.HasNext() {
			event, err := hist.Next()
			s.NoError(err)
			if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
				optionsUpdatedEvent = event
				break
			}
		}
		s.NotNil(optionsUpdatedEvent, "Workflow %s should have options updated event", workflowID)
		s.ProtoEqual(override, optionsUpdatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetVersioningOverride())

		// Verify the workflow execution info has the correct versioning override
		info, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, newRunID)
		s.NoError(err)

		expectedOverride := &workflowpb.VersioningOverride{
			Override: &workflowpb.VersioningOverride_Pinned{
				Pinned: &workflowpb.VersioningOverride_PinnedOverride{
					Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
					Version: &deploymentpb.WorkerDeploymentVersion{
						DeploymentName: deploymentName,
						BuildId:        buildID,
					},
				},
			},
		}
		s.ProtoEqual(expectedOverride.GetPinned().GetVersion(), info.WorkflowExecutionInfo.GetVersioningInfo().GetVersioningOverride().GetPinned().GetVersion())
	}
}

// Helper methods

// getFirstWFTaskCompleteEventID finds the first event corresponding to workflow task completion. This can be used as a good reset point for tests in this suite.
func (s *WorkflowResetSuite) getFirstWFTaskCompleteEventID(env *testcore.TestEnv, workflowID string, runID string) int64 {
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			return event.EventId
		}
	}
	s.Failf("couldn't find a workflow task complete event", "workflowID:[%s], runID:[%s]", workflowID, runID)
	return 0
}

// performReset is a helper method to reset the given workflow run and assert that it is successful.
func (s *WorkflowResetSuite) performReset(env *testcore.TestEnv, workflowID string, runID string) string {
	// Reset the workflow by providing the explicit runID (base run) to reset.
	resp, err := env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Reason:                    "testing-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: s.getFirstWFTaskCompleteEventID(env, workflowID, runID),
	})
	s.NoError(err)
	return resp.RunId
}

// assertMutableStateStatus asserts that the mutable state for the given run matches the expected status.
func (s *WorkflowResetSuite) assertMutableStateStatus(env *testcore.TestEnv, workflowID string, runID string, expectedStatus enumspb.WorkflowExecutionStatus) {
	ms, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.Equal(expectedStatus, ms.GetDatabaseMutableState().ExecutionState.Status)
}

// assertResetWorkflowLink asserts that the reset runID is properly recorded in the given run.
func (s *WorkflowResetSuite) assertResetWorkflowLink(env *testcore.TestEnv, workflowID string, runID string, expectedLinkRunID string) {
	baseMutableState, err := env.AdminClient().DescribeMutableState(s.Context(), &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.Equal(expectedLinkRunID, baseMutableState.GetDatabaseMutableState().ExecutionInfo.ResetRunId)
}

// prepareWorkflowRuns prepares a workflow with n runs. If isCurrentRunning is true then the last run is kept open.
func (s *WorkflowResetSuite) prepareWorkflowRuns(env *testcore.TestEnv, workflowID string, n int, isCurrentRunning bool, versioningConfig versioningConfig) []string {
	var runIDs []string

	// If versioning is requested, start a versioned poller and validate version membership
	if versioningConfig.Required {
		s.startVersionedPollerAndValidate(env, versioningConfig.DeploymentName, versioningConfig.BuildID)
	}

	for i := 0; i < n-1; i++ {
		runIDs = append(runIDs, s.prepareSingleRun(env, workflowID, false))
	}
	runIDs = append(runIDs, s.prepareSingleRun(env, workflowID, isCurrentRunning))
	return runIDs
}

func (s *WorkflowResetSuite) prepareSingleRun(env *testcore.TestEnv, workflowID string, isRunning bool) string {
	tv := env.Tv()
	run, err := env.SdkClient().ExecuteWorkflow(s.Context(), client.StartWorkflowOptions{
		TaskQueue: tv.TaskQueue().Name,
		ID:        workflowID,
	}, "test-workflow-arg")
	s.NoError(err)

	pollWTResp, err := env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		TaskToken: pollWTResp.TaskToken,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
				ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
					ActivityId: "test-activity-id",
					TaskQueue:  tv.TaskQueue(),

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

	pollATResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)
	_, err = env.FrontendClient().RespondActivityTaskCompleted(s.Context(), &workflowservice.RespondActivityTaskCompletedRequest{
		TaskToken: pollATResp.TaskToken,
	})
	s.NoError(err)

	pollWTResp, err = env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)

	_, err = env.FrontendClient().RespondWorkflowTaskCompleted(s.Context(), &workflowservice.RespondWorkflowTaskCompletedRequest{
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

// startVersionedPollerAndValidate starts a versioned poller for the given task queue
// and validates that the version is present in the task queue via matching RPC.
func (s *WorkflowResetSuite) startVersionedPollerAndValidate(
	env *testcore.TestEnv,
	deploymentName string,
	buildID string,
) {
	tv := env.Tv()
	// Start versioned poller in background
	go func() {
		_, _ = env.FrontendClient().PollWorkflowTaskQueue(s.Context(), &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: tv.TaskQueue(),
			Identity:  tv.WorkerIdentity(),
			DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
				DeploymentName:       deploymentName,
				BuildId:              buildID,
				WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
			},
		})
	}()

	// Validate version is present via matching RPC
	version := &deploymentspb.WorkerDeploymentVersion{
		DeploymentName: deploymentName,
		BuildId:        buildID,
	}
	s.EventuallyWithT(func(t *assert.CollectT) {
		a := require.New(t)
		resp, err := env.GetTestCluster().MatchingClient().CheckTaskQueueVersionMembership(
			s.Context(),
			&matchingservice.CheckTaskQueueVersionMembershipRequest{
				NamespaceId:   env.NamespaceID().String(),
				TaskQueue:     tv.TaskQueue().Name,
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				Version:       version,
			},
		)
		a.NoError(err)
		a.True(resp.GetIsMember())
	}, 10*time.Second, 100*time.Millisecond)
}

// getLatestRunsForWorkflows gets the latest run IDs for the given workflow IDs
func (s *WorkflowResetSuite) getLatestRunsForWorkflows(env *testcore.TestEnv, workflowIDs []string) []string {
	var runIDs []string
	for _, workflowID := range workflowIDs {
		// Describe the workflow to get the latest run
		info, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, "")
		s.NoError(err)
		runIDs = append(runIDs, info.WorkflowExecutionInfo.Execution.RunId)
	}
	return runIDs
}
