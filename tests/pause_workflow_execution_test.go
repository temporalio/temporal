package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type PauseWorkflowExecutionSuite struct {
	parallelsuite.Suite[*PauseWorkflowExecutionSuite]
}

type pauseWorkflowExecutionEnv struct {
	*testcore.TestEnv

	testEndSignal string
	pauseIdentity string
	pauseReason   string

	workflowFn      func(ctx workflow.Context) (string, error)
	childWorkflowFn func(ctx workflow.Context) (string, error)
	activityFn      func(ctx context.Context) (string, error)

	activityCompletedCh   chan struct{}
	activityCompletedOnce sync.Once

	activityShouldSucceed atomic.Bool
}

func TestPauseWorkflowExecutionSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &PauseWorkflowExecutionSuite{})
}

// newTestEnv creates a TestEnv with the dynamic config this suite needs and
// registers the workflows/activities the tests use.
func (s *PauseWorkflowExecutionSuite) newTestEnv(opts ...testcore.TestOption) *pauseWorkflowExecutionEnv {
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.WorkflowPauseEnabled, true),
	}
	testEnv := testcore.NewEnv(s.T(), append(baseOpts, opts...)...)

	env := &pauseWorkflowExecutionEnv{
		TestEnv:             testEnv,
		testEndSignal:       "test-end",
		pauseIdentity:       "functional-test",
		pauseReason:         "pausing workflow for acceptance test",
		activityCompletedCh: make(chan struct{}, 1),
	}

	env.workflowFn = func(ctx workflow.Context) (string, error) {
		env.Logger.Debug("workflow started")
		ao := workflow.ActivityOptions{
			StartToCloseTimeout:    5 * time.Second,
			ScheduleToCloseTimeout: 10 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var activityResult string
		env.Logger.Debug("executing activity")
		if err := workflow.ExecuteActivity(ctx, env.activityFn).Get(ctx, &activityResult); err != nil {
			return "", err
		}

		var childResult string
		env.Logger.Debug("executing child workflow")
		if err := workflow.ExecuteChildWorkflow(ctx, env.childWorkflowFn).Get(ctx, &childResult); err != nil {
			return "", err
		}

		env.Logger.Debug("waiting to receive signal to complete the workflow")
		signalCh := workflow.GetSignalChannel(ctx, env.testEndSignal)
		var signalPayload string
		signalCh.Receive(ctx, &signalPayload)
		env.Logger.Debug("signal received to complete the workflow")
		return signalPayload + activityResult + childResult, nil
	}

	env.childWorkflowFn = func(ctx workflow.Context) (string, error) {
		return "child-workflow", nil
	}

	env.activityFn = func(ctx context.Context) (string, error) {
		env.Logger.Debug("activity started")
		env.activityCompletedOnce.Do(func() {
			// blocks until the test case unblocks the activity.
			<-env.activityCompletedCh
		})
		env.Logger.Debug("activity completed")
		return "activity", nil
	}

	env.SdkWorker().RegisterWorkflow(env.workflowFn)
	env.SdkWorker().RegisterWorkflow(env.childWorkflowFn)
	env.SdkWorker().RegisterActivity(env.activityFn)

	// Setup for TestPauseWorkflowAndActivity
	env.activityShouldSucceed.Store(false)
	env.SdkWorker().RegisterWorkflow(env.workflowWithFailingActivity)
	env.SdkWorker().RegisterActivity(env.failingActivity)

	return env
}

// failingActivity is an activity that fails until activityShouldSucceed is set to true.
func (env *pauseWorkflowExecutionEnv) failingActivity(ctx context.Context) (string, error) {
	if env.activityShouldSucceed.Load() {
		return "activity-completed", nil
	}
	return "", errors.New("activity-failure")
}

// workflowWithFailingActivity is a workflow that executes the failing activity.
func (env *pauseWorkflowExecutionEnv) workflowWithFailingActivity(ctx workflow.Context) (string, error) {
	ao := workflow.ActivityOptions{
		ActivityID:             "failing-activity",
		StartToCloseTimeout:    5 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,
			BackoffCoefficient: 1,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var activityResult string
	if err := workflow.ExecuteActivity(ctx, env.failingActivity).Get(ctx, &activityResult); err != nil {
		return "", err
	}

	return activityResult, nil
}

// TestPauseUnpauseWorkflowExecution tests that the pause and unpause workflow execution APIs work as expected.
// Test sequence:
// 1. Start the workflow.
// 2. Pause the workflow. Assert that the workflow is paused.
// 3. Send signal to the workflow. Assert that the workflow is still paused.
// 4. Unpause the workflow. Assert that the workflow is now running.
// 5. Unblock the activity to complete the workflow.
// 6. Assert that the workflow is completed.
func (s *PauseWorkflowExecutionSuite) TestPauseUnpauseWorkflowExecution() {
	env := s.newTestEnv()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		// Wait for the workflow task to be processed and the activity to be scheduled,
		// so that a subsequent pause request is applied to a fully initialized workflow.
		require.NotEmpty(t, desc.PendingActivities)
	}, 5*time.Second, 100*time.Millisecond)

	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}

	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// ensure that the workflow is paused
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Send signal to the workflow to complete the workflow. Since the workflow is paused, it should stay paused.
	err = env.SdkClient().SignalWorkflow(s.Context(), workflowID, runID, env.testEndSignal, "signal to complete the workflow")
	s.NoError(err)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Unpause the workflow.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// Workflow status should be running after unpausing. The workflow won't complete until the activity is unblocked.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus(), "workflow is not running. Status: %s", info.GetStatus())

		// Verify TemporalPauseInfo search attribute is removed after unpause
		searchAttrs := info.GetSearchAttributes()
		if searchAttrs != nil {
			pauseInfoPayload, hasPauseInfo := searchAttrs.GetIndexedFields()["TemporalPauseInfo"]
			if hasPauseInfo && pauseInfoPayload != nil {
				var pauseInfoEntries []string
				err = payload.Decode(pauseInfoPayload, &pauseInfoEntries)
				require.NoError(t, err)
				require.Empty(t, pauseInfoEntries, "TemporalPauseInfo should be empty after unpause")
			}
		}
	}, 5*time.Second, 200*time.Millisecond)

	// Unblock the activity to complete the workflow.
	env.SendToChannel(env.activityCompletedCh)

	// assert that the workflow completes now.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus(), "workflow is not completed. Status: %s", info.GetStatus())
	}, 10*time.Second, 200*time.Millisecond)
}

// TestPauseWorkflowAndActivity tests the coexistence of workflow and activity pause entries in TemporalPauseInfo.
// 1. Start a workflow with a failing activity
// 2. Pause the activity
// 3. Pause the workflow
// 4. Verify TemporalPauseInfo contains both workflow and activity pause entries
// 5. Unblock the failing activity and unpause it
// 6. Verify activity completes but workflow remains paused (only workflow pause entries in TemporalPauseInfo)
// 7. Unpause the workflow
// 8. Verify workflow completes successfully
func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowAndActivity() {
	env := s.newTestEnv()

	// Reset the activity success flag for this test
	env.activityShouldSucceed.Store(false)

	// This matches the activity ID defined in workflowWithFailingActivity
	activityID := "failing-activity"

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-and-activity-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowWithFailingActivity)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Wait for activity to start and fail at least once
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.NotNil(t, desc.PendingActivities[0].LastFailure)
	}, 5*time.Second, 200*time.Millisecond)

	// Pause the activity
	pauseActivityRequest := &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Activity: &workflowservice.PauseActivityRequest_Id{Id: activityID},
		Identity: env.pauseIdentity,
		Reason:   "pausing activity for test",
	}
	pauseActivityResp, err := env.FrontendClient().PauseActivity(s.Context(), pauseActivityRequest)
	s.NoError(err)
	s.NotNil(pauseActivityResp)

	// Wait for activity to be paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.True(t, desc.PendingActivities[0].Paused)
	}, 5*time.Second, 200*time.Millisecond)

	// Pause the workflow
	pauseWorkflowRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseWorkflowResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseWorkflowRequest)
	s.NoError(err)
	s.NotNil(pauseWorkflowResp)

	// Verify both workflow and activity are paused, and TemporalPauseInfo contains entries for both
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		// Use existing helper to verify workflow is paused (includes workflow pause search attribute checks)
		s.assertWorkflowIsPaused(env, workflowID, runID)

		// Additionally verify activity is paused
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Len(desc.PendingActivities, 1)
		s.True(desc.PendingActivities[0].Paused)

		// Additionally verify TemporalPauseInfo contains activity pause entry
		s.True(env.hasActivityPauseEntries(desc), "Should contain at least one activity pause entry")
	}, 5*time.Second, 200*time.Millisecond)

	// Unblock the failing activity so it can succeed
	env.activityShouldSucceed.Store(true)

	// Unpause the activity
	unpauseActivityRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Activity: &workflowservice.UnpauseActivityRequest_Id{Id: activityID},
		Identity: env.pauseIdentity,
	}
	unpauseActivityResp, err := env.FrontendClient().UnpauseActivity(s.Context(), unpauseActivityRequest)
	s.NoError(err)
	s.NotNil(unpauseActivityResp)

	// Verify activity completes but workflow remains paused
	// TemporalPauseInfo should only contain workflow pause entries (activity entries removed)
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		// Use existing helper to verify workflow is still paused (includes workflow pause search attribute checks)
		s.assertWorkflowIsPaused(env, workflowID, runID)

		// Additionally verify activity is no longer pending (completed)
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		s.NoError(err)
		s.Empty(desc.PendingActivities, "Activity should have completed")

		// Verify the activity completed successfully by checking workflow history
		// Since this workflow only has one activity, the presence of ActivityTaskCompleted confirms success
		hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		activityCompleted := false
		for hist.HasNext() {
			event, err := hist.Next()
			s.NoError(err)
			if event.EventType == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
				activityCompleted = true
				break
			}
		}
		s.True(activityCompleted, "Activity should have completed successfully")

		// Additionally verify TemporalPauseInfo no longer contains activity pause entries
		s.False(env.hasActivityPauseEntries(desc), "Should not contain activity pause entries")
	}, 5*time.Second, 200*time.Millisecond)

	// Unpause the workflow
	unpauseWorkflowRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseWorkflowResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseWorkflowRequest)
	s.NoError(err)
	s.NotNil(unpauseWorkflowResp)

	// Verify workflow completes successfully
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus(), "workflow is not completed. Status: %s", info.GetStatus())

		// Verify TemporalPauseInfo is empty after unpause
		searchAttrs := info.GetSearchAttributes()
		if searchAttrs != nil {
			pauseInfoPayload, hasPauseInfo := searchAttrs.GetIndexedFields()["TemporalPauseInfo"]
			if hasPauseInfo && pauseInfoPayload != nil {
				var pauseInfoEntries []string
				err = payload.Decode(pauseInfoPayload, &pauseInfoEntries)
				require.NoError(t, err)
				require.Empty(t, pauseInfoEntries, "TemporalPauseInfo should be empty after workflow completes")
			}
		}
	}, 10*time.Second, 200*time.Millisecond)
}

// TestUnpauseWorkflowKeepsActivityPaused tests that unpausing a workflow does not unpause its paused activities.
// 1. Start a workflow with a failing activity
// 2. Pause the activity
// 3. Pause the workflow
// 4. Unpause the workflow (while the activity is still paused)
// 5. Verify the activity remains paused and the workflow is running
func (s *PauseWorkflowExecutionSuite) TestUnpauseWorkflowKeepsActivityPaused() {
	env := s.newTestEnv()

	env.activityShouldSucceed.Store(false)

	activityID := "failing-activity"

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("unpause-wf-keeps-activity-paused-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowWithFailingActivity)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Wait for activity to fail at least once
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.NotNil(t, desc.PendingActivities[0].LastFailure)
	}, 5*time.Second, 200*time.Millisecond)

	// Pause the activity
	_, err = env.FrontendClient().PauseActivity(s.Context(), &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: activityID},
		Identity:  env.pauseIdentity,
		Reason:    "pausing activity for test",
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.True(t, desc.PendingActivities[0].Paused)
	}, 5*time.Second, 200*time.Millisecond)

	// Pause the workflow
	_, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Unpause the workflow only
	_, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// Verify the workflow is running but the activity remains paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus(),
			"workflow should be running after unpause, got: %s", info.GetStatus())
		require.Len(t, desc.PendingActivities, 1)
		require.True(t, desc.PendingActivities[0].Paused, "activity should still be paused after workflow unpause")
	}, 5*time.Second, 200*time.Millisecond)

	// Cleanup: unblock and unpause the activity so the workflow can complete
	env.activityShouldSucceed.Store(true)
	_, err = env.FrontendClient().UnpauseActivity(s.Context(), &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Activity:  &workflowservice.UnpauseActivityRequest_Id{Id: activityID},
		Identity:  env.pauseIdentity,
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 10*time.Second, 200*time.Millisecond)
}

func (s *PauseWorkflowExecutionSuite) TestQueryWorkflowWhenPaused() {
	env := s.newTestEnv()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Wait for the workflow task to be processed and the activity to be scheduled,
	// so that a subsequent pause request is applied to a fully initialized workflow.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		require.NotEmpty(t, desc.PendingActivities)
	}, 5*time.Second, 100*time.Millisecond)

	// Pause the workflow.
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// Wait until paused.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// Issue a query to the paused workflow. It should return QueryRejected with WORKFLOW_EXECUTION_STATUS_PAUSED status.
	queryReq := &workflowservice.QueryWorkflowRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Query: &querypb.WorkflowQuery{
			QueryType: "__stack_trace",
		},
	}
	queryResp, err := env.FrontendClient().QueryWorkflow(s.Context(), queryReq)
	s.NoError(err)
	s.NotNil(queryResp)
	s.NotNil(queryResp.GetQueryRejected())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, queryResp.GetQueryRejected().GetStatus())

	// Unpause the workflow.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// Unblock the activity and send the signal to complete the workflow.
	env.SendToChannel(env.activityCompletedCh)
	err = env.SdkClient().SignalWorkflow(s.Context(), workflowID, runID, env.testEndSignal, "test end signal")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
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
	env := s.newTestEnv()

	namespaceName := env.Namespace().String()

	// fails when the identity is too long.
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   strings.Repeat("x", 2000),
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	resp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
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
		RunId:      uuid.NewString(),
		Identity:   env.pauseIdentity,
		Reason:     strings.Repeat("x", 2000),
		RequestId:  uuid.NewString(),
	}
	resp, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "reason is too long.")

	// fails when the request id is too long.
	pauseRequest = &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  strings.Repeat("x", 2000),
	}
	resp, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "request id is too long.")

	// fails when the dynamic config is disabled.
	env.OverrideDynamicConfig(dynamicconfig.WorkflowPauseEnabled, false)
	pauseRequest = &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	resp, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.Error(err)
	s.Nil(resp)
	var unimplementedErr *serviceerror.Unimplemented
	s.ErrorAs(err, &unimplementedErr)
	s.NotNil(unimplementedErr)
	s.Contains(unimplementedErr.Error(), namespaceName)
}

// TestUnpauseWorkflowExecutionRequestValidation tests unpause workflow execution request validation. We don't need a valid workflow.
// - fails when the identity is too long.
// - fails when the reason is too long.
// - fails when the request id is too long.
func (s *PauseWorkflowExecutionSuite) TestUnpauseWorkflowExecutionRequestValidation() {
	env := s.newTestEnv()

	namespaceName := env.Namespace().String()

	// fails when the identity is too long.
	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   strings.Repeat("x", 2000),
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	resp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.Error(err)
	s.Nil(resp)
	var invalidArgumentErr *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "identity is too long.")

	// fails when the reason is too long.
	unpauseRequest = &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   env.pauseIdentity,
		Reason:     strings.Repeat("x", 2000),
		RequestId:  uuid.NewString(),
	}
	resp, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "reason is too long.")

	// fails when the request id is too long.
	unpauseRequest = &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  namespaceName,
		WorkflowId: "test-workflow-id",
		RunId:      uuid.NewString(),
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  strings.Repeat("x", 2000),
	}
	resp, err = env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.Error(err)
	s.Nil(resp)
	s.ErrorAs(err, &invalidArgumentErr)
	s.NotNil(invalidArgumentErr)
	s.Contains(invalidArgumentErr.Error(), "request id is too long.")
}

func (s *PauseWorkflowExecutionSuite) TestPauseWorkflowExecutionAlreadyPaused() {
	env := s.newTestEnv()

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-wf-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, env.workflowFn)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		// Wait for the workflow task to be processed and the activity to be scheduled,
		// so that a subsequent pause request is applied to a fully initialized workflow.
		require.NotEmpty(t, desc.PendingActivities)
	}, 5*time.Second, 100*time.Millisecond)

	// 1st pause request should succeed.
	pauseRequest := &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	}
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// Wait until paused.
	s.Await(func(s *PauseWorkflowExecutionSuite) {
		s.assertWorkflowIsPaused(env, workflowID, runID)
	}, 5*time.Second, 200*time.Millisecond)

	// 2nd pause request should fail with failed precondition error.
	pauseRequest.RequestId = uuid.NewString()
	pauseResp, err = env.FrontendClient().PauseWorkflowExecution(s.Context(), pauseRequest)
	s.Error(err)
	s.Nil(pauseResp)
	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionErr)
	s.NotNil(failedPreconditionErr)
	s.Contains(failedPreconditionErr.Error(), "workflow is already paused.")

	unpauseRequest := &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     "cleanup after paused workflow test",
		RequestId:  uuid.NewString(),
	}
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		require.Nil(t, desc.GetWorkflowExtendedInfo().GetPauseInfo())
	}, 5*time.Second, 200*time.Millisecond)

	// Unblock the activity and send the signal to complete the workflow.
	env.SendToChannel(env.activityCompletedCh)
	err = env.SdkClient().SignalWorkflow(s.Context(), workflowID, runID, env.testEndSignal, "test end signal")
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus())
	}, 5*time.Second, 200*time.Millisecond)
}

// TestPauseDuringInFlightWorkflowTask reproduces the race described in
// https://github.com/temporalio/temporal/issues/10239: if
// PauseWorkflowExecution arrives while a worker has a workflow task in flight,
// the worker's RespondWorkflowTaskCompleted can still be accepted after the
// WORKFLOW_EXECUTION_PAUSED event is appended. A follow-up WORKFLOW_TASK_SCHEDULED
// is then written and the next workflow task completion resets Status to RUNNING
// without clearing executionInfo.PauseInfo. The workflow ends up stuck:
// Status=RUNNING with pauseInfo set, and UnpauseWorkflowExecution rejects with
// FailedPrecondition because it only inspects Status.
//
// The bug is timing-sensitive. It often does not reproduce on a single run.
// Run with -count=N (e.g. -count=50) to reliably observe failures.
func (s *PauseWorkflowExecutionSuite) TestPauseDuringInFlightWorkflowTask() {
	env := s.newTestEnv()

	const (
		tickActivityName = "pause-race-tick-activity"
		busyWorkflowName = "pause-race-busy-workflow"
		iterations       = 200
	)

	// tickActivity completes immediately so the workflow keeps cycling
	// through workflow tasks with minimal wall time between them.
	tickActivity := func(ctx context.Context) error {
		return nil
	}

	// busyWorkflow runs a long tight loop of short activities so that workflow
	// tasks are being scheduled/started/completed continuously. This widens
	// the chance of a Pause RPC arriving while a WT is in flight on the worker.
	busyWorkflow := func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout:    5 * time.Second,
			ScheduleToCloseTimeout: 30 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		for range iterations {
			if err := workflow.ExecuteActivity(ctx, tickActivityName).Get(ctx, nil); err != nil {
				return err
			}
		}
		return nil
	}

	env.SdkWorker().RegisterWorkflowWithOptions(busyWorkflow, workflow.RegisterOptions{Name: busyWorkflowName})
	env.SdkWorker().RegisterActivityWithOptions(tickActivity, activity.RegisterOptions{Name: tickActivityName})

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("pause-race-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, busyWorkflowName)
	s.NoError(err)
	workflowID := workflowRun.GetID()
	runID := workflowRun.GetRunID()

	// Wait until the workflow has progressed a few iterations so workflow
	// tasks are actively flowing through the worker.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, info.GetStatus())
		require.GreaterOrEqual(t, info.GetHistoryLength(), int64(15),
			"workflow has not started cycling through tasks yet")
	}, 10*time.Second, 50*time.Millisecond)

	// Issue the Pause while the worker is still busy. Repeat until either we
	// observe Status=PAUSED or we hit the desync state (Status=RUNNING with
	// pauseInfo set). The race window is small, so we don't always hit it on
	// the first pause/unpause cycle.
	pauseResp, err := env.FrontendClient().PauseWorkflowExecution(s.Context(), &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)
	s.NotNil(pauseResp)

	// Eventually the workflow should reach a stable PAUSED state. The bug
	// manifests as Status=RUNNING with pauseInfo still populated; this assertion
	// is what fails when the race fires.
	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
		require.NoError(t, err)
		info := desc.GetWorkflowExecutionInfo()
		require.NotNil(t, info)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, info.GetStatus(),
			"workflow ended up desynced: Status=%s, pauseInfo=%v (issue #10239 race)",
			info.GetStatus(), desc.GetWorkflowExtendedInfo().GetPauseInfo())
	}, 10*time.Second, 100*time.Millisecond)

	// Verify history contains no WORKFLOW_TASK_SCHEDULED event after
	// WORKFLOW_EXECUTION_PAUSED — that event (eventId #1963 in the issue
	// reproduction) is the smoking gun for the race.
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	inPaused := false
	for hist.HasNext() {
		event, herr := hist.Next()
		s.NoError(herr)
		switch event.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED:
			inPaused = true
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED:
			inPaused = false
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED:
			s.False(inPaused,
				"WORKFLOW_TASK_SCHEDULED at eventId=%d appended after WORKFLOW_EXECUTION_PAUSED (issue #10239 race)",
				event.EventId)
		default:
		}
	}

	// Unpause should succeed. When the race fires, Status is RUNNING and the
	// unpause API rejects with FailedPrecondition: "workflow is not paused".
	unpauseResp, err := env.FrontendClient().UnpauseWorkflowExecution(s.Context(), &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: workflowID,
		RunId:      runID,
		Identity:   env.pauseIdentity,
		Reason:     env.pauseReason,
		RequestId:  uuid.NewString(),
	})
	s.NoError(err, "UnpauseWorkflowExecution failed; workflow is stuck with Status=RUNNING and pauseInfo set (issue #10239 race)")
	s.NotNil(unpauseResp)

	// Cleanup: terminate so the busy loop doesn't run to completion.
	_, _ = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Reason: "cleanup after pause race test",
	})
}

// assertWorkflowIsPaused is a helper method which asserts that,
// - the workflow status is paused.
// - the workflow has the correct pause info.
// - the TemporalPauseInfo search attribute contains workflow pause entries.
// - there is no workflow task scheduled event inbetween pause and unpause events.
func (s *PauseWorkflowExecutionSuite) assertWorkflowIsPaused(env *pauseWorkflowExecutionEnv, workflowID string, runID string) {
	desc, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowID, runID)
	s.NoError(err)
	info := desc.GetWorkflowExecutionInfo()
	s.NotNil(info)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, info.GetStatus(), "workflow is not paused. Status: %s", info.GetStatus())
	if pauseInfo := desc.GetWorkflowExtendedInfo().GetPauseInfo(); pauseInfo != nil {
		s.Equal(env.pauseIdentity, pauseInfo.GetIdentity(), "pause identity is not correct")
		s.Equal(env.pauseReason, pauseInfo.GetReason(), "pause reason is not correct")
	}

	// Verify TemporalPauseInfo search attribute is set with workflow pause entries
	searchAttrs := info.GetSearchAttributes()
	s.NotNil(searchAttrs, "Search attributes should not be nil")
	pauseInfoPayload, hasPauseInfo := searchAttrs.GetIndexedFields()["TemporalPauseInfo"]
	s.True(hasPauseInfo, "TemporalPauseInfo search attribute should exist")
	s.NotNil(pauseInfoPayload)
	var pauseInfoEntries []string
	err = payload.Decode(pauseInfoPayload, &pauseInfoEntries)
	s.NoError(err)
	s.Contains(pauseInfoEntries, fmt.Sprintf("Workflow:%s", workflowID), "Should contain workflow ID")
	s.Contains(pauseInfoEntries, "Reason:"+env.pauseReason, "Should contain pause reason")

	// Also assert that there is no workflow task scheduled event after the pause event.
	hist := env.SdkClient().GetWorkflowHistory(s.Context(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	isPaused := false
	for hist.HasNext() {
		event, err := hist.Next()
		s.NoError(err)
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED {
			isPaused = true
			continue
		}
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED {
			isPaused = false
			continue
		}
		if isPaused {
			// These are all events after the pause event. None of these should be workflow task scheduled events.
			s.NotEqual(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, event.EventType)
		}
	}
}

// hasActivityPauseEntries checks if the TemporalPauseInfo search attribute contains any activity pause entries.
func (env *pauseWorkflowExecutionEnv) hasActivityPauseEntries(desc *workflowservice.DescribeWorkflowExecutionResponse) bool {
	searchAttrs := desc.GetWorkflowExecutionInfo().GetSearchAttributes()
	if searchAttrs == nil {
		return false
	}

	pauseInfoPayload, hasPauseInfo := searchAttrs.GetIndexedFields()["TemporalPauseInfo"]
	if !hasPauseInfo || pauseInfoPayload == nil {
		return false
	}

	var pauseInfoEntries []string
	if err := payload.Decode(pauseInfoPayload, &pauseInfoEntries); err != nil {
		return false
	}

	for _, entry := range pauseInfoEntries {
		if strings.HasPrefix(entry, "property:activityType=") {
			return true
		}
	}
	return false
}
