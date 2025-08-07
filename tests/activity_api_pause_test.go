package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
)

type ActivityApiPauseClientTestSuite struct {
	testcore.FunctionalTestBase
	tv                     *testvars.TestVars
	initialRetryInterval   time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration

	activityRetryPolicy *temporal.RetryPolicy
}

func TestActivityApiPauseClientTestSuite(t *testing.T) {
	s := new(ActivityApiPauseClientTestSuite)
	suite.Run(t, s)
}

func (s *ActivityApiPauseClientTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	s.tv = testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(s.Namespace())

	s.initialRetryInterval = 1 * time.Second
	s.scheduleToCloseTimeout = 30 * time.Minute
	s.startToCloseTimeout = 15 * time.Minute

	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}
}

func (s *ActivityApiPauseClientTestSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
	return func(ctx workflow.Context) error {

		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    s.startToCloseTimeout,
			ScheduleToCloseTimeout: s.scheduleToCloseTimeout,
			RetryPolicy:            s.activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)
		return err
	}
}

func (s *ActivityApiPauseClientTestSuite) TestActivityPauseApi_WhileRunning() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityPausedCn := make(chan struct{})
	var startedActivityCount atomic.Int32
	activityErr := errors.New("bad-luck-please-retry")

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		if startedActivityCount.Load() == 1 {
			s.WaitForChannel(ctx, activityPausedCn)
			return "", activityErr
		}
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// pause activity
	testIdentity := "test-identity"
	testReason := "test-reason"
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
		Identity: testIdentity,
		Reason:   testReason,
	}
	resp, err := s.FrontendClient().PauseActivity(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// make sure activity is paused on server while running on worker
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, description.PendingActivities[0].State)
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// unblock the activity
	activityPausedCn <- struct{}{}
	// make sure activity is paused on server and completed on the worker
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 5*time.Second, 500*time.Millisecond)

	description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Equal(1, len(description.PendingActivities))
	s.True(description.PendingActivities[0].Paused)

	// wait long enough for activity to retry if pause is not working
	// Note: because activity is retried we expect the attempts to be incremented
	err = util.InterruptibleSleep(ctx, 2*time.Second)
	s.NoError(err)

	// make sure activity is not completed, and was not retried
	description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Equal(1, len(description.PendingActivities))
	s.True(description.PendingActivities[0].Paused)
	s.Equal(int32(2), description.PendingActivities[0].Attempt)
	s.NotNil(description.PendingActivities[0].LastFailure)
	s.Equal(activityErr.Error(), description.PendingActivities[0].LastFailure.Message)
	s.NotNil(description.PendingActivities[0].PauseInfo)
	s.NotNil(description.PendingActivities[0].PauseInfo.GetManual())
	s.Equal(testIdentity, description.PendingActivities[0].PauseInfo.GetManual().Identity)
	s.Equal(testReason, description.PendingActivities[0].PauseInfo.GetManual().Reason)

	// unpause the activity
	unpauseRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Id{Id: "activity-id"},
	}
	unpauseResp, err := s.FrontendClient().UnpauseActivity(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}

func (s *ActivityApiPauseClientTestSuite) TestActivityPauseApi_IncreaseAttemptsOnFailure() {
	/*
	 * 1. Run an activity that runs forever
	 * 2. Pause the activity
	 * 3. Send a failure signal to the activity
	 * 4. Validate activity failed
	 * 5. Validate number of activity attempts increased
	 */
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var startedActivityCount atomic.Int32
	activityPausedCn := make(chan struct{})
	activityErr := errors.New("activity-failed-while-paused")
	var shouldSucceed atomic.Bool

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		if startedActivityCount.Load() == 1 {
			s.WaitForChannel(ctx, activityPausedCn)
			return "", activityErr
		}
		if shouldSucceed.Load() {
			return "done!", nil
		}
		return "", activityErr
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// pause activity
	testIdentity := "test-identity"
	testReason := "test-reason"
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
		Identity: testIdentity,
		Reason:   testReason,
	}
	resp, err := s.FrontendClient().PauseActivity(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// make sure activity is paused on server while running on worker
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, description.PendingActivities[0].State)
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// End the activity
	activityPausedCn <- struct{}{}

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description)
		require.Equal(t, 1, len(description.PendingActivities))
		require.True(t, description.PendingActivities[0].Paused)
		require.Equal(t, int32(2), description.PendingActivities[0].Attempt)
		require.NotNil(t, description.PendingActivities[0].LastFailure)
		require.NotNil(t, description.PendingActivities[0].PauseInfo)
		require.NotNil(t, description.PendingActivities[0].PauseInfo.GetManual())
		require.Equal(t, testIdentity, description.PendingActivities[0].PauseInfo.GetManual().Identity)
		require.Equal(t, testReason, description.PendingActivities[0].PauseInfo.GetManual().Reason)
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// Let the workflow finish gracefully
	// set the flag to make activity succeed on next attempt
	shouldSucceed.Store(true)

	// unpause the activity
	unpauseRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Id{Id: "activity-id"},
	}
	unpauseResp, err := s.FrontendClient().UnpauseActivity(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// wait for activity to complete
	s.EventuallyWithT(func(t *assert.CollectT) {
		require.Equal(t, int32(2), startedActivityCount.Load())
	}, 5*time.Second, 100*time.Millisecond)

	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}

func (s *ActivityApiPauseClientTestSuite) TestActivityPauseApi_WhileWaiting() {
	// In this case, pause happens when activity is in retry state.
	// Make sure that activity is paused and then unpaused.
	// Also check that activity will not be retried while unpaused.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.initialRetryInterval = 1 * time.Second
	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	var startedActivityCount atomic.Int32

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		if startedActivityCount.Load() == 1 {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, 1, len(description.PendingActivities))
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 5*time.Second, 100*time.Millisecond)

	// pause activity
	testIdentity := "test-identity"
	testReason := "test-reason"
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
		Identity: testIdentity,
		Reason:   testReason,
	}
	resp, err := s.FrontendClient().PauseActivity(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// wait long enough for activity to retry if pause is not working
	util.InterruptibleSleep(ctx, 2*time.Second)

	// make sure activity is not completed, and was not retried
	description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Equal(1, len(description.PendingActivities))
	s.True(description.PendingActivities[0].Paused)
	s.Equal(int32(2), description.PendingActivities[0].Attempt)
	s.NotNil(description.PendingActivities[0].PauseInfo)
	s.NotNil(description.PendingActivities[0].PauseInfo.GetManual())
	s.Equal(testIdentity, description.PendingActivities[0].PauseInfo.GetManual().Identity)
	s.Equal(testReason, description.PendingActivities[0].PauseInfo.GetManual().Reason)

	// unpause the activity
	unpauseRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Id{Id: "activity-id"},
	}
	unpauseResp, err := s.FrontendClient().UnpauseActivity(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// wait for activity to complete
	s.EventuallyWithT(func(t *assert.CollectT) {
		require.Equal(t, int32(2), startedActivityCount.Load())
	}, 5*time.Second, 100*time.Millisecond)

	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)

}

func (s *ActivityApiPauseClientTestSuite) TestActivityPauseApi_WhileRetryNoWait() {
	// In this case, pause can happen when activity is in retry state.
	// Make sure that activity is paused and then unpaused.
	// Also tests noWait flag.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.initialRetryInterval = 30 * time.Second
	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	var startedActivityCount atomic.Int32

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		if startedActivityCount.Load() == 1 {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 5*time.Second, 100*time.Millisecond)

	// pause activity
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	}
	resp, err := s.FrontendClient().PauseActivity(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// unpause the activity
	unpauseRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Id{Id: "activity-id"},
	}
	unpauseResp, err := s.FrontendClient().UnpauseActivity(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// wait for activity to complete. It should happen immediately since noWait is set
	s.EventuallyWithT(func(t *assert.CollectT) {
		require.Equal(t, int32(2), startedActivityCount.Load())
	}, 2*time.Second, 100*time.Millisecond)

	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}

func (s *ActivityApiPauseClientTestSuite) TestActivityPauseApi_WithReset() {
	// pause/unpause the activity with reset option and noWait flag
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.initialRetryInterval = 1 * time.Second
	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	var startedActivityCount atomic.Int32
	activityWasReset := false
	activityCompleteCn := make(chan struct{})

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)

		if !activityWasReset {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}
		s.WaitForChannel(ctx, activityCompleteCn)
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start/fail few times
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Greater(t, startedActivityCount.Load(), int32(1))
	}, 5*time.Second, 100*time.Millisecond)

	// pause activity
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	}
	resp, err := s.FrontendClient().PauseActivity(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// wait for activity to be in paused state and waiting for retry
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
		// also verify that the number of attempts was not reset
		require.True(t, description.PendingActivities[0].Attempt > 1)
	}, 5*time.Second, 100*time.Millisecond)

	activityWasReset = true

	// unpause the activity with reset, and set noWait flag
	unpauseRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity:      &workflowservice.UnpauseActivityRequest_Id{Id: "activity-id"},
		ResetAttempts: true,
	}
	unpauseResp, err := s.FrontendClient().UnpauseActivity(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// wait for activity to be running
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		// also verify that the number of attempts was reset
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 5*time.Second, 100*time.Millisecond)

	// let activity finish
	activityCompleteCn <- struct{}{}

	// wait for workflow to finish
	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}
