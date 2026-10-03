package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
)

type ActivityApiResetClientTestSuite struct {
	parallelsuite.Suite[*ActivityApiResetClientTestSuite]
}

type activityResetTestEnv struct {
	*testcore.TestEnv
	tv                     *testvars.TestVars
	initialRetryInterval   time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration
	activityRetryPolicy    *temporal.RetryPolicy

	// apiName selects which reset API variant to exercise ("legacy-api" or "execution-api").
	// Passed through parallelsuite.Run; used by newActivityResetTestEnv to initialise resetFn.
	apiName string
	// resetFn is the adapter for the API under test, initialised in newActivityResetTestEnv.
	resetFn func(ctx context.Context, wfID, actID string, resetHeartbeat, keepPaused bool) error
}

// TestActivityApiResetClientTestSuite runs the suite twice: once with the legacy
// ResetActivity API and once with the newer ResetActivityExecution API.
func TestActivityApiResetClientTestSuite(t *testing.T) {
	for _, apiName := range []string{"legacy-api", "execution-api"} {
		t.Run(apiName, func(t *testing.T) {
			parallelsuite.Run(t, &ActivityApiResetClientTestSuite{}, apiName)
		})
	}
}

func newActivityResetTestEnv(t *testing.T, apiName string) *activityResetTestEnv {
	t.Helper()

	env := &activityResetTestEnv{
		TestEnv: testcore.NewEnv(t),
		apiName: apiName,
	}

	env.tv = testvars.New(t).WithTaskQueue(env.WorkerTaskQueue()).WithNamespaceName(env.Namespace())

	env.initialRetryInterval = 1 * time.Second
	env.scheduleToCloseTimeout = 30 * time.Minute
	env.startToCloseTimeout = 15 * time.Minute

	env.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    env.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	if env.apiName == "execution-api" {
		env.resetFn = func(ctx context.Context, wfID, actID string, resetHeartbeat, keepPaused bool) error {
			_, err := env.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
				Namespace:      env.Namespace().String(),
				WorkflowId:     wfID,
				ActivityId:     actID,
				ResetHeartbeat: resetHeartbeat,
				KeepPaused:     keepPaused,
			})
			return err
		}
	} else {
		env.resetFn = func(ctx context.Context, wfID, actID string, resetHeartbeat, keepPaused bool) error {
			_, err := env.FrontendClient().ResetActivity(ctx, &workflowservice.ResetActivityRequest{
				Namespace:      env.Namespace().String(),
				Execution:      &commonpb.WorkflowExecution{WorkflowId: wfID},
				Activity:       &workflowservice.ResetActivityRequest_Id{Id: actID},
				ResetHeartbeat: resetHeartbeat,
				KeepPaused:     keepPaused,
			})
			return err
		}
	}
	return env
}

func (env *activityResetTestEnv) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
	return func(ctx workflow.Context) error {

		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    env.startToCloseTimeout,
			ScheduleToCloseTimeout: env.scheduleToCloseTimeout,
			RetryPolicy:            env.activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)
		return err
	}
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_AfterWorkflowCompleted(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	workflowFn := func(workflow.Context) error { return nil }
	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.Require().NoError(err)
	s.Require().NoError(workflowRun.Get(ctx, nil))

	err = env.resetFn(ctx, workflowRun.GetID(), "activity-id", false, false)
	s.Require().ErrorContains(err, "workflow execution already completed")
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_AfterRetry(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	// activity reset is called after multiple attempts,
	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	var activityWasReset atomic.Bool
	activityCompleteCh := make(chan struct{})
	var startedActivityCount atomic.Int32

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)

		if activityWasReset.Load() == false {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}

		s.Rcv(activityCompleteCh)
		return "done!", nil
	}

	workflowFn := env.makeWorkflowFunc(activityFunction)

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	wfId := testcore.RandomizeStr("wfid-" + s.T().Name())
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfId,
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start/fail few times
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Greater(t, startedActivityCount.Load(), int32(1))
	}, 5*time.Second, 200*time.Millisecond)

	s.NoError(env.resetFn(ctx, workflowRun.GetID(), "activity-id", false, false))

	activityWasReset.Store(true)

	// wait for activity to be running
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		// also verify that the number of attempts was reset
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)

	}, 5*time.Second, 100*time.Millisecond)

	// let activity finish
	s.Snd(activityCompleteCh, struct{}{})

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_WhileRunning(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	// activity reset is called while activity is running
	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	activityCompleteCh := make(chan struct{})
	var startedActivityCount atomic.Int32
	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		s.Rcv(activityCompleteCh)
		return "done!", nil
	}

	workflowFn := env.makeWorkflowFunc(activityFunction)

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        env.tv.WorkflowID(),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
	}, 5*time.Second, 200*time.Millisecond)

	s.NoError(env.resetFn(ctx, workflowRun.GetID(), "activity-id", false, false))

	// wait a bit
	util.InterruptibleSleep(ctx, 1*time.Second)

	// check if workflow and activity are still running
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		// also verify that the number of attempts was reset
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 5*time.Second, 100*time.Millisecond)

	// let activity finish
	s.Snd(activityCompleteCh, struct{}{})

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)

	// make sure that only a single instance of the activity was running
	s.Equal(int32(1), startedActivityCount.Load())
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_UnpausesRunningActivity(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	activityCompleteCh := make(chan struct{})
	var startedActivityCount atomic.Int32
	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		s.Rcv(activityCompleteCh)
		return "done!", nil
	}

	workflowFn := env.makeWorkflowFunc(activityFunction)
	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        env.tv.WorkflowID(),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.Require().NoError(err)

	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
	}, 5*time.Second, 200*time.Millisecond)

	_, err = env.FrontendClient().PauseActivity(ctx, &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowRun.GetID()},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	})
	s.Require().NoError(err)

	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, description.PendingActivities[0].State)
		require.True(t, description.PendingActivities[0].Paused)
		require.NotNil(t, description.PendingActivities[0].PauseInfo)
	}, 5*time.Second, 200*time.Millisecond)

	s.Require().NoError(env.resetFn(ctx, workflowRun.GetID(), "activity-id", false, false))

	// keepPaused=false must clear both the paused flag and its associated metadata.
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		require.False(t, description.PendingActivities[0].Paused)
		require.Nil(t, description.PendingActivities[0].PauseInfo)
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 5*time.Second, 200*time.Millisecond)

	s.Snd(activityCompleteCh, struct{}{})

	s.Require().NoError(workflowRun.Get(ctx, nil))
	s.Equal(int32(1), startedActivityCount.Load())
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_TimesOutOnUnpause(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	env.startToCloseTimeout = time.Second
	env.activityRetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 1}

	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	activityCompleteCh := make(chan struct{})
	defer close(activityCompleteCh)
	activityFunction := func() (string, error) {
		s.Rcv(activityCompleteCh)
		return "done!", nil
	}

	workflowFn := env.makeWorkflowFunc(activityFunction)
	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        env.tv.WorkflowID(),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.Require().NoError(err)

	var activityStartedAt time.Time
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		pendingActivity := description.PendingActivities[0]
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, pendingActivity.State)
		require.NotNil(t, pendingActivity.LastStartedTime)
		activityStartedAt = pendingActivity.LastStartedTime.AsTime()
	}, 5*time.Second, 200*time.Millisecond)

	_, err = env.FrontendClient().PauseActivity(ctx, &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowRun.GetID()},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	})
	s.Require().NoError(err)

	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, description.PendingActivities[0].State)
		require.True(t, description.PendingActivities[0].Paused)
	}, 5*time.Second, 200*time.Millisecond)

	// Allow the original timeout task to be processed while the activity is paused.
	originalDeadline := activityStartedAt.Add(env.startToCloseTimeout)
	await.RequireTrue(s.T(), func() bool {
		return time.Now().After(originalDeadline.Add(2 * time.Second))
	}, 5*time.Second, 100*time.Millisecond)

	description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.Require().NoError(err)
	s.Require().Len(description.GetPendingActivities(), 1)
	s.True(description.PendingActivities[0].Paused)

	s.Require().NoError(env.resetFn(ctx, workflowRun.GetID(), "activity-id", false, false))

	workflowResultCtx, workflowResultCancel := context.WithTimeout(ctx, 10*time.Second)
	defer workflowResultCancel()
	err = workflowRun.Get(workflowResultCtx, nil)
	s.Require().Error(err)
	var activityErr *temporal.ActivityError
	s.Require().ErrorAs(err, &activityErr)
	timeoutErr, ok := activityErr.Unwrap().(*temporal.TimeoutError)
	s.Require().True(ok)
	s.Equal(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, timeoutErr.TimeoutType())
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_InRetry(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	// reset is called while activity is in retry
	env.initialRetryInterval = 1 * time.Minute
	env.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    env.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	var startedActivityCount atomic.Int32
	activityCompleteCh := make(chan struct{})

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)

		if startedActivityCount.Load() == 1 {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}

		s.Rcv(activityCompleteCh)
		return "done!", nil
	}

	workflowFn := env.makeWorkflowFunc(activityFunction)

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	wfId := testcore.RandomizeStr("wf_id-" + s.T().Name())
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfId,
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start, fail and wait for retry
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, description.PendingActivities[0].State)
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	s.NoError(env.resetFn(ctx, workflowRun.GetID(), "activity-id", false, false))

	// wait for activity to start. Wait time is shorter than original retry interval
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		require.Equal(t, int32(2), startedActivityCount.Load())
		// also verify that the number of attempts was reset
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 2*time.Second, 200*time.Millisecond)

	// let previous activity complete
	s.Snd(activityCompleteCh, struct{}{})

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_KeepPaused(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	// reset is called while activity is in retry
	env.initialRetryInterval = 1 * time.Minute
	env.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    env.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	var startedActivityCount atomic.Int32
	var activityWasReset atomic.Bool
	activityCompleteCh := make(chan struct{})

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)

		if !activityWasReset.Load() {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}

		s.Rcv(activityCompleteCh)
		return "done!", nil
	}

	workflowFn := env.makeWorkflowFunc(activityFunction)

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	wfId := testcore.RandomizeStr("wf_id-" + s.T().Name())
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfId,
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start, fail few times and wait for retry
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, description.PendingActivities[0].State)
		require.Greater(t, description.PendingActivities[0].Attempt, int32(1))
	}, 5*time.Second, 200*time.Millisecond)

	// pause the activity
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	}
	pauseResp, err := env.FrontendClient().PauseActivity(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// verify that activity is paused
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
		// also verify that the number of attempts was not reset
		require.Greater(t, description.PendingActivities[0].Attempt, int32(1))
		require.True(t, description.PendingActivities[0].Paused)
	}, 5*time.Second, 100*time.Millisecond)

	// reset the activity, while keeping it paused
	s.NoError(env.resetFn(ctx, workflowRun.GetID(), "activity-id", false, true))

	// verify that activity is still paused, and reset
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
		// also verify that the number of attempts was reset
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 2*time.Second, 200*time.Millisecond)

	// let activity stop failing
	activityWasReset.Store(true)

	// unpause the activity
	unpauseRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Id{Id: "activity-id"},
	}
	unpauseResp, err := env.FrontendClient().UnpauseActivity(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// let  activity complete
	s.Snd(activityCompleteCh, struct{}{})

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)
}

func requirePayload(t require.TestingT, expected string, pls *commonpb.Payloads) {
	require.NotNil(t, pls)
	require.NotNil(t, pls.Payloads)
	require.Len(t, pls.Payloads, 1)
	var actual string
	err := payloads.Decode(pls, &actual)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

// TestActivityReset_HeartbeatDetails covers the default: a reset rewinds the attempt count but
// keeps the heartbeat checkpoint.
func (s *ActivityApiResetClientTestSuite) TestActivityReset_HeartbeatDetails(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	s.runResetHeartbeatDetails(env, false, true)
}

// TestActivityReset_HeartbeatDetailsWithResetHeartbeatFlag covers the opt-in discard.
func (s *ActivityApiResetClientTestSuite) TestActivityReset_HeartbeatDetailsWithResetHeartbeatFlag(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	s.runResetHeartbeatDetails(env, true, false)
}

// runResetHeartbeatDetails runs an activity that heartbeats "first", resets it mid-attempt, then
// lets that attempt fail so the reset lands on the retry, and checks whether the checkpoint
// survived. The retried attempt heartbeats "second" to show heartbeating still works afterwards.
func (s *ActivityApiResetClientTestSuite) runResetHeartbeatDetails(env *activityResetTestEnv, resetHeartbeat, expectPreserved bool) {
	activityCompleteCh := make(chan struct{})
	var activityIteration atomic.Int32
	var activityShouldBreak atomic.Bool
	var activityShouldFinish atomic.Bool

	activityFn := func(ctx context.Context) (string, error) {
		if activityIteration.Load() == 0 {
			for activityShouldBreak.Load() == false {
				activity.RecordHeartbeat(ctx, "first")
				time.Sleep(time.Second) //nolint:forbidigo
			}
			return "", errors.New("bad-luck-please-retry")
		}
		// not the first iteration
		s.Rcv(activityCompleteCh)
		for activityShouldFinish.Load() == false {
			activity.RecordHeartbeat(ctx, "second")
			time.Sleep(time.Second) //nolint:forbidigo
		}
		return "Done", nil
	}

	activityId := "heartbeat_retry"
	workflowFn := func(ctx workflow.Context) (string, error) {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             activityId,
			DisableEagerExecution:  true,
			StartToCloseTimeout:    env.startToCloseTimeout,
			ScheduleToCloseTimeout: env.scheduleToCloseTimeout,
			RetryPolicy:            env.activityRetryPolicy,
		}), activityFn).Get(ctx, &ret)
		return ret, err
	}

	env.SdkWorker().RegisterActivity(activityFn)
	env.SdkWorker().RegisterWorkflow(workflowFn)

	wfID := testcore.RandomizeStr("wfid-" + s.T().Name())
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 wfID,
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()
	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	runId := workflowRun.GetRunID()
	s.NotEmpty(runId)

	// make sure activity is running and sending heartbeats
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		requirePayload(t, "first", description.PendingActivities[0].GetHeartbeatDetails())
		require.Equal(t, int32(0), activityIteration.Load())
	}, 5*time.Second, 500*time.Millisecond)

	s.NoError(env.resetFn(ctx, workflowRun.GetID(), activityId, resetHeartbeat, false))

	activityIteration.Store(1)
	activityShouldBreak.Store(true)

	// wait for activity to fail and retried
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		ap := description.PendingActivities[0]

		require.Equal(t, int32(2), ap.Attempt)
		if expectPreserved {
			requirePayload(t, "first", ap.GetHeartbeatDetails())
		} else {
			require.Nil(t, ap.HeartbeatDetails)
		}
		require.Equal(t, int32(1), activityIteration.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// let activity start producing heartbeats
	s.Snd(activityCompleteCh, struct{}{})

	// make sure activity is running and sending heartbeats
	await.Require(ctx, s.T(), func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, int32(1), activityIteration.Load())
		require.Len(t, description.PendingActivities, 1)
		requirePayload(t, "second", description.PendingActivities[0].GetHeartbeatDetails())
	}, 5*time.Second, 500*time.Millisecond)

	// let activity finish
	activityShouldFinish.Store(true)

	// wait for workflow to finish
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)
	s.NotEmpty(out)
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_WhilePaused(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	// Reset is called while the activity is in PAUSED state (SCHEDULED→PAUSED via TransitionPaused).
	// The activity should remain PAUSED with attempt count reset to 1. After unpause it should complete.
	env.initialRetryInterval = 1 * time.Minute
	env.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    env.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	var startedActivityCount atomic.Int32
	var activityWasReset atomic.Bool
	activityCompleteCh := make(chan struct{})

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		if !activityWasReset.Load() {
			return "", errors.New("bad-luck-please-retry")
		}
		s.Rcv(activityCompleteCh)
		return "done!", nil
	}

	workflowFn := env.makeWorkflowFunc(activityFunction)
	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	wfID := testcore.RandomizeStr("wf_id-" + s.T().Name())
	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)

	// wait for activity to fail and enter retry backoff (SCHEDULED state waiting for retry)
	await.Require(ctx, s.T(), func(t *await.T) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.PendingActivities[0].State)
		require.Greater(t, desc.PendingActivities[0].Attempt, int32(1))
	}, 5*time.Second, 200*time.Millisecond)

	// pause the activity (transitions SCHEDULED→PAUSED)
	_, err = env.FrontendClient().PauseActivity(ctx, &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	})
	s.NoError(err)

	// wait for PAUSED state
	await.Require(ctx, s.T(), func(t *await.T) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, desc.PendingActivities[0].State)
		require.Greater(t, desc.PendingActivities[0].Attempt, int32(1))
	}, 5*time.Second, 100*time.Millisecond)

	// reset while paused — activity should stay PAUSED, but attempt resets to 1
	s.NoError(env.resetFn(ctx, wfID, "activity-id", false, true))

	await.Require(ctx, s.T(), func(t *await.T) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, desc.PendingActivities[0].State)
		require.Equal(t, int32(1), desc.PendingActivities[0].Attempt)
	}, 5*time.Second, 100*time.Millisecond)

	activityWasReset.Store(true)

	// unpause — activity should run and complete
	_, err = env.FrontendClient().UnpauseActivity(ctx, &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: wfID},
		Activity:  &workflowservice.UnpauseActivityRequest_Id{Id: "activity-id"},
	})
	s.NoError(err)

	s.Snd(activityCompleteCh, struct{}{})

	s.NoError(workflowRun.Get(ctx, nil))
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_TerminateWhileDeferredReset(apiName string) {
	env := newActivityResetTestEnv(s.T(), apiName)

	// Reset is called while activity is STARTED (sets ActivityReset=true as a deferred flag).
	// The workflow is then terminated before the activity retries. Verifies the activity
	// and workflow terminate cleanly without the deferred reset flag causing issues.
	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	activityBlockCh := make(chan struct{})
	var startedActivityCount atomic.Int32

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		s.Rcv(activityBlockCh)
		return "done!", nil
	}

	workflowFn := env.makeWorkflowFunc(activityFunction)
	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	wfID := testcore.RandomizeStr("wf_id-" + s.T().Name())
	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        wfID,
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)

	// wait for activity to start
	await.Require(ctx, s.T(), func(t *await.T) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, desc.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, desc.PendingActivities[0].State)
	}, 5*time.Second, 200*time.Millisecond)

	// reset while running — sets ActivityReset=true as deferred flag
	s.NoError(env.resetFn(ctx, wfID, "activity-id", false, false))

	// terminate the workflow before the activity retries
	err = env.SdkClient().TerminateWorkflow(ctx, wfID, workflowRun.GetRunID(), "test termination")
	s.NoError(err)

	// unblock the activity worker so it can respond
	close(activityBlockCh)

	// verify the workflow is terminated
	await.Require(ctx, s.T(), func(t *await.T) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, desc.GetWorkflowExecutionInfo().GetStatus())
	}, 10*time.Second, 200*time.Millisecond)
}
