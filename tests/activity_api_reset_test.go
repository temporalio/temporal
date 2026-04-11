// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
)

type ActivityApiResetClientTestSuite struct {
	parallelsuite.Suite[*ActivityApiResetClientTestSuite]
}

func TestActivityApiResetClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityApiResetClientTestSuite{})
}

func (s *ActivityApiResetClientTestSuite) makeWorkflowFunc(activityFunction ActivityFunctions, retryPolicy *temporal.RetryPolicy) WorkflowFunction {
	return func(ctx workflow.Context) error {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    15 * time.Minute,
			ScheduleToCloseTimeout: 30 * time.Minute,
			RetryPolicy:            retryPolicy,
		}), activityFunction).Get(ctx, &ret)
		return err
	}
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_AfterRetry() {
	// activity reset is called after multiple attempts,
	env := testcore.NewEnv(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

		env.WaitForChannel(ctx, activityCompleteCh)
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction, &temporal.RetryPolicy{
		InitialInterval:    1 * time.Second,
		BackoffCoefficient: 1,
	})

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
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Greater(t, startedActivityCount.Load(), int32(1))
	}, 5*time.Second, 200*time.Millisecond)

	resetRequest := &workflowservice.ResetActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.ResetActivityRequest_Id{Id: "activity-id"},
	}
	resp, err := env.FrontendClient().ResetActivity(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resp)

	activityWasReset.Store(true)

	// wait for activity to be running
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		// also verify that the number of attempts was reset
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)

	}, 5*time.Second, 100*time.Millisecond)

	// let activity finish
	activityCompleteCh <- struct{}{}

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_WhileRunning() {
	// activity reset is called while activity is running
	env := testcore.NewEnv(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityCompleteCh := make(chan struct{})
	var startedActivityCount atomic.Int32
	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		env.WaitForChannel(ctx, activityCompleteCh)
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction, &temporal.RetryPolicy{
		InitialInterval:    1 * time.Second,
		BackoffCoefficient: 1,
	})

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
	}, 5*time.Second, 200*time.Millisecond)

	resetRequest := &workflowservice.ResetActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.ResetActivityRequest_Id{Id: "activity-id"},
	}
	resp, err := env.FrontendClient().ResetActivity(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resp)

	// wait a bit
	util.InterruptibleSleep(ctx, 1*time.Second)

	// check if workflow and activity are still running
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		// also verify that the number of attempts was reset
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 5*time.Second, 100*time.Millisecond)

	// let activity finish
	activityCompleteCh <- struct{}{}

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)

	// make sure that only a single instance of the activity was running
	s.Equal(int32(1), startedActivityCount.Load())
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_InRetry() {
	// reset is called while activity is in retry
	env := testcore.NewEnv(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var startedActivityCount atomic.Int32
	activityCompleteCh := make(chan struct{})

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)

		if startedActivityCount.Load() == 1 {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}

		env.WaitForChannel(ctx, activityCompleteCh)
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction, &temporal.RetryPolicy{
		InitialInterval:    1 * time.Minute,
		BackoffCoefficient: 1,
	})

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
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, description.PendingActivities[0].State)
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	resetRequest := &workflowservice.ResetActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.ResetActivityRequest_Id{Id: "activity-id"},
	}
	resp, err := env.FrontendClient().ResetActivity(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resp)

	// wait for activity to start. Wait time is shorter than original retry interval
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		require.Equal(t, int32(2), startedActivityCount.Load())
		// also verify that the number of attempts was reset
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 2*time.Second, 200*time.Millisecond)

	// let previous activity complete
	activityCompleteCh <- struct{}{}

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_KeepPaused() {
	// reset is called while activity is in retry
	env := testcore.NewEnv(s.T())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

		env.WaitForChannel(ctx, activityCompleteCh)
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction, &temporal.RetryPolicy{
		InitialInterval:    1 * time.Minute,
		BackoffCoefficient: 1,
	})

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
	s.EventuallyWithT(func(t *assert.CollectT) {
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
	s.EventuallyWithT(func(t *assert.CollectT) {
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
	resetRequest := &workflowservice.ResetActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity:   &workflowservice.ResetActivityRequest_Id{Id: "activity-id"},
		KeepPaused: true,
	}
	resp, err := env.FrontendClient().ResetActivity(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resp)

	// verify that activity is still paused, and reset
	s.EventuallyWithT(func(t *assert.CollectT) {
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
	activityCompleteCh <- struct{}{}

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

func (s *ActivityApiResetClientTestSuite) TestActivityReset_HeartbeatDetails() {
	// Latest reported heartbeat on activity should be available throughout workflow execution or until activity succeeds.
	// If activity was reset with "reset-heartbeat" flag, when returned heartbeat details should be nil.
	// 1. Start workflow with single activity
	// 2. First invocation of activity sets heartbeat details and fails upon request.
	// 3. Second invocation triggers waits to be triggered, and then send new heartbeat until requested to finish.
	// 6. Once workflow completes -- we're done.
	env := testcore.NewEnv(s.T())

	activityRetryPolicy := &temporal.RetryPolicy{
		InitialInterval:    1 * time.Second,
		BackoffCoefficient: 1,
	}

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
		env.WaitForChannel(ctx, activityCompleteCh)
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
			StartToCloseTimeout:    15 * time.Minute,
			ScheduleToCloseTimeout: 30 * time.Minute,
			RetryPolicy:            activityRetryPolicy,
		}), activityFn).Get(ctx, &ret)
		return ret, err
	}

	env.SdkWorker().RegisterActivity(activityFn)
	env.SdkWorker().RegisterWorkflow(workflowFn)

	wfId := "functional-test-heartbeat-details-after-reset"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 wfId,
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	s.NotNil(workflowRun)
	runId := workflowRun.GetRunID()
	s.NotEmpty(runId)

	// make sure activity is running and sending heartbeats
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		requirePayload(t, "first", description.PendingActivities[0].GetHeartbeatDetails())
		require.Equal(t, int32(0), activityIteration.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// reset the activity, with heartbeats
	resetRequest := &workflowservice.ResetActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity:       &workflowservice.ResetActivityRequest_Id{Id: activityId},
		ResetHeartbeat: true,
	}

	resp, err := env.FrontendClient().ResetActivity(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resp)

	activityIteration.Store(1)
	activityShouldBreak.Store(true)

	// wait for activity to fail and retried
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		ap := description.PendingActivities[0]

		require.Equal(t, int32(2), ap.Attempt)
		// make sure heartbeat was reset
		require.Nil(t, ap.HeartbeatDetails)
		require.Equal(t, int32(1), activityIteration.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// let activity start producing heartbeats
	activityCompleteCh <- struct{}{}

	// make sure activity is running and sending heartbeats
	s.EventuallyWithT(func(t *assert.CollectT) {
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
