// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
)

type ActivityApiPauseClientTestSuite struct {
	testcore.ClientFunctionalSuite
	tv                     *testvars.TestVars
	initialRetryInterval   time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration

	activityRetryPolicy *temporal.RetryPolicy
}

func (s *ActivityApiPauseClientTestSuite) SetupSuite() {
	s.ClientFunctionalSuite.SetupSuite()
	s.OverrideDynamicConfig(dynamicconfig.ActivityAPIsEnabled, true)
	s.tv = testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(namespace.Name(s.Namespace()))
}

func (s *ActivityApiPauseClientTestSuite) SetupTest() {
	s.ClientFunctionalSuite.SetupTest()

	s.initialRetryInterval = 1 * time.Second
	s.scheduleToCloseTimeout = 30 * time.Minute
	s.startToCloseTimeout = 15 * time.Minute

	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}
}

func TestActivityApiPauseClientTestSuite(t *testing.T) {
	s := new(ActivityApiPauseClientTestSuite)
	suite.Run(t, s)
}

func (s *ActivityApiPauseClientTestSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
	return func(ctx workflow.Context) (string, error) {

		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    s.startToCloseTimeout,
			ScheduleToCloseTimeout: s.scheduleToCloseTimeout,
			RetryPolicy:            s.activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)
		return "done!", err
	}
}

func (s *ActivityApiPauseClientTestSuite) TestActivityPauseApi_WhileRunning() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityPaused := make(chan struct{})
	var activityCompleted atomic.Int32

	activityFunction := func() (string, error) {
		if activityCompleted.Load() == 0 {
			activityErr := errors.New("bad-luck-please-retry")
			activityCompleted.Add(1)
			s.WaitForChannel(ctx, activityPaused)
			return "", activityErr
		}
		activityCompleted.Add(1)
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
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, int32(1), activityCompleted.Load())
	}, 10*time.Second, 500*time.Millisecond)

	// pause activity
	pauseRequest := &workflowservicepb.PauseActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
	}
	resp, err := s.FrontendClient().PauseActivityById(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// unblock the activity
	activityPaused <- struct{}{}

	description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Equal(1, len(description.PendingActivities))
	s.True(description.PendingActivities[0].Paused)

	// wait long enough for activity to retry if pause is not working
	util.InterruptibleSleep(ctx, 2*time.Second)

	// make sure activity is not completed, and was not retried
	description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Equal(1, len(description.PendingActivities))
	s.True(description.PendingActivities[0].Paused)
	s.Equal(int32(1), description.PendingActivities[0].Attempt)

	// unpause the activity
	unpauseRequest := &workflowservicepb.UnpauseActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		Operation: &workflowservicepb.UnpauseActivityByIdRequest_Resume{
			Resume: &workflowservicepb.UnpauseActivityByIdRequest_ResumeOperation{
				NoWait: false,
			},
		},
	}
	unpauseResp, err := s.FrontendClient().UnpauseActivityById(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	s.EventuallyWithT(func(t *assert.CollectT) {
		assert.Equal(t, int32(2), activityCompleted.Load())
	}, 5*time.Second, 100*time.Millisecond)

	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}

func (s *ActivityApiPauseClientTestSuite) TestActivityPauseApi_WhileWaiting() {
	// in this case pause happen when activity for in retry state
	// make sure that activity is paused and then unpaused
	// also check that activity will not be retried while unpaused
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.initialRetryInterval = 1 * time.Second
	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	var activityCompleted atomic.Int32

	activityFunction := func() (string, error) {
		if activityCompleted.Load() == 0 {
			activityErr := errors.New("bad-luck-please-retry")
			activityCompleted.Add(1)
			return "", activityErr
		}
		activityCompleted.Add(1)
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
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, int32(1), activityCompleted.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// pause activity
	pauseRequest := &workflowservicepb.PauseActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
	}
	resp, err := s.FrontendClient().PauseActivityById(ctx, pauseRequest)
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

	// unpause the activity
	unpauseRequest := &workflowservicepb.UnpauseActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		Operation: &workflowservicepb.UnpauseActivityByIdRequest_Resume{
			Resume: &workflowservicepb.UnpauseActivityByIdRequest_ResumeOperation{
				NoWait: false,
			},
		},
	}
	unpauseResp, err := s.FrontendClient().UnpauseActivityById(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// wait for activity to complete
	s.EventuallyWithT(func(t *assert.CollectT) {
		assert.Equal(t, int32(2), activityCompleted.Load())
	}, 5*time.Second, 100*time.Millisecond)

	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)

}

func (s *ActivityApiPauseClientTestSuite) TestActivityPauseApi_WhileRetryNoWait() {
	// in this case pause happen when activity for in retry state
	// make sure that activity is paused and then unpaused
	// also test noWait flag
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.initialRetryInterval = 30 * time.Second
	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	var activityCompleted atomic.Int32

	activityFunction := func() (string, error) {
		if activityCompleted.Load() == 0 {
			activityErr := errors.New("bad-luck-please-retry")
			activityCompleted.Add(1)
			return "", activityErr
		}
		activityCompleted.Add(1)
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
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, int32(1), activityCompleted.Load())
	}, 5*time.Second, 100*time.Millisecond)

	// pause activity
	pauseRequest := &workflowservicepb.PauseActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
	}
	resp, err := s.FrontendClient().PauseActivityById(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// unpause the activity, and set noWait flag
	unpauseRequest := &workflowservicepb.UnpauseActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		Operation: &workflowservicepb.UnpauseActivityByIdRequest_Resume{
			Resume: &workflowservicepb.UnpauseActivityByIdRequest_ResumeOperation{
				NoWait: true,
			},
		},
	}
	unpauseResp, err := s.FrontendClient().UnpauseActivityById(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// wait for activity to complete. It should happen immediately since noWait is set
	s.EventuallyWithT(func(t *assert.CollectT) {
		assert.Equal(t, int32(2), activityCompleted.Load())
	}, 5*time.Second, 100*time.Millisecond)

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

	var activityCompleted atomic.Int32
	activityReseted := false
	activityCompleteSignal := make(chan struct{})

	activityFunction := func() (string, error) {
		if !activityReseted {
			activityErr := errors.New("bad-luck-please-retry")
			activityCompleted.Add(1)
			return "", activityErr
		}
		activityCompleted.Add(1)
		s.WaitForChannel(ctx, activityCompleteSignal)
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
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, int32(2), activityCompleted.Load())
	}, 5*time.Second, 100*time.Millisecond)

	// pause activity
	pauseRequest := &workflowservicepb.PauseActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
	}
	resp, err := s.FrontendClient().PauseActivityById(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// wait for activity to be in paused state and waiting for retry
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, description.PendingActivities[0].State)
		// also verify that the number of attempts was not reset
		assert.True(t, description.PendingActivities[0].Attempt > 1)
	}, 5*time.Second, 100*time.Millisecond)

	activityReseted = true

	// unpause the activity with reset, and set noWait flag
	unpauseRequest := &workflowservicepb.UnpauseActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		Operation: &workflowservicepb.UnpauseActivityByIdRequest_Reset_{
			Reset_: &workflowservicepb.UnpauseActivityByIdRequest_ResetOperation{
				NoWait: true,
			},
		},
	}
	unpauseResp, err := s.FrontendClient().UnpauseActivityById(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// wait for activity to be running
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		// also verify that the number of attempts was reset
		assert.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 5*time.Second, 100*time.Millisecond)

	// let activity finish
	activityCompleteSignal <- struct{}{}

	// wait for workflow to finish
	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}
