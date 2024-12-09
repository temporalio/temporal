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

type ActivityApiResetClientTestSuite struct {
	testcore.ClientFunctionalSuite
	tv                     *testvars.TestVars
	initialRetryInterval   time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration

	activityRetryPolicy *temporal.RetryPolicy
}

func (s *ActivityApiResetClientTestSuite) SetupSuite() {
	s.ClientFunctionalSuite.SetupSuite()
	s.OverrideDynamicConfig(dynamicconfig.ActivityAPIsEnabled, true)
	s.tv = testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(namespace.Name(s.Namespace()))
}

func (s *ActivityApiResetClientTestSuite) SetupTest() {
	s.ClientFunctionalSuite.SetupTest()

	s.initialRetryInterval = 1 * time.Second
	s.scheduleToCloseTimeout = 30 * time.Minute
	s.startToCloseTimeout = 15 * time.Minute

	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}
}

func TestActivityApiResetClientTestSuite(t *testing.T) {
	s := new(ActivityApiResetClientTestSuite)
	suite.Run(t, s)
}

func (s *ActivityApiResetClientTestSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
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

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_Acceptance() {
	// activity reset is called after multiple attempts,
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var activityReseted atomic.Bool
	activityCompleteSignal := make(chan struct{})
	var activityCompleted atomic.Int32

	activityFunction := func() (string, error) {
		if activityReseted.Load() == false {
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

	wfId := testcore.RandomizeStr("wfid-" + s.T().Name())
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfId,
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start/fail few times
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.True(t, activityCompleted.Load() > 1)
	}, 5*time.Second, 200*time.Millisecond)

	resetRequest := &workflowservicepb.ResetActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		NoWait:     true,
	}
	resp, err := s.FrontendClient().ResetActivityById(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resp)

	activityReseted.Store(true)

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

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_WithNoWait() {
	// activity reset is called while activity is running, with NoWait=true to start new activity immediately
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityCompleteSignal1 := make(chan struct{})
	activityCompleteSignal2 := make(chan struct{})
	var activityReseted atomic.Bool

	activityFunction := func() (string, error) {
		if activityReseted.Load() == false {
			activityErr := errors.New("bad-luck-please-retry")
			s.WaitForChannel(ctx, activityCompleteSignal1)
			return "", activityErr
		}

		s.WaitForChannel(ctx, activityCompleteSignal2)
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	wfId := testcore.RandomizeStr("wfid-" + s.T().Name())
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfId,
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
	}, 5*time.Second, 200*time.Millisecond)

	activityReseted.Store(true)
	resetRequest := &workflowservicepb.ResetActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		NoWait:     true,
	}
	resp, err := s.FrontendClient().ResetActivityById(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resp)

	// let previous activity complete
	activityCompleteSignal1 <- struct{}{}
	// wait a bit to make sure previous activity is completed
	util.InterruptibleSleep(ctx, 1*time.Second)

	// check if workflow and activity are still running
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		// also verify that the number of attempts was reset
		assert.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 5*time.Second, 100*time.Millisecond)

	// let activity finish
	activityCompleteSignal2 <- struct{}{}

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}

func (s *ActivityApiResetClientTestSuite) TestActivityResetApi_InRetry() {
	// reset is called while activity is in retry
	s.initialRetryInterval = 1 * time.Minute
	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var activityCompleted atomic.Int32
	activityCompleteSignal := make(chan struct{})

	activityFunction := func() (string, error) {
		if activityCompleted.Load() == 0 {
			activityCompleted.Add(1)
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}

		activityCompleted.Add(1)
		s.WaitForChannel(ctx, activityCompleteSignal)
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	wfId := testcore.RandomizeStr("wf_id-" + s.T().Name())
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfId,
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start, fail and wait for retry
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, description.PendingActivities[0].State)
		assert.Equal(t, int32(1), activityCompleted.Load())
	}, 5*time.Second, 200*time.Millisecond)

	resetRequest := &workflowservicepb.ResetActivityByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		NoWait:     true,
	}
	resp, err := s.FrontendClient().ResetActivityById(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resp)

	// wait for activity to start. Wait time is shorter than original retry interval
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
		assert.Equal(t, int32(2), activityCompleted.Load())
		// also verify that the number of attempts was reset
		assert.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 2*time.Second, 200*time.Millisecond)

	// let previous activity complete
	activityCompleteSignal <- struct{}{}

	// wait for workflow to complete
	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}
