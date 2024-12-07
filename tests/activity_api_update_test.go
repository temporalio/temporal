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
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type ActivityApiUpdateClientTestSuite struct {
	testcore.ClientFunctionalSuite
	tv                     *testvars.TestVars
	initialRetryInterval   time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration

	activityRetryPolicy *temporal.RetryPolicy
}

func (s *ActivityApiUpdateClientTestSuite) SetupSuite() {
	s.ClientFunctionalSuite.SetupSuite()
	s.OverrideDynamicConfig(dynamicconfig.ActivityAPIsEnabled, true)
	s.tv = testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(namespace.Name(s.Namespace()))
}

func (s *ActivityApiUpdateClientTestSuite) SetupTest() {
	s.ClientFunctionalSuite.SetupTest()

	s.initialRetryInterval = 10 * time.Minute
	s.scheduleToCloseTimeout = 30 * time.Minute
	s.startToCloseTimeout = 15 * time.Minute

	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}
}

func TestActivityApiUpdateClientTestSuite(t *testing.T) {
	s := new(ActivityApiUpdateClientTestSuite)
	suite.Run(t, s)
}

type (
	ActivityFunctions func() (string, error)
	WorkflowFunction  func(context2 workflow.Context) (string, error)
)

func (s *ActivityApiUpdateClientTestSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
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

func (s *ActivityApiUpdateClientTestSuite) TestActivityUpdateApi_ChangeRetryInterval() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityUpdated := make(chan struct{})

	var activityCompleted atomic.Int32
	activityFunction := func() (string, error) {
		if activityCompleted.Load() == 0 {
			activityErr := errors.New("bad-luck-please-retry")
			activityCompleted.Add(1)
			return "", activityErr
		}

		s.WaitForChannel(ctx, activityUpdated)
		activityCompleted.Add(1)
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

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, int32(1), activityCompleted.Load())
	}, 10*time.Second, 500*time.Millisecond)

	updateRequest := &workflowservicepb.UpdateActivityOptionsByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		ActivityOptions: &activitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(1 * time.Second),
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
	}
	resp, err := s.FrontendClient().UpdateActivityOptionsById(ctx, updateRequest)
	s.NoError(err)
	s.NotNil(resp)

	description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Equal(1, len(description.PendingActivities))

	activityUpdated <- struct{}{}

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 0, len(description.PendingActivities))
		assert.Equal(t, int32(2), activityCompleted.Load())
	}, 3*time.Second, 500*time.Millisecond)

	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}

func (s *ActivityApiUpdateClientTestSuite) TestActivityUpdateApi_ChangeScheduleToClose() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	wfId := "functional-test-activity-update-api-schedule-to-close"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfId,
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start (and fail)
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(description.PendingActivities))
		assert.Equal(t, int32(1), activityCompleted.Load())
	}, 2*time.Second, 200*time.Millisecond)

	// update schedule_to_close_timeout
	updateRequest := &workflowservicepb.UpdateActivityOptionsByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		ActivityOptions: &activitypb.ActivityOptions{
			ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout"}},
	}
	resp, err := s.FrontendClient().UpdateActivityOptionsById(ctx, updateRequest)
	s.NoError(err)
	s.NotNil(resp)

	// activity should fail immediately
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 0, len(description.PendingActivities))
		assert.Equal(t, int32(1), activityCompleted.Load())
	}, 2*time.Second, 200*time.Millisecond)

	var out string
	err = workflowRun.Get(ctx, &out)
	var activityError *temporal.ActivityError
	s.True(errors.As(err, &activityError))
	s.Equal(enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, activityError.RetryState())
	var timeoutError *temporal.TimeoutError
	s.True(errors.As(activityError.Unwrap(), &timeoutError))
	s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutError.TimeoutType())

	s.Equal(int32(1), activityCompleted.Load())
}

func (s *ActivityApiUpdateClientTestSuite) TestActivityUpdateApi_ChangeScheduleToCloseAndRetry() {
	// change both schedule to close and retry policy
	// initial values are chosen in such a way that activity will fail due to schedule to close timeout
	// we change schedule to close to a longer value and retry policy to a shorter value
	// after that activity should succeed
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	// make scheduleToClose shorter than retry interval
	s.scheduleToCloseTimeout = 8 * time.Second
	s.startToCloseTimeout = 8 * time.Second
	s.initialRetryInterval = 5 * time.Second

	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	wfId := "functional-test-activity-update-api-schedule-to-close"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        wfId,
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start (and fail)
	s.EventuallyWithT(func(t *assert.CollectT) {
		assert.True(t, activityCompleted.Load() > 0)
	}, 2*time.Second, 200*time.Millisecond)

	// update schedule_to_close_timeout, make it longer
	// also update retry policy interval, make it shorter
	updateRequest := &workflowservicepb.UpdateActivityOptionsByIdRequest{
		Namespace:  s.Namespace(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		ActivityOptions: &activitypb.ActivityOptions{
			ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(1 * time.Second),
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout", "retry_policy.initial_interval"}},
	}

	resp, err := s.FrontendClient().UpdateActivityOptionsById(ctx, updateRequest)
	s.NoError(err)
	s.NotNil(resp)
	// check that the update was successful
	s.Equal(int64(10), resp.GetActivityOptions().ScheduleToCloseTimeout.GetSeconds())
	// check that field we didn't update is the same
	s.Equal(int64(s.startToCloseTimeout.Seconds()), resp.GetActivityOptions().StartToCloseTimeout.GetSeconds())

	// now activity should succeed
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Equal(t, 0, len(description.PendingActivities))
		assert.Equal(t, int32(2), activityCompleted.Load())
	}, 5*time.Second, 200*time.Millisecond)

	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)
}
