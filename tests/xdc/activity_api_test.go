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

package xdc

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
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type (
	ActivityFunctions func() (string, error)
	WorkflowFunction  func(context2 workflow.Context) error

	ActivityApiStateReplicationSuite struct {
		xdcBaseSuite
	}
)

func TestActivityApiStateReplicationSuite(t *testing.T) {
	s := new(ActivityApiStateReplicationSuite)
	suite.Run(t, s)
}

func (s *ActivityApiStateReplicationSuite) SetupSuite() {
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]interface{})
	}
	s.dynamicConfigOverrides[dynamicconfig.ActivityAPIsEnabled.Key()] = true

	s.setupSuite([]string{"active", "standby"})
}

func (s *ActivityApiStateReplicationSuite) SetupTest() {
	s.setupTest()
}

func (s *ActivityApiStateReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *ActivityApiStateReplicationSuite) TestPauseActivityFailover() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityPausedCn := make(chan struct{})
	var activityWasPaused atomic.Bool
	var startedActivityCount atomic.Int32

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		if activityWasPaused.Load() == false {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}
		s.waitForChannel(ctx, activityPausedCn)
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	ns := s.createGlobalNamespace()
	activeSDKClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster1.Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)

	taskQueue := testcore.RandomizeStr("tq")
	worker1 := sdkworker.New(activeSDKClient, taskQueue, sdkworker.Options{})

	worker1.RegisterWorkflow(workflowFn)
	worker1.RegisterActivity(activityFunction)

	s.NoError(worker1.Start())
	defer worker1.Stop()

	// start a workflow
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wfid-" + s.T().Name()),
		TaskQueue: taskQueue,
	}

	workflowRun, err := activeSDKClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start/fail few times
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		assert.Len(t, description.GetPendingActivities(), 1)
		assert.Greater(t, startedActivityCount.Load(), int32(2))
	}, 5*time.Second, 200*time.Millisecond)

	// pause the activity in cluster 1
	pauseRequest := &workflowservice.PauseActivityByIdRequest{
		Namespace:  ns,
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
	}
	pauseResp, err := s.cluster1.Host().FrontendClient().PauseActivityById(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// reset the activity in cluster 1
	resetRequest := &workflowservice.ResetActivityByIdRequest{
		Namespace:  ns,
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		NoWait:     false,
	}
	resetResp, err := s.cluster1.Host().FrontendClient().ResetActivityById(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resetResp)

	// update the activity properties in cluster 1
	updateRequest := &workflowservice.UpdateActivityOptionsByIdRequest{
		Namespace:  ns,
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		ActivityOptions: &activitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(2 * time.Second),
				MaximumAttempts: 10,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval", "retry_policy.maximum_attempts"}},
	}
	respUpdate, err := s.cluster1.Host().FrontendClient().UpdateActivityOptionsById(ctx, updateRequest)
	s.NoError(err)
	s.NotNil(respUpdate)

	// verify activity is paused is cluster 1
	description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Equal(1, len(description.PendingActivities))
	s.True(description.PendingActivities[0].Paused)
	s.Equal(int32(1), description.PendingActivities[0].Attempt)
	// currently we are not replicating retry interval
	// s.Equal(int64(2), description.PendingActivities[0].CurrentRetryInterval.GetSeconds())

	// stop worker1 so cluster 1 won't make any progress on the activity (just in case)
	worker1.Stop()

	// failover to standby cluster
	s.failover(ns, s.clusterNames[1], int64(2), s.cluster1.FrontendClient())

	// get standby client
	standbyClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.cluster2.Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)
	s.NotNil(standbyClient)

	// verify activity is still paused in cluster 2
	description, err = standbyClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Equal(1, len(description.PendingActivities))
	s.True(description.PendingActivities[0].Paused)
	s.Equal(int32(1), description.PendingActivities[0].Attempt)
	s.Equal(int64(2), description.PendingActivities[0].CurrentRetryInterval.GetSeconds())
	s.Equal(int32(10), description.PendingActivities[0].MaximumAttempts)

	// start worker2
	worker2 := sdkworker.New(standbyClient, taskQueue, sdkworker.Options{})
	worker2.RegisterWorkflow(workflowFn)
	worker2.RegisterActivity(activityFunction)
	s.NoError(worker2.Start())
	defer worker2.Stop()

	// let the activity make progress once unpaused
	activityWasPaused.Store(true)

	// unpause the activity in cluster 2
	unpauseRequest := &workflowservice.UnpauseActivityByIdRequest{
		Namespace:  ns,
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		Operation: &workflowservice.UnpauseActivityByIdRequest_Resume{
			Resume: &workflowservice.UnpauseActivityByIdRequest_ResumeOperation{
				NoWait: true,
			},
		},
	}
	unpauseResp, err := s.cluster2.Host().FrontendClient().UnpauseActivityById(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// unblock the activity
	activityPausedCn <- struct{}{}

	// wait for activity to finish
	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}

func (s *ActivityApiStateReplicationSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
	initialRetryInterval := 1 * time.Second
	scheduleToCloseTimeout := 30 * time.Minute
	startToCloseTimeout := 15 * time.Minute

	activityRetryPolicy := &temporal.RetryPolicy{
		InitialInterval:    initialRetryInterval,
		BackoffCoefficient: 1,
	}

	return func(ctx workflow.Context) error {

		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    startToCloseTimeout,
			ScheduleToCloseTimeout: scheduleToCloseTimeout,
			RetryPolicy:            activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)
		return err
	}
}

func (s *ActivityApiStateReplicationSuite) waitForChannel(ctx context.Context, ch chan struct{}) {
	s.T().Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		s.FailNow("context timeout while waiting for channel")
	}
}
