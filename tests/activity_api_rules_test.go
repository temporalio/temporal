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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	rulespb "go.temporal.io/api/rules/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/tests/testcore"
)

type ActivityApiRulesClientTestSuite struct {
	testcore.FunctionalTestSdkSuite

	initialRetryInterval   time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration

	activityRetryPolicy *temporal.RetryPolicy
}

func TestActivityApiRulesClientTestSuite(t *testing.T) {
	s := new(ActivityApiRulesClientTestSuite)
	suite.Run(t, s)
}

type internalRulesTestWorkflow struct {
	initialRetryInterval   time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration

	activityRetryPolicy *temporal.RetryPolicy

	startedActivityCount atomic.Int32
	letActivitySucceed   atomic.Bool
	activityCompleteCn   chan struct{}

	testSuite *testcore.FunctionalTestBase
	ctx       context.Context
}

func newInternalRulesTestWorkflow(ctx context.Context, testSuite *testcore.FunctionalTestBase) *internalRulesTestWorkflow {
	wf := &internalRulesTestWorkflow{
		initialRetryInterval:   1 * time.Second,
		scheduleToCloseTimeout: 30 * time.Minute,
		startToCloseTimeout:    15 * time.Minute,
		activityCompleteCn:     make(chan struct{}),
		testSuite:              testSuite,
		ctx:                    ctx,
	}
	wf.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    wf.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	return wf
}

func (w *internalRulesTestWorkflow) WorkflowFunc(ctx workflow.Context) error {
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "activity-id",
		DisableEagerExecution:  true,
		StartToCloseTimeout:    w.startToCloseTimeout,
		ScheduleToCloseTimeout: w.scheduleToCloseTimeout,
		RetryPolicy:            w.activityRetryPolicy,
	}), w.ActivityFunc).Get(ctx, nil)
	return err
}

func (w *internalRulesTestWorkflow) ActivityFunc() (string, error) {
	w.startedActivityCount.Add(1)

	if !w.letActivitySucceed.Load() {
		activityErr := errors.New("bad-luck-please-retry")
		return "", activityErr
	}
	w.testSuite.WaitForChannel(w.ctx, w.activityCompleteCn)
	return "done!", nil
}

func (s *ActivityApiRulesClientTestSuite) SetupTest() {
	s.FunctionalTestSdkSuite.SetupTest()

	s.OverrideDynamicConfig(dynamicconfig.WorkflowRulesAPIsEnabled, true)

	s.initialRetryInterval = 1 * time.Second
	s.scheduleToCloseTimeout = 30 * time.Minute
	s.startToCloseTimeout = 15 * time.Minute

	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}
}

func (s *ActivityApiRulesClientTestSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
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

func (s *ActivityApiRulesClientTestSuite) createWorkflow(ctx context.Context, workflowFn WorkflowFunction) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	return workflowRun
}

func (s *ActivityApiRulesClientTestSuite) TestActivityRulesApi_WhileRetrying() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testWorkflow := newInternalRulesTestWorkflow(ctx, &s.FunctionalTestBase)

	s.Worker().RegisterWorkflow(testWorkflow.WorkflowFunc)
	s.Worker().RegisterActivity(testWorkflow.ActivityFunc)

	workflowRun := s.createWorkflow(ctx, testWorkflow.WorkflowFunc)

	// wait for activity to start and fail few times
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.PendingActivities, 1)
		}
		assert.Less(t, int32(1), testWorkflow.startedActivityCount.Load())
	}, 10*time.Second, 200*time.Millisecond)

	// create rule to pause activity
	ruleID := "pause-activity"
	activityType := "ActivityFunc"
	createRuleRequest := &workflowservice.CreateWorkflowRuleRequest{
		Namespace: s.Namespace().String(),
		Spec: &rulespb.WorkflowRuleSpec{
			Id: ruleID,
			Trigger: &rulespb.WorkflowRuleSpec_ActivityStart{
				ActivityStart: &rulespb.WorkflowRuleSpec_ActivityStartTrigger{
					Predicate: fmt.Sprintf("ActivityType = \"%s\"", activityType),
				},
			},
			Actions: []*rulespb.Action{
				{
					Variant: &rulespb.Action_Pause{
						Pause: &rulespb.Action_ActionPause{
							Scope: enumspb.RULE_ACTION_SCOPE_ACTIVITY,
						},
					},
				},
			},
		},
	}
	createRuleResponse, err := s.FrontendClient().CreateWorkflowRule(ctx, createRuleRequest)
	s.NoError(err)
	s.NotNil(createRuleResponse)

	// verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := s.FrontendClient().ListWorkflowRules(ctx, &workflowservice.ListWorkflowRulesRequest{
			Namespace: s.Namespace().String(),
		})
		s.NoError(err)
		s.NotNil(nsResp)
		s.NotNil(nsResp.Rules)
		if nsResp.GetRules() != nil {
			s.Len(nsResp.Rules, 1)
			s.Equal(ruleID, nsResp.Rules[0].Spec.Id)
		}
	}, 5*time.Second, 200*time.Millisecond)

	// wait for activity to be paused by rule
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.PendingActivities, 1)
			assert.True(t, description.PendingActivities[0].GetActivityType().GetName() == activityType)
			assert.True(t, description.PendingActivities[0].GetPaused())
		}
		assert.Less(t, int32(1), testWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	// unpause the activity
	_, err = s.FrontendClient().UnpauseActivity(ctx, &workflowservice.UnpauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Type{Type: activityType},
	})
	s.NoError(err)

	// wait for activity to be unpaused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.PendingActivities, 1)
			assert.True(t, description.PendingActivities[0].GetActivityType().GetName() == activityType)
			assert.False(t, description.PendingActivities[0].GetPaused())
		}
		assert.Less(t, int32(1), testWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	// unblock the activity and let the workflow finish
	testWorkflow.letActivitySucceed.Store(true)
	testWorkflow.activityCompleteCn <- struct{}{}

	// wait for workflow to finish
	var out string
	err = workflowRun.Get(ctx, &out)
}
