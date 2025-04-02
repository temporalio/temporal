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
	rulespb "go.temporal.io/api/rules/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/util"
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
	activityFailedCn     chan struct{}

	testSuite *testcore.FunctionalTestBase
	logger    log.Logger
	ctx       context.Context
}

func newInternalRulesTestWorkflow(ctx context.Context, testSuite *testcore.FunctionalTestBase, logger log.Logger) *internalRulesTestWorkflow {
	wf := &internalRulesTestWorkflow{
		initialRetryInterval:   1 * time.Second,
		scheduleToCloseTimeout: 30 * time.Minute,
		startToCloseTimeout:    15 * time.Minute,
		activityCompleteCn:     make(chan struct{}),
		activityFailedCn:       make(chan struct{}),
		testSuite:              testSuite,
		ctx:                    ctx,
		logger:                 logger,
	}
	wf.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    wf.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	return wf
}

func (w *internalRulesTestWorkflow) WorkflowFuncForRetryTask(ctx workflow.Context) error {
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "activity-id",
		DisableEagerExecution:  true,
		StartToCloseTimeout:    w.startToCloseTimeout,
		ScheduleToCloseTimeout: w.scheduleToCloseTimeout,
		RetryPolicy:            w.activityRetryPolicy,
	}), w.ActivityFuncForRetryTask).Get(ctx, nil)
	return err
}

func (w *internalRulesTestWorkflow) WorkflowFuncForRetryActivity(ctx workflow.Context) error {
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "activity-id",
		DisableEagerExecution:  true,
		StartToCloseTimeout:    w.startToCloseTimeout,
		ScheduleToCloseTimeout: w.scheduleToCloseTimeout,
		RetryPolicy:            w.activityRetryPolicy,
	}), w.ActivityFuncForRetryActivity).Get(ctx, nil)
	return err
}

func (w *internalRulesTestWorkflow) ActivityFuncForRetryActivity() (string, error) {
	w.startedActivityCount.Add(1)

	if !w.letActivitySucceed.Load() {
		w.testSuite.WaitForChannel(w.ctx, w.activityFailedCn)
		activityErr := errors.New("bad-luck-please-retry")
		return "", activityErr
	}
	w.testSuite.WaitForChannel(w.ctx, w.activityCompleteCn)
	return "done!", nil
}

func (w *internalRulesTestWorkflow) ActivityFuncForRetryTask() (string, error) {
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

func (s *ActivityApiRulesClientTestSuite) TestActivityRulesApi_CRUD() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initial state - no rules
	nsResp, err := s.FrontendClient().ListWorkflowRules(ctx, &workflowservice.ListWorkflowRulesRequest{
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
	s.NotNil(nsResp)
	s.Len(nsResp.Rules, 0)

	// create a rule
	ruleID1 := "pause-activity-rule-1"
	activityType := "ActivityFunc"

	createRuleRequest := s.createPauseRuleRequest(activityType, ruleID1)
	createRuleResponse, err := s.FrontendClient().CreateWorkflowRule(ctx, createRuleRequest)
	s.NoError(err)
	s.NotNil(createRuleResponse)

	// verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := s.FrontendClient().ListWorkflowRules(ctx, &workflowservice.ListWorkflowRulesRequest{
			Namespace: s.Namespace().String(),
		})
		assert.NoError(s.T(), err)
		assert.NotNil(s.T(), nsResp)
		assert.NotNil(s.T(), nsResp.Rules)
		if assert.Len(s.T(), nsResp.Rules, 1) {
			assert.Equal(s.T(), ruleID1, nsResp.Rules[0].Spec.Id)
		}
	}, 5*time.Second, 200*time.Millisecond)

	// create a second rule with the same ID
	createRuleResponse, err = s.FrontendClient().CreateWorkflowRule(ctx, createRuleRequest)
	var invalidArgument *serviceerror.InvalidArgument
	s.Error(err)
	s.ErrorAs(err, &invalidArgument)
	s.Nil(createRuleResponse)

	// create a second rule with a different ID
	ruleID2 := "pause-activity-rule-2"
	createRuleRequest.Spec.Id = ruleID2
	createRuleResponse, err = s.FrontendClient().CreateWorkflowRule(ctx, createRuleRequest)
	s.NoError(err)
	s.NotNil(createRuleResponse)

	// verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := s.FrontendClient().ListWorkflowRules(ctx, &workflowservice.ListWorkflowRulesRequest{
			Namespace: s.Namespace().String(),
		})
		assert.NoError(s.T(), err)
		assert.NotNil(s.T(), nsResp)
		assert.NotNil(s.T(), nsResp.Rules)
		if assert.Len(s.T(), nsResp.Rules, 2) {
			// we can't guarantee the order of the rules
			assert.True(s.T(), nsResp.Rules[0].Spec.Id == ruleID1 || nsResp.Rules[1].Spec.Id == ruleID1)
			assert.True(s.T(), nsResp.Rules[0].Spec.Id == ruleID2 || nsResp.Rules[1].Spec.Id == ruleID2)
		}
	}, 5*time.Second, 200*time.Millisecond)

	// get rule by ID
	describeRuleResponse, err := s.FrontendClient().DescribeWorkflowRule(ctx, &workflowservice.DescribeWorkflowRuleRequest{
		Namespace: s.Namespace().String(),
		RuleId:    ruleID1,
	})
	s.NoError(err)
	s.NotNil(describeRuleResponse)
	s.Equal(ruleID1, describeRuleResponse.Rule.Spec.Id)

	describeRuleResponse, err = s.FrontendClient().DescribeWorkflowRule(ctx, &workflowservice.DescribeWorkflowRuleRequest{
		Namespace: s.Namespace().String(),
		RuleId:    ruleID2,
	})
	s.NoError(err)
	s.NotNil(describeRuleResponse)
	s.Equal(ruleID2, describeRuleResponse.Rule.Spec.Id)

	// delete rule 1
	deleteRuleResponse, err := s.FrontendClient().DeleteWorkflowRule(ctx, &workflowservice.DeleteWorkflowRuleRequest{
		Namespace: s.Namespace().String(),
		RuleId:    ruleID1,
	})
	s.NoError(err)
	s.NotNil(deleteRuleResponse)

	// verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := s.FrontendClient().ListWorkflowRules(ctx, &workflowservice.ListWorkflowRulesRequest{
			Namespace: s.Namespace().String(),
		})
		assert.NoError(s.T(), err)
		assert.NotNil(s.T(), nsResp)
		assert.NotNil(s.T(), nsResp.Rules)
		if assert.Len(s.T(), nsResp.Rules, 1) {
			// we can't guarantee the order of the rules
			assert.Equal(s.T(), ruleID2, nsResp.Rules[0].Spec.Id)
		}
	}, 5*time.Second, 200*time.Millisecond)

	// delete rule 2
	deleteRuleResponse, err = s.FrontendClient().DeleteWorkflowRule(ctx, &workflowservice.DeleteWorkflowRuleRequest{
		Namespace: s.Namespace().String(),
		RuleId:    ruleID2,
	})
	s.NoError(err)
	s.NotNil(deleteRuleResponse)

	// verify that frontend has updated namespaces and all rules are deleted
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := s.FrontendClient().ListWorkflowRules(ctx, &workflowservice.ListWorkflowRulesRequest{
			Namespace: s.Namespace().String(),
		})
		assert.NoError(s.T(), err)
		assert.NotNil(s.T(), nsResp)
		assert.Len(s.T(), nsResp.Rules, 0)
	}, 5*time.Second, 200*time.Millisecond)
}

func (s *ActivityApiRulesClientTestSuite) TestActivityRulesApi_RetryActivity() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testWorkflow := newInternalRulesTestWorkflow(ctx, &s.FunctionalTestBase, s.Logger)

	s.Worker().RegisterWorkflow(testWorkflow.WorkflowFuncForRetryActivity)
	s.Worker().RegisterActivity(testWorkflow.ActivityFuncForRetryActivity)

	workflowRun := s.createWorkflow(ctx, testWorkflow.WorkflowFuncForRetryActivity)

	// wait for activity to start
	s.EventuallyWithT(func(t *assert.CollectT) {
		assert.Equal(t, int32(1), testWorkflow.startedActivityCount.Load())
	}, 10*time.Second, 200*time.Millisecond)

	// create rule to pause activity
	ruleID := "pause-activity"
	activityType := "ActivityFuncForRetryActivity"
	createRuleRequest := s.createPauseRuleRequest(activityType, ruleID)
	createRuleResponse, err := s.FrontendClient().CreateWorkflowRule(ctx, createRuleRequest)
	s.NoError(err)
	s.NotNil(createRuleResponse)

	// verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := s.FrontendClient().ListWorkflowRules(ctx, &workflowservice.ListWorkflowRulesRequest{
			Namespace: s.Namespace().String(),
		})
		assert.NoError(s.T(), err)
		assert.NotNil(s.T(), nsResp)
		assert.NotNil(s.T(), nsResp.Rules)
		if nsResp.GetRules() != nil {
			assert.Len(s.T(), nsResp.Rules, 1)
			assert.Equal(s.T(), ruleID, nsResp.Rules[0].Spec.Id)
		}
	}, 5*time.Second, 200*time.Millisecond)

	// Let activity fail
	err = util.InterruptibleSleep(ctx, 1*time.Second)
	s.NoError(err)

	testWorkflow.activityFailedCn <- struct{}{}

	// check that activity was paused by the rule
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.PendingActivities, 1)
			assert.True(t, description.PendingActivities[0].GetActivityType().GetName() == activityType)
			assert.True(t, description.PendingActivities[0].GetPaused())
		}
		assert.Equal(t, int32(1), testWorkflow.startedActivityCount.Load())
	}, 2*time.Second, 200*time.Millisecond)

	// let activity succeed
	testWorkflow.letActivitySucceed.Store(true)

	// remove the rule so it didn't interfere with the activity
	deleteRuleResponse, err := s.FrontendClient().DeleteWorkflowRule(ctx, &workflowservice.DeleteWorkflowRuleRequest{
		Namespace: s.Namespace().String(),
		RuleId:    ruleID,
	})
	s.NoError(err)
	s.NotNil(deleteRuleResponse)

	// make sure there is no rules
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := s.FrontendClient().ListWorkflowRules(ctx, &workflowservice.ListWorkflowRulesRequest{
			Namespace: s.Namespace().String(),
		})
		assert.NoError(s.T(), err)
		assert.NotNil(s.T(), nsResp)
		assert.Len(s.T(), nsResp.Rules, 0)
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
		assert.Equal(t, int32(1), testWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	// let activity complete
	testWorkflow.activityCompleteCn <- struct{}{}

	// wait for workflow to finish
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiRulesClientTestSuite) TestActivityRulesApi_RetryTask() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// overall test execution plan:
	// 1. start workflow
	// 2. wait for activity to start and fail exactly once
	// 3. create rule to pause activity
	// 4. wait for activity to be paused by rule
	// 5. let activity succeed
	// 6. Remove the rule so it didn't interfere with the activity
	// 7. Make sure there is no rules
	// 6. Unpause the activity. this will also trigger the activity
	// 7. Wait for activity to be unpaused
	// 8. Let activity complete
	// 9. Wait for workflow to finish

	testRetryTaskWorkflow := newInternalRulesTestWorkflow(ctx, &s.FunctionalTestBase, s.Logger)

	// set much longer retry interval to make sure that activity is retried at least once
	s.initialRetryInterval = 4 * time.Second
	s.activityRetryPolicy.InitialInterval = s.initialRetryInterval

	s.Worker().RegisterWorkflow(testRetryTaskWorkflow.WorkflowFuncForRetryTask)
	s.Worker().RegisterActivity(testRetryTaskWorkflow.ActivityFuncForRetryTask)

	// 1. Start workflow
	workflowRun := s.createWorkflow(ctx, testRetryTaskWorkflow.WorkflowFuncForRetryTask)

	// 2. Wait for activity to start and fail exactly once
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.PendingActivities, 1)
		}
		assert.Equal(t, int32(1), testRetryTaskWorkflow.startedActivityCount.Load())
	}, 2*time.Second, 200*time.Millisecond)

	// 3. Create rule to pause activity
	ruleID := "pause-activity"
	activityType := "ActivityFuncForRetryTask"
	createRuleRequest := s.createPauseRuleRequest(activityType, ruleID)
	createRuleResponse, err := s.FrontendClient().CreateWorkflowRule(ctx, createRuleRequest)
	s.NoError(err)
	s.NotNil(createRuleResponse)

	// 4. verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := s.FrontendClient().ListWorkflowRules(ctx, &workflowservice.ListWorkflowRulesRequest{
			Namespace: s.Namespace().String(),
		})
		assert.NoError(s.T(), err)
		assert.NotNil(s.T(), nsResp)
		assert.NotNil(s.T(), nsResp.Rules)
		if nsResp.GetRules() != nil {
			assert.Len(s.T(), nsResp.Rules, 1)
			assert.Equal(s.T(), ruleID, nsResp.Rules[0].Spec.Id)
		}
	}, 5*time.Second, 200*time.Millisecond)

	// Let namespace config propagate.
	// There is no good way to check if the namespace config has propagated to the history service
	err = util.InterruptibleSleep(ctx, 2*time.Second)
	s.NoError(err)

	// 5. wait for activity to be paused by rule. This should happen in the activity retry task
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.PendingActivities, 1)
			assert.True(t, description.PendingActivities[0].GetActivityType().GetName() == activityType)
			assert.True(t, description.PendingActivities[0].GetPaused())
		}
		assert.Equal(t, int32(1), testRetryTaskWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	// let activity succeed
	testRetryTaskWorkflow.letActivitySucceed.Store(true)

	// remove the rule so it didn't interfere with the activity
	deleteRuleResponse, err := s.FrontendClient().DeleteWorkflowRule(ctx, &workflowservice.DeleteWorkflowRuleRequest{
		Namespace: s.Namespace().String(),
		RuleId:    ruleID,
	})
	s.NoError(err)
	s.NotNil(deleteRuleResponse)

	// make sure there is no rules
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := s.FrontendClient().ListWorkflowRules(ctx, &workflowservice.ListWorkflowRulesRequest{
			Namespace: s.Namespace().String(),
		})
		assert.NoError(s.T(), err)
		assert.NotNil(s.T(), nsResp)
		assert.Len(s.T(), nsResp.Rules, 0)
	}, 5*time.Second, 200*time.Millisecond)

	// Let namespace config propagate.
	// There is no good way to check if the namespace config has propagated to the history service
	err = util.InterruptibleSleep(ctx, 2*time.Second)
	s.NoError(err)

	// unpause the activity. this will also trigger the activity
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
		assert.LessOrEqual(t, int32(1), testRetryTaskWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	// let activity complete
	testRetryTaskWorkflow.activityCompleteCn <- struct{}{}
	// wait for workflow to finish
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiRulesClientTestSuite) createPauseRuleRequest(
	activityType string, ruleID string,
) *workflowservice.CreateWorkflowRuleRequest {
	createRuleRequest := &workflowservice.CreateWorkflowRuleRequest{
		Namespace: s.Namespace().String(),
		Spec: &rulespb.WorkflowRuleSpec{
			Id: ruleID,
			Trigger: &rulespb.WorkflowRuleSpec_ActivityStart{
				ActivityStart: &rulespb.WorkflowRuleSpec_ActivityStartingTrigger{
					Predicate: fmt.Sprintf("ActivityType = \"%s\"", activityType),
				},
			},
			Actions: []*rulespb.WorkflowRuleAction{
				{
					Variant: &rulespb.WorkflowRuleAction_ActivityPause{
						ActivityPause: &rulespb.WorkflowRuleAction_ActionActivityPause{},
					},
				},
			},
		},
	}
	return createRuleRequest
}
