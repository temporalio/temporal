package tests

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	rulespb "go.temporal.io/api/rules/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
)

type ActivityApiRulesClientTestSuite struct {
	parallelsuite.Suite[*ActivityApiRulesClientTestSuite]
}

func TestActivityApiRulesClientTestSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &ActivityApiRulesClientTestSuite{})
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

	env *testcore.TestEnv
}

func newInternalRulesTestWorkflow(env *testcore.TestEnv) *internalRulesTestWorkflow {
	wf := &internalRulesTestWorkflow{
		initialRetryInterval:   1 * time.Second,
		scheduleToCloseTimeout: 30 * time.Minute,
		startToCloseTimeout:    15 * time.Minute,
		activityCompleteCn:     make(chan struct{}),
		activityFailedCn:       make(chan struct{}),
		env:                    env,
	}
	wf.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    wf.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	return wf
}

func (w *internalRulesTestWorkflow) workflowFunc(ctx workflow.Context, activityFunc func() (string, error)) error {
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "activity-id",
		DisableEagerExecution:  true,
		StartToCloseTimeout:    w.startToCloseTimeout,
		ScheduleToCloseTimeout: w.scheduleToCloseTimeout,
		RetryPolicy:            w.activityRetryPolicy,
	}), activityFunc).Get(ctx, nil)
	return err
}

func (w *internalRulesTestWorkflow) WorkflowFuncForRetryActivity(ctx workflow.Context) error {
	return w.workflowFunc(ctx, w.ActivityFuncForRetryActivity)
}

func (w *internalRulesTestWorkflow) WorkflowFuncForRetryTask(ctx workflow.Context) error {
	return w.workflowFunc(ctx, w.ActivityFuncForRetryTask)
}

func (w *internalRulesTestWorkflow) WorkflowFuncForPrePause(ctx workflow.Context) error {
	return w.workflowFunc(ctx, w.ActivityFuncForPrePause)
}

func (w *internalRulesTestWorkflow) ActivityFuncForRetryActivity() (string, error) {
	w.startedActivityCount.Add(1)

	if !w.letActivitySucceed.Load() {
		w.env.WaitForChannel(w.activityFailedCn)
		activityErr := errors.New("bad-luck-please-retry")
		return "", activityErr
	}
	w.env.WaitForChannel(w.activityCompleteCn)
	return "done!", nil
}

func (w *internalRulesTestWorkflow) ActivityFuncForRetryTask() (string, error) {
	w.startedActivityCount.Add(1)

	if !w.letActivitySucceed.Load() {
		activityErr := errors.New("bad-luck-please-retry")
		return "", activityErr
	}
	w.env.WaitForChannel(w.activityCompleteCn)
	return "done!", nil
}

func (w *internalRulesTestWorkflow) ActivityFuncForPrePause() (string, error) {
	w.startedActivityCount.Add(1)
	w.env.WaitForChannel(w.activityCompleteCn)
	return "done!", nil
}

func (s *ActivityApiRulesClientTestSuite) newTestEnv() *testcore.TestEnv {
	return testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowRulesAPIsEnabled, true),
	)
}

func (s *ActivityApiRulesClientTestSuite) createWorkflow(env *testcore.TestEnv, workflowFn WorkflowFunction) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	return workflowRun
}

func (s *ActivityApiRulesClientTestSuite) TestActivityRulesApi_CRUD() {
	env := s.newTestEnv()

	// Initial state - no rules
	nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
		Namespace: env.Namespace().String(),
	})
	s.NoError(err)
	s.NotNil(nsResp)
	s.Empty(nsResp.Rules)

	// create a rule
	ruleID1 := "pause-activity-rule-1"
	activityType := "ActivityFunc"

	createRuleRequest := s.createPauseRuleRequest(env.Namespace().String(), activityType, ruleID1)
	createRuleResponse, err := env.FrontendClient().CreateWorkflowRule(s.Context(), createRuleRequest)
	s.NoError(err)
	s.NotNil(createRuleResponse)

	// verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.NotNil(t, nsResp)
		require.NotNil(t, nsResp.Rules)
		require.Len(t, nsResp.Rules, 1)
		require.Equal(t, ruleID1, nsResp.Rules[0].Spec.Id)
	}, 5*time.Second, 200*time.Millisecond)

	// create a second rule with the same ID
	createRuleResponse, err = env.FrontendClient().CreateWorkflowRule(s.Context(), createRuleRequest)
	var invalidArgument *serviceerror.InvalidArgument
	s.Error(err)
	s.ErrorAs(err, &invalidArgument)
	s.Nil(createRuleResponse)

	// create a second rule with a different ID
	ruleID2 := "pause-activity-rule-2"
	createRuleRequest.Spec.Id = ruleID2
	createRuleResponse, err = env.FrontendClient().CreateWorkflowRule(s.Context(), createRuleRequest)
	s.NoError(err)
	s.NotNil(createRuleResponse)

	// verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.NotNil(t, nsResp)
		require.NotNil(t, nsResp.Rules)
		require.Len(t, nsResp.Rules, 2)
		// we can't guarantee the order of the rules
		require.True(t, nsResp.Rules[0].Spec.Id == ruleID1 || nsResp.Rules[1].Spec.Id == ruleID1)
		require.True(t, nsResp.Rules[0].Spec.Id == ruleID2 || nsResp.Rules[1].Spec.Id == ruleID2)
	}, 5*time.Second, 200*time.Millisecond)

	// get rule by ID
	describeRuleResponse, err := env.FrontendClient().DescribeWorkflowRule(s.Context(), &workflowservice.DescribeWorkflowRuleRequest{
		Namespace: env.Namespace().String(),
		RuleId:    ruleID1,
	})
	s.NoError(err)
	s.NotNil(describeRuleResponse)
	s.Equal(ruleID1, describeRuleResponse.Rule.Spec.Id)

	describeRuleResponse, err = env.FrontendClient().DescribeWorkflowRule(s.Context(), &workflowservice.DescribeWorkflowRuleRequest{
		Namespace: env.Namespace().String(),
		RuleId:    ruleID2,
	})
	s.NoError(err)
	s.NotNil(describeRuleResponse)
	s.Equal(ruleID2, describeRuleResponse.Rule.Spec.Id)

	// delete rule 1
	deleteRuleResponse, err := env.FrontendClient().DeleteWorkflowRule(s.Context(), &workflowservice.DeleteWorkflowRuleRequest{
		Namespace: env.Namespace().String(),
		RuleId:    ruleID1,
	})
	s.NoError(err)
	s.NotNil(deleteRuleResponse)

	// verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.NotNil(t, nsResp)
		require.NotNil(t, nsResp.Rules)
		require.Len(t, nsResp.Rules, 1)
		// we can't guarantee the order of the rules
		require.Equal(t, ruleID2, nsResp.Rules[0].Spec.Id)
	}, 5*time.Second, 200*time.Millisecond)

	// delete rule 2
	deleteRuleResponse, err = env.FrontendClient().DeleteWorkflowRule(s.Context(), &workflowservice.DeleteWorkflowRuleRequest{
		Namespace: env.Namespace().String(),
		RuleId:    ruleID2,
	})
	s.NoError(err)
	s.NotNil(deleteRuleResponse)

	// verify that frontend has updated namespaces and all rules are deleted
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.NotNil(t, nsResp)
		require.Empty(t, nsResp.Rules)
	}, 5*time.Second, 200*time.Millisecond)
}

func (s *ActivityApiRulesClientTestSuite) TestActivityRulesApi_RetryActivity() {
	env := s.newTestEnv()

	testWorkflow := newInternalRulesTestWorkflow(env)
	env.SdkWorker().RegisterWorkflow(testWorkflow.WorkflowFuncForRetryActivity)
	env.SdkWorker().RegisterActivity(testWorkflow.ActivityFuncForRetryActivity)

	workflowRun := s.createWorkflow(env, testWorkflow.WorkflowFuncForRetryActivity)

	// wait for activity to start
	s.EventuallyWithT(func(t *assert.CollectT) {
		require.Equal(t, int32(1), testWorkflow.startedActivityCount.Load())
	}, 10*time.Second, 200*time.Millisecond)

	// create rule to pause activity
	ruleID := "pause-activity"
	activityType := "ActivityFuncForRetryActivity"
	createRuleRequest := s.createPauseRuleRequest(env.Namespace().String(), activityType, ruleID)
	createRuleResponse, err := env.FrontendClient().CreateWorkflowRule(s.Context(), createRuleRequest)
	s.NoError(err)
	s.NotNil(createRuleResponse)

	// verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.NotNil(t, nsResp)
		require.Len(t, nsResp.Rules, 1)
		require.Equal(t, ruleID, nsResp.Rules[0].Spec.Id)
	}, 5*time.Second, 200*time.Millisecond)

	// Let namespace config propagate.
	// There is no good way to check if the namespace config has propagated to the history service
	err = util.InterruptibleSleep(s.Context(), 4*time.Second)
	s.NoError(err)

	testWorkflow.activityFailedCn <- struct{}{}

	// check that activity was paused by the rule
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, activityType, description.PendingActivities[0].GetActivityType().GetName())
		require.True(t, description.PendingActivities[0].GetPaused())
		require.Equal(t, int32(1), testWorkflow.startedActivityCount.Load())
	}, 2*time.Second, 200*time.Millisecond)

	// make sure activity pause info is set
	description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Len(description.PendingActivities, 1)
	s.True(description.PendingActivities[0].Paused)
	s.NotNil(description.PendingActivities[0].PauseInfo)
	rule := description.PendingActivities[0].PauseInfo.GetRule()
	s.NotNil(rule)
	s.Equal(ruleID, rule.RuleId)

	// let activity succeed
	testWorkflow.letActivitySucceed.Store(true)

	// remove the rule so it didn't interfere with the activity
	deleteRuleResponse, err := env.FrontendClient().DeleteWorkflowRule(s.Context(), &workflowservice.DeleteWorkflowRuleRequest{
		Namespace: env.Namespace().String(),
		RuleId:    ruleID,
	})
	s.NoError(err)
	s.NotNil(deleteRuleResponse)

	// make sure there is no rules
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.NotNil(t, nsResp)
		require.Empty(t, nsResp.Rules)
	}, 5*time.Second, 200*time.Millisecond)

	// Let namespace config propagate.
	// There is no good way to check if the namespace config has propagated to the history service
	err = util.InterruptibleSleep(s.Context(), 4*time.Second)
	s.NoError(err)

	// unpause the activity
	_, err = env.FrontendClient().UnpauseActivity(s.Context(), &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Type{Type: activityType},
	})
	s.NoError(err)

	// wait for activity to be unpaused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, activityType, description.PendingActivities[0].GetActivityType().GetName())
		require.False(t, description.PendingActivities[0].GetPaused())
		require.LessOrEqual(t, int32(1), testWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	// let activity complete
	testWorkflow.activityCompleteCn <- struct{}{}

	// wait for workflow to finish
	var out string
	err = workflowRun.Get(s.Context(), &out)
	s.NoError(err)
}

func (s *ActivityApiRulesClientTestSuite) TestActivityRulesApi_RetryTask() {
	env := s.newTestEnv()

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

	testRetryTaskWorkflow := newInternalRulesTestWorkflow(env)

	// set much longer retry interval to make sure that activity is retried at least once
	testRetryTaskWorkflow.activityRetryPolicy.InitialInterval = 4 * time.Second

	env.SdkWorker().RegisterWorkflow(testRetryTaskWorkflow.WorkflowFuncForRetryTask)
	env.SdkWorker().RegisterActivity(testRetryTaskWorkflow.ActivityFuncForRetryTask)

	// 1. Start workflow
	workflowRun := s.createWorkflow(env, testRetryTaskWorkflow.WorkflowFuncForRetryTask)

	// 2. Wait for activity to start and fail exactly once
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, int32(1), testRetryTaskWorkflow.startedActivityCount.Load())
	}, 2*time.Second, 200*time.Millisecond)

	// 3. Create rule to pause activity
	ruleID := "pause-activity"
	activityType := "ActivityFuncForRetryTask"
	createRuleRequest := s.createPauseRuleRequest(env.Namespace().String(), activityType, ruleID)
	createRuleResponse, err := env.FrontendClient().CreateWorkflowRule(s.Context(), createRuleRequest)
	s.NoError(err)
	s.NotNil(createRuleResponse)

	// 4. verify that frontend has updated namespaces
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.NotNil(t, nsResp)
		require.Len(t, nsResp.Rules, 1)
		require.Equal(t, ruleID, nsResp.Rules[0].Spec.Id)
	}, 5*time.Second, 200*time.Millisecond)

	// Let namespace config propagate.
	// There is no good way to check if the namespace config has propagated to the history service
	err = util.InterruptibleSleep(s.Context(), 2*time.Second)
	s.NoError(err)

	// 5. wait for activity to be paused by rule. This should happen in the activity retry task
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, activityType, description.PendingActivities[0].GetActivityType().GetName())
		require.True(t, description.PendingActivities[0].GetPaused())
		require.Equal(t, int32(1), testRetryTaskWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	// make sure activity pause info is set
	description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Len(description.PendingActivities, 1)
	s.True(description.PendingActivities[0].Paused)
	s.NotNil(description.PendingActivities[0].PauseInfo)
	rule := description.PendingActivities[0].PauseInfo.GetRule()
	s.NotNil(rule)
	s.Equal(ruleID, rule.RuleId)

	// let activity succeed
	testRetryTaskWorkflow.letActivitySucceed.Store(true)

	// remove the rule so it didn't interfere with the activity
	deleteRuleResponse, err := env.FrontendClient().DeleteWorkflowRule(s.Context(), &workflowservice.DeleteWorkflowRuleRequest{
		Namespace: env.Namespace().String(),
		RuleId:    ruleID,
	})
	s.NoError(err)
	s.NotNil(deleteRuleResponse)

	// make sure there is no rules
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.NotNil(t, nsResp)
		require.Empty(t, nsResp.Rules)
	}, 5*time.Second, 200*time.Millisecond)

	// Let namespace config propagate.
	// There is no good way to check if the namespace config has propagated to the history service
	err = util.InterruptibleSleep(s.Context(), 2*time.Second)
	s.NoError(err)

	// unpause the activity. this will also trigger the activity
	_, err = env.FrontendClient().UnpauseActivity(s.Context(), &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Type{Type: activityType},
	})
	s.NoError(err)

	// wait for activity to be unpaused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, activityType, description.PendingActivities[0].GetActivityType().GetName())
		require.False(t, description.PendingActivities[0].GetPaused())
		require.LessOrEqual(t, int32(1), testRetryTaskWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	// let activity complete
	testRetryTaskWorkflow.activityCompleteCn <- struct{}{}
	// wait for workflow to finish
	var out string
	err = workflowRun.Get(s.Context(), &out)
	s.NoError(err)
}

func (s *ActivityApiRulesClientTestSuite) TestActivityRulesApi_PrePause() {
	env := s.newTestEnv()

	// overall test execution plan:
	// 1. create rule to pause activity
	// 2. wait for rule to be propagated to frontend
	// 2. wait for rule to be propagated to history service
	// 4. start workflow
	// 5. wait for activity to be paused by rule
	// 6. Remove the rule so it didn't interfere with the activity
	// 7. Make sure there is no rules in frontend service
	// 8. Let namespace config changes propagate to the history service.
	// 9. Unpause the activity. this will also trigger the activity
	// 10. Wait for activity to be unpaused
	// 11. Let activity complete
	// 12. Wait for workflow to finish

	testRetryTaskWorkflow := newInternalRulesTestWorkflow(env)

	env.SdkWorker().RegisterWorkflow(testRetryTaskWorkflow.WorkflowFuncForPrePause)
	env.SdkWorker().RegisterActivity(testRetryTaskWorkflow.ActivityFuncForPrePause)

	// 1. Create rule to pause activity
	ruleID := "pause-activity"
	activityType := "ActivityFuncForPrePause"
	createRuleRequest := s.createPauseRuleRequest(env.Namespace().String(), activityType, ruleID)
	createRuleResponse, err := env.FrontendClient().CreateWorkflowRule(s.Context(), createRuleRequest)
	s.NoError(err)
	s.NotNil(createRuleResponse)

	// 2. Verify that frontend has updated namespaces and rules are available
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.NotNil(t, nsResp)
		require.NotNil(t, nsResp.Rules)
		require.Len(t, nsResp.Rules, 1)
		require.Equal(t, ruleID, nsResp.Rules[0].Spec.Id)
	}, 5*time.Second, 200*time.Millisecond)

	// 3. Let namespace config propagate to the history service.
	// There is no good way to check if the namespace config has propagated to the history service
	err = util.InterruptibleSleep(s.Context(), 2*time.Second)
	s.NoError(err)

	// 4. Start workflow
	workflowRun := s.createWorkflow(env, testRetryTaskWorkflow.WorkflowFuncForPrePause)

	// 5. Wait for activity to be paused by rule. This should happen in the recording activity task started
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, activityType, description.PendingActivities[0].GetActivityType().GetName())
		require.True(t, description.PendingActivities[0].GetPaused())
		require.NotNil(t, description.PendingActivities[0].GetPauseInfo())
		require.Equal(t, ruleID, description.PendingActivities[0].GetPauseInfo().GetRule().GetRuleId())
		// to be sure activity doesn't actually start
		require.Equal(t, int32(0), testRetryTaskWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	// make sure activity pause info is set
	description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Len(description.PendingActivities, 1)
	s.True(description.PendingActivities[0].Paused)
	s.NotNil(description.PendingActivities[0].PauseInfo)
	s.Equal(ruleID, description.PendingActivities[0].PauseInfo.GetRule().GetRuleId())

	// 6. Remove the rule so it didn't interfere with the activity
	deleteRuleResponse, err := env.FrontendClient().DeleteWorkflowRule(s.Context(), &workflowservice.DeleteWorkflowRuleRequest{
		Namespace: env.Namespace().String(),
		RuleId:    ruleID,
	})
	s.NoError(err)
	s.NotNil(deleteRuleResponse)

	// 7. Make sure there is no rules in frontend
	s.EventuallyWithT(func(t *assert.CollectT) {
		nsResp, err := env.FrontendClient().ListWorkflowRules(s.Context(), &workflowservice.ListWorkflowRulesRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(t, err)
		require.NotNil(t, nsResp)
		require.Empty(t, nsResp.Rules)
	}, 5*time.Second, 200*time.Millisecond)

	// 8. Let namespace config changes propagate to the history service.
	// There is no good way to check if the namespace config has propagated to the history service
	err = util.InterruptibleSleep(s.Context(), 2*time.Second)
	s.NoError(err)

	// 9. Unpause the activity. this will also trigger the activity
	_, err = env.FrontendClient().UnpauseActivity(s.Context(), &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Type{Type: activityType},
	})
	s.NoError(err)

	// 10. Wait for activity to be unpaused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, activityType, description.PendingActivities[0].GetActivityType().GetName())
		require.False(t, description.PendingActivities[0].GetPaused())
		require.Equal(t, int32(1), testRetryTaskWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	// 11. Let activity complete
	testRetryTaskWorkflow.activityCompleteCn <- struct{}{}

	// 12. Wait for workflow to finish
	var out string
	err = workflowRun.Get(s.Context(), &out)
	s.NoError(err)
}

func (s *ActivityApiRulesClientTestSuite) createPauseRuleRequest(
	namespace, activityType, ruleID string,
) *workflowservice.CreateWorkflowRuleRequest {
	createRuleRequest := &workflowservice.CreateWorkflowRuleRequest{
		Namespace: namespace,
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
