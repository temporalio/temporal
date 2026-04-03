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
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type ActivityExecutionApiTestSuite struct {
	testcore.FunctionalTestBase
}

func TestActivityExecutionApiTestSuite(t *testing.T) {
	suite.Run(t, new(ActivityExecutionApiTestSuite))
}

func (s *ActivityExecutionApiTestSuite) makeWorkflowFunc(activityFn ActivityFunctions, retryPolicy *temporal.RetryPolicy) WorkflowFunction {
	return func(ctx workflow.Context) error {
		var ret string
		return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			ScheduleToCloseTimeout: 30 * time.Minute,
			StartToCloseTimeout:    15 * time.Minute,
			RetryPolicy:            retryPolicy,
		}), activityFn).Get(ctx, &ret)
	}
}

func (s *ActivityExecutionApiTestSuite) TestPauseActivityExecution() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityStartedCh := make(chan struct{})
	activityBlockCh := make(chan struct{})
	var startedCount atomic.Int32

	activityFn := func() (string, error) {
		if startedCount.Add(1) == 1 {
			close(activityStartedCh)
			s.WaitForChannel(ctx, activityBlockCh)
		}
		return "done!", nil
	}
	workflowFn := s.makeWorkflowFunc(activityFn, &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 1,
	})
	s.SdkWorker().RegisterWorkflow(workflowFn)
	s.SdkWorker().RegisterActivity(activityFn)

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr(s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}, workflowFn)
	s.NoError(err)

	s.WaitForChannel(ctx, activityStartedCh)

	_, err = s.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		Identity:   "test-identity",
		Reason:     "test-pause",
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		s.NoError(err)
		s.Len(desc.PendingActivities, 1)
		s.True(desc.PendingActivities[0].Paused)
	}, 5*time.Second, 200*time.Millisecond)

	close(activityBlockCh)
}

func (s *ActivityExecutionApiTestSuite) TestUnpauseActivityExecution() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityStartedCh := make(chan struct{})
	activityBlockCh := make(chan struct{})
	var startedCount atomic.Int32

	activityFn := func() (string, error) {
		if startedCount.Add(1) == 1 {
			close(activityStartedCh)
			s.WaitForChannel(ctx, activityBlockCh)
			return "", errors.New("retry-me")
		}
		return "done!", nil
	}
	workflowFn := s.makeWorkflowFunc(activityFn, &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 1,
	})
	s.SdkWorker().RegisterWorkflow(workflowFn)
	s.SdkWorker().RegisterActivity(activityFn)

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr(s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}, workflowFn)
	s.NoError(err)

	s.WaitForChannel(ctx, activityStartedCh)

	_, err = s.FrontendClient().PauseActivity(ctx, &workflowservice.PauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowRun.GetID()},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
		Reason:    "test-pause",
	})
	s.NoError(err)

	close(activityBlockCh)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		s.NoError(err)
		s.Len(desc.PendingActivities, 1)
		s.True(desc.PendingActivities[0].Paused)
	}, 5*time.Second, 200*time.Millisecond)

	_, err = s.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		Identity:   "test-identity",
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		s.NoError(err)
		s.Len(desc.PendingActivities, 1)
		s.False(desc.PendingActivities[0].Paused)
	}, 5*time.Second, 200*time.Millisecond)
}

func (s *ActivityExecutionApiTestSuite) TestResetActivityExecution() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var startedCount atomic.Int32
	activityResetCh := make(chan struct{})

	activityFn := func() (string, error) {
		if startedCount.Add(1) == 1 {
			return "", errors.New("retry-me")
		}
		s.WaitForChannel(ctx, activityResetCh)
		return "done!", nil
	}
	workflowFn := s.makeWorkflowFunc(activityFn, &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 1,
	})
	s.SdkWorker().RegisterWorkflow(workflowFn)
	s.SdkWorker().RegisterActivity(activityFn)

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr(s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}, workflowFn)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		s.NoError(err)
		s.Len(desc.PendingActivities, 1)
		s.Greater(desc.PendingActivities[0].Attempt, int32(1))
	}, 10*time.Second, 200*time.Millisecond)

	_, err = s.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		Identity:   "test-identity",
	})
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		s.NoError(err)
		s.Len(desc.PendingActivities, 1)
		s.Equal(int32(1), desc.PendingActivities[0].Attempt)
	}, 5*time.Second, 200*time.Millisecond)

	close(activityResetCh)
}

func (s *ActivityExecutionApiTestSuite) TestUpdateActivityExecutionOptions() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityBlockCh := make(chan struct{})
	var startedCount atomic.Int32

	activityFn := func() (string, error) {
		if startedCount.Add(1) == 1 {
			return "", errors.New("retry-me")
		}
		s.WaitForChannel(ctx, activityBlockCh)
		return "done!", nil
	}
	workflowFn := s.makeWorkflowFunc(activityFn, &temporal.RetryPolicy{
		InitialInterval:    10 * time.Minute,
		BackoffCoefficient: 1,
		MaximumAttempts:    100,
	})
	s.SdkWorker().RegisterWorkflow(workflowFn)
	s.SdkWorker().RegisterActivity(activityFn)

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr(s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}, workflowFn)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		s.NoError(err)
		s.Len(desc.PendingActivities, 1)
		s.Equal(int32(1), startedCount.Load())
	}, 5*time.Second, 200*time.Millisecond)

	resp, err := s.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: workflowRun.GetID(),
		ActivityId: "activity-id",
		Identity:   "test-identity",
		ActivityOptions: &activitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(time.Second),
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
	})
	s.NoError(err)
	s.NotNil(resp)

	s.EventuallyWithT(func(t *assert.CollectT) {
		s.Equal(int32(2), startedCount.Load())
	}, 10*time.Second, 200*time.Millisecond)

	close(activityBlockCh)
}
