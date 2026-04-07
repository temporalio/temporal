package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// makeActivityExecutionAPIWorkflowFunc returns a workflow that runs a single activity by name.
func makeActivityExecutionAPIWorkflowFunc(activityName string, retryPolicy *temporal.RetryPolicy) WorkflowFunction {
	return func(ctx workflow.Context) error {
		var ret string
		return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			ScheduleToCloseTimeout: 30 * time.Minute,
			StartToCloseTimeout:    15 * time.Minute,
			RetryPolicy:            retryPolicy,
		}), activityName).Get(ctx, &ret)
	}
}

func TestActivityExecutionAPI(t *testing.T) {
	s := testcore.NewEnv(t, testcore.WithSdkWorker())

	t.Run("PauseActivityExecution", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
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
		actName := testcore.RandomizeStr("act")
		wfName := testcore.RandomizeStr("wf")
		workflowFn := makeActivityExecutionAPIWorkflowFunc(actName, &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1,
		})
		s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wfName})
		s.SdkWorker().RegisterActivityWithOptions(activityFn, sdkactivity.RegisterOptions{Name: actName})

		workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr(t.Name()),
			TaskQueue: s.WorkerTaskQueue(),
		}, wfName)
		require.NoError(t, err)

		s.WaitForChannel(ctx, activityStartedCh)

		_, err = s.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			WorkflowId: workflowRun.GetID(),
			ActivityId: "activity-id",
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(t, err)
			require.Len(t, desc.PendingActivities, 1)
			require.True(t, desc.PendingActivities[0].Paused)
		}, 5*time.Second, 200*time.Millisecond)

		close(activityBlockCh)
	})

	t.Run("UnpauseActivityExecution", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
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
		actName := testcore.RandomizeStr("act")
		wfName := testcore.RandomizeStr("wf")
		workflowFn := makeActivityExecutionAPIWorkflowFunc(actName, &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1,
		})
		s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wfName})
		s.SdkWorker().RegisterActivityWithOptions(activityFn, sdkactivity.RegisterOptions{Name: actName})

		workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr(t.Name()),
			TaskQueue: s.WorkerTaskQueue(),
		}, wfName)
		require.NoError(t, err)

		s.WaitForChannel(ctx, activityStartedCh)

		_, err = s.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			WorkflowId: workflowRun.GetID(),
			ActivityId: "activity-id",
			Identity:   "test-identity",
			Reason:     "test-pause",
		})
		require.NoError(t, err)

		close(activityBlockCh)

		s.EventuallyWithT(func(t *assert.CollectT) {
			desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(t, err)
			require.Len(t, desc.PendingActivities, 1)
			require.True(t, desc.PendingActivities[0].Paused)
		}, 5*time.Second, 200*time.Millisecond)

		_, err = s.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			WorkflowId: workflowRun.GetID(),
			ActivityId: "activity-id",
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(t, err)
			require.Len(t, desc.PendingActivities, 1)
			require.False(t, desc.PendingActivities[0].Paused)
		}, 5*time.Second, 200*time.Millisecond)
	})

	t.Run("ResetActivityExecution", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
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
		actName := testcore.RandomizeStr("act")
		wfName := testcore.RandomizeStr("wf")
		workflowFn := makeActivityExecutionAPIWorkflowFunc(actName, &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 1,
		})
		s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wfName})
		s.SdkWorker().RegisterActivityWithOptions(activityFn, sdkactivity.RegisterOptions{Name: actName})

		workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr(t.Name()),
			TaskQueue: s.WorkerTaskQueue(),
		}, wfName)
		require.NoError(t, err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(t, err)
			require.Len(t, desc.PendingActivities, 1)
			require.Greater(t, desc.PendingActivities[0].Attempt, int32(1))
		}, 10*time.Second, 200*time.Millisecond)

		_, err = s.FrontendClient().ResetActivityExecution(ctx, &workflowservice.ResetActivityExecutionRequest{
			Namespace:  s.Namespace().String(),
			WorkflowId: workflowRun.GetID(),
			ActivityId: "activity-id",
			Identity:   "test-identity",
		})
		require.NoError(t, err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(t, err)
			require.Len(t, desc.PendingActivities, 1)
			require.Equal(t, int32(1), desc.PendingActivities[0].Attempt)
		}, 5*time.Second, 200*time.Millisecond)

		close(activityResetCh)
	})

	t.Run("UpdateActivityExecutionOptions", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
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
		actName := testcore.RandomizeStr("act")
		wfName := testcore.RandomizeStr("wf")
		workflowFn := makeActivityExecutionAPIWorkflowFunc(actName, &temporal.RetryPolicy{
			InitialInterval:    10 * time.Minute,
			BackoffCoefficient: 1,
			MaximumAttempts:    100,
		})
		s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wfName})
		s.SdkWorker().RegisterActivityWithOptions(activityFn, sdkactivity.RegisterOptions{Name: actName})

		workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr(t.Name()),
			TaskQueue: s.WorkerTaskQueue(),
		}, wfName)
		require.NoError(t, err)

		s.EventuallyWithT(func(t *assert.CollectT) {
			desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(t, err)
			require.Len(t, desc.PendingActivities, 1)
			require.Equal(t, int32(1), startedCount.Load())
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
		require.NoError(t, err)
		require.NotNil(t, resp)

		s.EventuallyWithT(func(t *assert.CollectT) {
			require.Equal(t, int32(2), startedCount.Load())
		}, 10*time.Second, 200*time.Millisecond)

		close(activityBlockCh)
	})
}
