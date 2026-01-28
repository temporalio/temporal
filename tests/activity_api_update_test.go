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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	defaultMaximumAttempts = 100
)

type (
	ActivityFunctions func() (string, error)
	WorkflowFunction  func(context2 workflow.Context) error
)

func makeWorkflowFuncForActivityUpdate(
	activityFunction ActivityFunctions,
	scheduleToCloseTimeout time.Duration,
	initialRetryInterval time.Duration,
) WorkflowFunction {
	return func(ctx workflow.Context) error {

		activityRetryPolicy := &temporal.RetryPolicy{
			InitialInterval:    initialRetryInterval,
			BackoffCoefficient: 1,
			MaximumAttempts:    defaultMaximumAttempts,
		}

		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			ScheduleToCloseTimeout: scheduleToCloseTimeout,
			StartToCloseTimeout:    scheduleToCloseTimeout,
			RetryPolicy:            activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)
		return err
	}
}

func TestActivityApiUpdateClient(t *testing.T) {
	t.Run("ChangeRetryInterval", func(t *testing.T) {
		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		tv := testvars.New(t).
			WithTaskQueue(taskQueue).
			WithNamespaceName(s.Namespace())

		activityUpdated := make(chan struct{})

		var startedActivityCount atomic.Int32
		activityFunction := func() (string, error) {
			startedActivityCount.Add(1)
			if startedActivityCount.Load() == 1 {
				activityErr := errors.New("bad-luck-please-retry")

				return "", activityErr
			}

			s.WaitForChannel(ctx, activityUpdated)
			return "done!", nil
		}

		scheduleToCloseTimeout := 30 * time.Minute
		retryTimeout := 10 * time.Minute
		workflowFn := makeWorkflowFuncForActivityUpdate(activityFunction, scheduleToCloseTimeout, retryTimeout)

		worker.RegisterWorkflow(workflowFn)
		worker.RegisterActivity(activityFunction)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:        tv.WorkflowID(),
			TaskQueue: taskQueue,
		}

		workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
		require.NoError(t, err)

		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.Len(ct, description.GetPendingActivities(), 1)
			require.Equal(ct, int32(1), startedActivityCount.Load())
		}, 10*time.Second, 500*time.Millisecond)

		updateRequest := &workflowservice.UpdateActivityOptionsRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowRun.GetID(),
			},
			Activity: &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity-id"},
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Second),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
		}
		resp, err := s.FrontendClient().UpdateActivityOptions(ctx, updateRequest)
		require.NoError(t, err)
		require.NotNil(t, resp)

		description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, 1, len(description.PendingActivities))

		activityUpdated <- struct{}{}

		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err = sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.Empty(ct, description.GetPendingActivities())
			require.Equal(ct, int32(2), startedActivityCount.Load())
		}, 3*time.Second, 100*time.Millisecond)

		var out string
		err = workflowRun.Get(ctx, &out)

		require.NoError(t, err)
	})

	t.Run("ChangeScheduleToClose", func(t *testing.T) {
		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		tv := testvars.New(t).
			WithTaskQueue(taskQueue).
			WithNamespaceName(s.Namespace())

		var startedActivityCount atomic.Int32
		activityFunction := func() (string, error) {
			startedActivityCount.Add(1)
			if startedActivityCount.Load() == 1 {
				activityErr := errors.New("bad-luck-please-retry")
				return "", activityErr
			}
			return "done!", nil
		}

		scheduleToCloseTimeout := 30 * time.Minute
		retryTimeout := 10 * time.Minute

		workflowFn := makeWorkflowFuncForActivityUpdate(activityFunction, scheduleToCloseTimeout, retryTimeout)

		worker.RegisterWorkflow(workflowFn)
		worker.RegisterActivity(activityFunction)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:        tv.WorkflowID(),
			TaskQueue: taskQueue,
		}

		workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
		require.NoError(t, err)

		// wait for activity to start (and fail)
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.Len(ct, description.GetPendingActivities(), 1)
			require.Equal(ct, int32(1), startedActivityCount.Load())

		}, 2*time.Second, 200*time.Millisecond)

		// update schedule_to_close_timeout
		updateRequest := &workflowservice.UpdateActivityOptionsRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowRun.GetID(),
			},
			Activity: &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity-id"},
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout"}},
		}
		resp, err := s.FrontendClient().UpdateActivityOptions(ctx, updateRequest)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// activity should fail immediately
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.Empty(ct, description.GetPendingActivities())
			require.Equal(ct, int32(1), startedActivityCount.Load())
		}, 2*time.Second, 200*time.Millisecond)

		var out string
		err = workflowRun.Get(ctx, &out)
		var activityError *temporal.ActivityError
		require.True(t, errors.As(err, &activityError))
		require.Equal(t, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, activityError.RetryState())
		var timeoutError *temporal.TimeoutError
		require.True(t, errors.As(activityError.Unwrap(), &timeoutError))
		require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutError.TimeoutType())
		require.Equal(t, int32(1), startedActivityCount.Load())
	})

	t.Run("ChangeScheduleToCloseAndRetry", func(t *testing.T) {
		// change both schedule to close and retry policy
		// initial values are chosen in such a way that activity will fail due to schedule to close timeout
		// we change schedule to close to a longer value and retry policy to a shorter value
		// after that activity should succeed
		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		tv := testvars.New(t).
			WithTaskQueue(taskQueue).
			WithNamespaceName(s.Namespace())

		var startedActivityCount atomic.Int32
		activityFunction := func() (string, error) {
			startedActivityCount.Add(1)
			if startedActivityCount.Load() == 1 {
				activityErr := errors.New("bad-luck-please-retry")

				return "", activityErr
			}
			return "done!", nil
		}

		// make scheduleToClose shorter than retry 2nd retry interval
		scheduleToCloseTimeout := 8 * time.Second
		retryInterval := 5 * time.Second

		workflowFn := makeWorkflowFuncForActivityUpdate(
			activityFunction, scheduleToCloseTimeout, retryInterval)

		worker.RegisterWorkflow(workflowFn)
		worker.RegisterActivity(activityFunction)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:        tv.WorkflowID(),
			TaskQueue: taskQueue,
		}

		workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
		require.NoError(t, err)

		// wait for activity to start (and fail)
		s.EventuallyWithT(func(ct *assert.CollectT) {
			require.NotZero(ct, startedActivityCount.Load())
		}, 2*time.Second, 200*time.Millisecond)

		// update schedule_to_close_timeout, make it longer
		// also update retry policy interval, make it shorter
		newScheduleToCloseTimeout := 10 * time.Second
		updateRequest := &workflowservice.UpdateActivityOptionsRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowRun.GetID(),
			},
			Activity: &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity-id"},
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToCloseTimeout: durationpb.New(newScheduleToCloseTimeout),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Second),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout", "retry_policy.initial_interval"}},
		}

		resp, err := s.FrontendClient().UpdateActivityOptions(ctx, updateRequest)
		require.NoError(t, err)
		require.NotNil(t, resp)
		// check that the update was successful
		require.Equal(t, int64(newScheduleToCloseTimeout.Seconds()), resp.GetActivityOptions().ScheduleToCloseTimeout.GetSeconds())
		// check that field we didn't update is the same
		require.Equal(t, int64(scheduleToCloseTimeout.Seconds()), resp.GetActivityOptions().StartToCloseTimeout.GetSeconds())

		// now activity should succeed
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.Empty(ct, description.GetPendingActivities())
			require.Equal(ct, int32(2), startedActivityCount.Load())
		}, 5*time.Second, 200*time.Millisecond)

		var out string
		err = workflowRun.Get(ctx, &out)
		require.NoError(t, err)
	})

	t.Run("ResetDefaultOptions", func(t *testing.T) {
		// plan:
		// 1. start the workflow, wait for activity to start and fail,
		// 2. update activity options to change retry policy maximum attempts
		// 3. reset activity options to default, verify that retry policy is reset to default
		// 4. update activity options again, this time change schedule to close timeout and retry policy initial interval
		// 5. let activity finish, verify that it finished with updated options

		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		worker := sdkworker.New(sdkClient, taskQueue, sdkworker.Options{})

		tv := testvars.New(t).
			WithTaskQueue(taskQueue).
			WithNamespaceName(s.Namespace())

		activityUpdated := make(chan struct{})

		var startedActivityCount atomic.Int32
		activityFunction := func() (string, error) {
			startedActivityCount.Add(1)
			if startedActivityCount.Load() == 1 {
				activityErr := errors.New("bad-luck-please-retry")

				return "", activityErr
			}

			s.WaitForChannel(ctx, activityUpdated)
			return "done!", nil
		}

		scheduleToCloseTimeout := 30 * time.Minute
		retryTimeout := 10 * time.Minute
		workflowFn := makeWorkflowFuncForActivityUpdate(activityFunction, scheduleToCloseTimeout, retryTimeout)

		worker.RegisterWorkflow(workflowFn)
		worker.RegisterActivity(activityFunction)
		require.NoError(t, worker.Start())
		defer worker.Stop()

		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:        tv.WorkflowID(),
			TaskQueue: taskQueue,
		}

		workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
		require.NoError(t, err)

		// wait for activity to start (and fail)
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.Len(ct, description.GetPendingActivities(), 1)
			require.Equal(ct, int32(1), startedActivityCount.Load())
		}, 10*time.Second, 500*time.Millisecond)

		// update activity options, set retry policy to 1000 attempts
		updateRequest := &workflowservice.UpdateActivityOptionsRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowRun.GetID(),
			},
			Activity: &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity-id"},
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumAttempts: 1000,
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_attempts"}},
		}
		resp, err := s.FrontendClient().UpdateActivityOptions(ctx, updateRequest)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// check that the update was successful
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.Equal(ct, 1, len(description.PendingActivities))
			require.Equal(ct, int32(1000), description.PendingActivities[0].GetActivityOptions().GetRetryPolicy().GetMaximumAttempts())
		}, 3*time.Second, 200*time.Millisecond)

		// reset activity options to default
		updateRequest.ActivityOptions = nil
		updateRequest.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{}}
		updateRequest.RestoreOriginal = true
		resp, err = s.FrontendClient().UpdateActivityOptions(ctx, updateRequest)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// check that the update was successful
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.Equal(ct, 1, len(description.PendingActivities))
			require.Equal(ct, int32(defaultMaximumAttempts), description.PendingActivities[0].GetActivityOptions().GetRetryPolicy().GetMaximumAttempts())
		}, 3*time.Second, 200*time.Millisecond)

		// update activity options again, this time set retry interval to 1 second
		newScheduleToCloseTimeout := 10 * time.Second
		updateRequest.ActivityOptions = &activitypb.ActivityOptions{
			ScheduleToCloseTimeout: durationpb.New(newScheduleToCloseTimeout),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(1 * time.Second),
			},
		}
		updateRequest.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout", "retry_policy.initial_interval"}}
		updateRequest.RestoreOriginal = false
		resp, err = s.FrontendClient().UpdateActivityOptions(ctx, updateRequest)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// let activity finish
		activityUpdated <- struct{}{}

		// wait for activity to finish
		s.EventuallyWithT(func(ct *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			require.NoError(ct, err)
			require.Empty(ct, description.GetPendingActivities())
			require.Equal(ct, int32(2), startedActivityCount.Load())
		}, 3*time.Second, 100*time.Millisecond)

		var out string
		err = workflowRun.Get(ctx, &out)

		require.NoError(t, err)
	})
}
