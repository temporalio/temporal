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
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	defaultMaximumAttempts   = 100
	activityUpdateWorkflowID = "activity-update-workflow-id"
)

type (
	ActivityFunctions func() (string, error)
	WorkflowFunction  func(workflow.Context) error
)

func makeActivityUpdateWorkflowFunc(
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

// activityUpdateAPI abstracts UpdateActivityOptions/UpdateActivityExecutionOptions
// so the same test body can verify both APIs.
type activityUpdateAPI struct {
	name   string
	update func(ctx context.Context, s *testcore.TestEnv, wfID, actID string, opts *activitypb.ActivityOptions, maskPaths []string, restoreOriginal bool) (*activitypb.ActivityOptions, error)
}

func updateAPIs() []activityUpdateAPI {
	return []activityUpdateAPI{
		{
			name: "legacy-api",
			update: func(ctx context.Context, s *testcore.TestEnv, wfID, actID string, opts *activitypb.ActivityOptions, maskPaths []string, restoreOriginal bool) (*activitypb.ActivityOptions, error) {
				resp, err := s.FrontendClient().UpdateActivityOptions(ctx, &workflowservice.UpdateActivityOptionsRequest{
					Namespace:       s.Namespace().String(),
					Execution:       &commonpb.WorkflowExecution{WorkflowId: wfID},
					Activity:        &workflowservice.UpdateActivityOptionsRequest_Id{Id: actID},
					ActivityOptions: opts,
					UpdateMask:      &fieldmaskpb.FieldMask{Paths: maskPaths},
					RestoreOriginal: restoreOriginal,
				})
				if err != nil {
					return nil, err
				}
				return resp.GetActivityOptions(), nil
			},
		},
		{
			name: "execution-api",
			update: func(ctx context.Context, s *testcore.TestEnv, wfID, actID string, opts *activitypb.ActivityOptions, maskPaths []string, restoreOriginal bool) (*activitypb.ActivityOptions, error) {
				resp, err := s.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace:       s.Namespace().String(),
					WorkflowId:      wfID,
					ActivityId:      actID,
					ActivityOptions: opts,
					UpdateMask:      &fieldmaskpb.FieldMask{Paths: maskPaths},
					RestoreOriginal: restoreOriginal,
				})
				if err != nil {
					return nil, err
				}
				return resp.GetActivityOptions(), nil
			},
		},
	}
}

func TestActivityApiUpdateClientTestSuite(t *testing.T) {
	t.Parallel()

	for _, api := range updateAPIs() {
		api := api
		t.Run(api.name, func(t *testing.T) {
			t.Parallel()

			t.Run("TestActivityUpdateApi_ChangeRetryInterval", func(t *testing.T) {
				s := testcore.NewEnv(t, testcore.WithSdkWorker())

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

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
				workflowFn := makeActivityUpdateWorkflowFunc(activityFunction, scheduleToCloseTimeout, retryTimeout)

				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        activityUpdateWorkflowID,
					TaskQueue: s.WorkerTaskQueue(),
				}

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				s.EventuallyWithT(func(t *assert.CollectT) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.GetPendingActivities(), 1)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 10*time.Second, 500*time.Millisecond)

				_, err = api.update(ctx, s, workflowRun.GetID(), "activity-id",
					&activitypb.ActivityOptions{
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval: durationpb.New(1 * time.Second),
						},
					},
					[]string{"retry_policy.initial_interval"},
					false,
				)
				s.NoError(err)

				description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				s.NoError(err)
				s.Len(description.PendingActivities, 1)

				activityUpdated <- struct{}{}

				s.EventuallyWithT(func(t *assert.CollectT) {
					description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Empty(t, description.GetPendingActivities())
					require.Equal(t, int32(2), startedActivityCount.Load())
				}, 3*time.Second, 100*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)

				s.NoError(err)
			})

			t.Run("TestActivityUpdateApi_ChangeScheduleToClose", func(t *testing.T) {
				s := testcore.NewEnv(t, testcore.WithSdkWorker())

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

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

				workflowFn := makeActivityUpdateWorkflowFunc(activityFunction, scheduleToCloseTimeout, retryTimeout)

				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        activityUpdateWorkflowID,
					TaskQueue: s.WorkerTaskQueue(),
				}

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start (and fail)
				s.EventuallyWithT(func(t *assert.CollectT) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.GetPendingActivities(), 1)
					require.Equal(t, int32(1), startedActivityCount.Load())

				}, 2*time.Second, 200*time.Millisecond)

				// update schedule_to_close_timeout
				_, err = api.update(ctx, s, workflowRun.GetID(), "activity-id",
					&activitypb.ActivityOptions{
						ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
					},
					[]string{"schedule_to_close_timeout"},
					false,
				)
				s.NoError(err)

				// activity should fail immediately
				s.EventuallyWithT(func(t *assert.CollectT) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Empty(t, description.GetPendingActivities())
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 2*time.Second, 200*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)
				var activityError *temporal.ActivityError
				s.ErrorAs(err, &activityError)
				// SCHEDULE_TO_CLOSE timeout now returns RETRY_STATE_TIMEOUT instead of RETRY_STATE_NON_RETRYABLE_FAILURE
				s.Equal(enumspb.RETRY_STATE_TIMEOUT, activityError.RetryState())
				var timeoutError *temporal.TimeoutError
				s.ErrorAs(activityError, &timeoutError)
				s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutError.TimeoutType())
				s.Equal(int32(1), startedActivityCount.Load())
			})

			t.Run("TestActivityUpdateApi_ChangeScheduleToCloseAndRetry", func(t *testing.T) {
				// change both schedule to close and retry policy
				// initial values are chosen in such a way that activity will fail due to schedule to close timeout
				// we change schedule to close to a longer value and retry policy to a shorter value
				// after that activity should succeed
				s := testcore.NewEnv(t, testcore.WithSdkWorker())

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

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

				workflowFn := makeActivityUpdateWorkflowFunc(
					activityFunction, scheduleToCloseTimeout, retryInterval)

				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        activityUpdateWorkflowID,
					TaskQueue: s.WorkerTaskQueue(),
				}

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start (and fail)
				s.EventuallyWithT(func(t *assert.CollectT) {
					require.NotZero(t, startedActivityCount.Load())
				}, 2*time.Second, 200*time.Millisecond)

				// update schedule_to_close_timeout, make it longer
				// also update retry policy interval, make it shorter
				newScheduleToCloseTimeout := 10 * time.Second
				respOpts, err := api.update(ctx, s, workflowRun.GetID(), "activity-id",
					&activitypb.ActivityOptions{
						ScheduleToCloseTimeout: durationpb.New(newScheduleToCloseTimeout),
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval: durationpb.New(1 * time.Second),
						},
					},
					[]string{"schedule_to_close_timeout", "retry_policy.initial_interval"},
					false,
				)
				s.NoError(err)
				// check that the update was successful
				s.Equal(int64(newScheduleToCloseTimeout.Seconds()), respOpts.ScheduleToCloseTimeout.GetSeconds())
				// check that field we didn't update is the same
				s.Equal(int64(scheduleToCloseTimeout.Seconds()), respOpts.StartToCloseTimeout.GetSeconds())

				// now activity should succeed
				s.EventuallyWithT(func(t *assert.CollectT) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Empty(t, description.GetPendingActivities())
					require.Equal(t, int32(2), startedActivityCount.Load())
				}, 5*time.Second, 200*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)
				s.NoError(err)
			})

			t.Run("TestActivityUpdateApi_ResetDefaultOptions", func(t *testing.T) {
				// plan:
				// 1. start the workflow, wait for activity to start and fail,
				// 2. update activity options to change retry policy maximum attempts
				// 3. reset activity options to default, verify that retry policy is reset to default
				// 4. update activity options again, this time change schedule to close timeout and retry policy initial interval
				// 5. let activity finish, verify that it finished with updated options
				s := testcore.NewEnv(t, testcore.WithSdkWorker())

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

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
				workflowFn := makeActivityUpdateWorkflowFunc(activityFunction, scheduleToCloseTimeout, retryTimeout)

				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        activityUpdateWorkflowID,
					TaskQueue: s.WorkerTaskQueue(),
				}

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start (and fail)
				s.EventuallyWithT(func(t *assert.CollectT) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.GetPendingActivities(), 1)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 10*time.Second, 500*time.Millisecond)

				// update activity options, set retry policy to 1000 attempts
				_, err = api.update(ctx, s, workflowRun.GetID(), "activity-id",
					&activitypb.ActivityOptions{
						RetryPolicy: &commonpb.RetryPolicy{
							MaximumAttempts: 1000,
						},
					},
					[]string{"retry_policy.maximum_attempts"},
					false,
				)
				s.NoError(err)

				// check that the update was successful
				s.EventuallyWithT(func(t *assert.CollectT) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, int32(1000), description.PendingActivities[0].GetActivityOptions().GetRetryPolicy().GetMaximumAttempts())
				}, 3*time.Second, 200*time.Millisecond)

				// reset activity options to default
				_, err = api.update(ctx, s, workflowRun.GetID(), "activity-id",
					nil,
					[]string{},
					true,
				)
				s.NoError(err)

				// check that the reset was successful
				s.EventuallyWithT(func(t *assert.CollectT) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, int32(defaultMaximumAttempts), description.PendingActivities[0].GetActivityOptions().GetRetryPolicy().GetMaximumAttempts())
				}, 3*time.Second, 200*time.Millisecond)

				// update activity options again, this time set retry interval to 1 second
				newScheduleToCloseTimeout := 10 * time.Second
				_, err = api.update(ctx, s, workflowRun.GetID(), "activity-id",
					&activitypb.ActivityOptions{
						ScheduleToCloseTimeout: durationpb.New(newScheduleToCloseTimeout),
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval: durationpb.New(1 * time.Second),
						},
					},
					[]string{"schedule_to_close_timeout", "retry_policy.initial_interval"},
					false,
				)
				s.NoError(err)

				// let activity finish
				activityUpdated <- struct{}{}

				// wait for activity to finish
				s.EventuallyWithT(func(t *assert.CollectT) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Empty(t, description.GetPendingActivities())
					require.Equal(t, int32(2), startedActivityCount.Load())
				}, 3*time.Second, 100*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)

				s.NoError(err)
			})
		})
	}
}
