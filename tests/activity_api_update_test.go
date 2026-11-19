package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
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

type ActivityAPIUpdateSuite struct {
	parallelsuite.Suite[*ActivityAPIUpdateSuite]
}

func TestActivityAPIUpdateSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityAPIUpdateSuite{})
}

func (s *ActivityAPIUpdateSuite) TestClient() {
	for _, api := range updateAPIs() {
		s.Run(api.name, func(s *ActivityAPIUpdateSuite) {

			s.Run("ChangeRetryInterval", func(s *ActivityAPIUpdateSuite) {
				env := testcore.NewEnv(s.T())

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

					s.Rcv(activityUpdated)
					return "done!", nil
				}

				scheduleToCloseTimeout := 30 * time.Minute
				retryTimeout := 10 * time.Minute
				workflowFn := makeActivityUpdateWorkflowFunc(activityFunction, scheduleToCloseTimeout, retryTimeout)

				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        activityUpdateWorkflowID,
					TaskQueue: env.WorkerTaskQueue(),
				}

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				s.Await(func(s *ActivityAPIUpdateSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.GetPendingActivities(), 1)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 10*time.Second, 500*time.Millisecond)

				_, err = api.update(ctx, env, workflowRun.GetID(), "activity-id",
					&activitypb.ActivityOptions{
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval: durationpb.New(1 * time.Second),
						},
					},
					[]string{"retry_policy.initial_interval"},
					false,
				)
				s.NoError(err)

				description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				s.NoError(err)
				s.Len(description.PendingActivities, 1)

				select {
				case activityUpdated <- struct{}{}:
				case <-ctx.Done():
					s.FailNow("timed out waiting for activity to receive update signal")
				}

				s.Await(func(s *ActivityAPIUpdateSuite) {
					description, err = env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Empty(description.GetPendingActivities())
					s.Equal(int32(2), startedActivityCount.Load())
				}, 3*time.Second, 100*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)

				s.NoError(err)
			})

			s.Run("ChangeScheduleToClose", func(s *ActivityAPIUpdateSuite) {
				env := testcore.NewEnv(s.T())

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

				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        activityUpdateWorkflowID,
					TaskQueue: env.WorkerTaskQueue(),
				}

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start (and fail)
				s.Await(func(s *ActivityAPIUpdateSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.GetPendingActivities(), 1)
					s.Equal(int32(1), startedActivityCount.Load())

				}, 2*time.Second, 200*time.Millisecond)

				// update schedule_to_close_timeout
				_, err = api.update(ctx, env, workflowRun.GetID(), "activity-id",
					&activitypb.ActivityOptions{
						ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
					},
					[]string{"schedule_to_close_timeout"},
					false,
				)
				s.NoError(err)

				// activity should fail immediately
				s.Await(func(s *ActivityAPIUpdateSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Empty(description.GetPendingActivities())
					s.Equal(int32(1), startedActivityCount.Load())
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

			s.Run("ChangeScheduleToCloseAndRetry", func(s *ActivityAPIUpdateSuite) {
				// change both schedule to close and retry policy
				// initial values are chosen in such a way that activity will fail due to schedule to close timeout
				// we change schedule to close to a longer value and retry policy to a shorter value
				// after that activity should succeed
				env := testcore.NewEnv(s.T())

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

				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        activityUpdateWorkflowID,
					TaskQueue: env.WorkerTaskQueue(),
				}

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start (and fail)
				s.Await(func(s *ActivityAPIUpdateSuite) {
					s.NotZero(startedActivityCount.Load())
				}, 2*time.Second, 200*time.Millisecond)

				// update schedule_to_close_timeout, make it longer
				// also update retry policy interval, make it shorter
				newScheduleToCloseTimeout := 10 * time.Second
				respOpts, err := api.update(ctx, env, workflowRun.GetID(), "activity-id",
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
				s.Await(func(s *ActivityAPIUpdateSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Empty(description.GetPendingActivities())
					s.Equal(int32(2), startedActivityCount.Load())
				}, 5*time.Second, 200*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)
				s.NoError(err)
			})

			s.Run("ResetDefaultOptions", func(s *ActivityAPIUpdateSuite) {
				// plan:
				// 1. start the workflow, wait for activity to start and fail,
				// 2. update activity options to change retry policy maximum attempts
				// 3. reset activity options to default, verify that retry policy is reset to default
				// 4. update activity options again, this time change schedule to close timeout and retry policy initial interval
				// 5. let activity finish, verify that it finished with updated options
				env := testcore.NewEnv(s.T())

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

					s.Rcv(activityUpdated)
					return "done!", nil
				}

				scheduleToCloseTimeout := 30 * time.Minute
				retryTimeout := 10 * time.Minute
				workflowFn := makeActivityUpdateWorkflowFunc(activityFunction, scheduleToCloseTimeout, retryTimeout)

				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        activityUpdateWorkflowID,
					TaskQueue: env.WorkerTaskQueue(),
				}

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start (and fail)
				s.Await(func(s *ActivityAPIUpdateSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.GetPendingActivities(), 1)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 10*time.Second, 500*time.Millisecond)

				// update activity options, set retry policy to 1000 attempts
				_, err = api.update(ctx, env, workflowRun.GetID(), "activity-id",
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
				s.Await(func(s *ActivityAPIUpdateSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(int32(1000), description.PendingActivities[0].GetActivityOptions().GetRetryPolicy().GetMaximumAttempts())
				}, 3*time.Second, 200*time.Millisecond)

				// reset activity options to default
				_, err = api.update(ctx, env, workflowRun.GetID(), "activity-id",
					nil,
					[]string{},
					true,
				)
				s.NoError(err)

				// check that the reset was successful
				s.Await(func(s *ActivityAPIUpdateSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(int32(defaultMaximumAttempts), description.PendingActivities[0].GetActivityOptions().GetRetryPolicy().GetMaximumAttempts())
				}, 3*time.Second, 200*time.Millisecond)

				// update activity options again, this time set retry interval to 1 second
				newScheduleToCloseTimeout := 10 * time.Second
				_, err = api.update(ctx, env, workflowRun.GetID(), "activity-id",
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
				select {
				case activityUpdated <- struct{}{}:
				case <-ctx.Done():
					s.FailNow("timed out waiting for activity to receive update signal")
				}

				// wait for activity to finish
				s.Await(func(s *ActivityAPIUpdateSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Empty(description.GetPendingActivities())
					s.Equal(int32(2), startedActivityCount.Load())
				}, 3*time.Second, 100*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)

				s.NoError(err)
			})
		})
	}
}

// TestExecutionOptions tests the new UpdateActivityExecutionOptions RPC
// on workflow activities (workflow_id != ""), verifying it behaves identically to the
// existing UpdateActivityOptions RPC.
func (s *ActivityAPIUpdateSuite) TestExecutionOptions() {
	s.Run("ChangeRetryInterval", func(s *ActivityAPIUpdateSuite) {
		env := testcore.NewEnv(s.T())

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityUpdated := make(chan struct{})

		var startedActivityCount atomic.Int32
		activityFunction := func() (string, error) {
			startedActivityCount.Add(1)
			if startedActivityCount.Load() == 1 {
				return "", errors.New("bad-luck-please-retry")
			}
			s.Rcv(activityUpdated)
			return "done!", nil
		}

		workflowFn := makeActivityUpdateWorkflowFunc(activityFunction, 30*time.Minute, 10*time.Minute)
		env.SdkWorker().RegisterWorkflow(workflowFn)
		env.SdkWorker().RegisterActivity(activityFunction)

		workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        activityUpdateWorkflowID,
			TaskQueue: env.WorkerTaskQueue(),
		}, workflowFn)
		s.NoError(err)

		s.Await(func(s *ActivityAPIUpdateSuite) {
			description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(err)
			s.Len(description.GetPendingActivities(), 1)
			s.Equal(int32(1), startedActivityCount.Load())
		}, 10*time.Second, 500*time.Millisecond)

		resp, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
			ActivityId: "activity-id",
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Second),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}},
		})
		s.NoError(err)
		s.NotNil(resp)

		select {
		case activityUpdated <- struct{}{}:
		case <-ctx.Done():
			s.FailNow("timed out waiting for activity to receive update signal")
		}

		s.Await(func(s *ActivityAPIUpdateSuite) {
			description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(err)
			s.Empty(description.GetPendingActivities())
			s.Equal(int32(2), startedActivityCount.Load())
		}, 3*time.Second, 100*time.Millisecond)

		var out string
		err = workflowRun.Get(ctx, &out)
		s.NoError(err)
	})

	s.Run("ChangeScheduleToClose", func(s *ActivityAPIUpdateSuite) {
		env := testcore.NewEnv(s.T())

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var startedActivityCount atomic.Int32
		activityFunction := func() (string, error) {
			startedActivityCount.Add(1)
			if startedActivityCount.Load() == 1 {
				return "", errors.New("bad-luck-please-retry")
			}
			return "done!", nil
		}

		workflowFn := makeActivityUpdateWorkflowFunc(activityFunction, 30*time.Minute, 10*time.Minute)
		env.SdkWorker().RegisterWorkflow(workflowFn)
		env.SdkWorker().RegisterActivity(activityFunction)

		workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        activityUpdateWorkflowID,
			TaskQueue: env.WorkerTaskQueue(),
		}, workflowFn)
		s.NoError(err)

		s.Await(func(s *ActivityAPIUpdateSuite) {
			description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(err)
			s.Len(description.GetPendingActivities(), 1)
			s.Equal(int32(1), startedActivityCount.Load())
		}, 2*time.Second, 200*time.Millisecond)

		resp, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
			ActivityId: "activity-id",
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout"}},
		})
		s.NoError(err)
		s.NotNil(resp)

		s.Await(func(s *ActivityAPIUpdateSuite) {
			description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(err)
			s.Empty(description.GetPendingActivities())
			s.Equal(int32(1), startedActivityCount.Load())
		}, 2*time.Second, 200*time.Millisecond)

		var out string
		err = workflowRun.Get(ctx, &out)
		var activityError *temporal.ActivityError
		s.ErrorAs(err, &activityError)
		s.Equal(enumspb.RETRY_STATE_TIMEOUT, activityError.RetryState())
		var timeoutError *temporal.TimeoutError
		s.ErrorAs(activityError, &timeoutError)
		s.Equal(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, timeoutError.TimeoutType())
		s.Equal(int32(1), startedActivityCount.Load())
	})

	s.Run("ChangeScheduleToCloseAndRetry", func(s *ActivityAPIUpdateSuite) {
		env := testcore.NewEnv(s.T())

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var startedActivityCount atomic.Int32
		activityFunction := func() (string, error) {
			startedActivityCount.Add(1)
			if startedActivityCount.Load() == 1 {
				return "", errors.New("bad-luck-please-retry")
			}
			return "done!", nil
		}

		scheduleToCloseTimeout := 8 * time.Second
		workflowFn := makeActivityUpdateWorkflowFunc(activityFunction, scheduleToCloseTimeout, 5*time.Second)
		env.SdkWorker().RegisterWorkflow(workflowFn)
		env.SdkWorker().RegisterActivity(activityFunction)

		workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        activityUpdateWorkflowID,
			TaskQueue: env.WorkerTaskQueue(),
		}, workflowFn)
		s.NoError(err)

		s.Await(func(s *ActivityAPIUpdateSuite) {
			s.NotZero(startedActivityCount.Load())
		}, 2*time.Second, 200*time.Millisecond)

		newScheduleToCloseTimeout := 10 * time.Second
		resp, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
			ActivityId: "activity-id",
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToCloseTimeout: durationpb.New(newScheduleToCloseTimeout),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Second),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout", "retry_policy.initial_interval"}},
		})
		s.NoError(err)
		s.NotNil(resp)
		s.Equal(int64(newScheduleToCloseTimeout.Seconds()), resp.GetActivityOptions().ScheduleToCloseTimeout.GetSeconds())
		s.Equal(int64(scheduleToCloseTimeout.Seconds()), resp.GetActivityOptions().StartToCloseTimeout.GetSeconds())

		s.Await(func(s *ActivityAPIUpdateSuite) {
			description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(err)
			s.Empty(description.GetPendingActivities())
			s.Equal(int32(2), startedActivityCount.Load())
		}, 5*time.Second, 200*time.Millisecond)

		var out string
		err = workflowRun.Get(ctx, &out)
		s.NoError(err)
	})

	s.Run("ResetDefaultOptions", func(s *ActivityAPIUpdateSuite) {
		env := testcore.NewEnv(s.T())

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		activityUpdated := make(chan struct{})

		var startedActivityCount atomic.Int32
		activityFunction := func() (string, error) {
			startedActivityCount.Add(1)
			if startedActivityCount.Load() == 1 {
				return "", errors.New("bad-luck-please-retry")
			}
			s.Rcv(activityUpdated)
			return "done!", nil
		}

		workflowFn := makeActivityUpdateWorkflowFunc(activityFunction, 30*time.Minute, 10*time.Minute)
		env.SdkWorker().RegisterWorkflow(workflowFn)
		env.SdkWorker().RegisterActivity(activityFunction)

		workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        activityUpdateWorkflowID,
			TaskQueue: env.WorkerTaskQueue(),
		}, workflowFn)
		s.NoError(err)

		s.Await(func(s *ActivityAPIUpdateSuite) {
			description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(err)
			s.Len(description.GetPendingActivities(), 1)
			s.Equal(int32(1), startedActivityCount.Load())
		}, 10*time.Second, 500*time.Millisecond)

		// Update max attempts to 1000.
		resp, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
			ActivityId: "activity-id",
			ActivityOptions: &activitypb.ActivityOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumAttempts: 1000,
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_attempts"}},
		})
		s.NoError(err)
		s.NotNil(resp)

		s.Await(func(s *ActivityAPIUpdateSuite) {
			description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(err)
			s.Len(description.PendingActivities, 1)
			s.Equal(int32(1000), description.PendingActivities[0].GetActivityOptions().GetRetryPolicy().GetMaximumAttempts())
		}, 3*time.Second, 200*time.Millisecond)

		// Reset to original options.
		resp, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:       env.Namespace().String(),
			WorkflowId:      workflowRun.GetID(),
			RunId:           workflowRun.GetRunID(),
			ActivityId:      "activity-id",
			RestoreOriginal: true,
		})
		s.NoError(err)
		s.NotNil(resp)

		s.Await(func(s *ActivityAPIUpdateSuite) {
			description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(err)
			s.Len(description.PendingActivities, 1)
			s.Equal(int32(defaultMaximumAttempts), description.PendingActivities[0].GetActivityOptions().GetRetryPolicy().GetMaximumAttempts())
		}, 3*time.Second, 200*time.Millisecond)

		// Update retry interval to unblock the second attempt.
		resp, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
			Namespace:  env.Namespace().String(),
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
			ActivityId: "activity-id",
			ActivityOptions: &activitypb.ActivityOptions{
				ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(1 * time.Second),
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout", "retry_policy.initial_interval"}},
		})
		s.NoError(err)
		s.NotNil(resp)

		select {
		case activityUpdated <- struct{}{}:
		case <-ctx.Done():
			s.FailNow("timed out waiting for activity to receive update signal")
		}

		s.Await(func(s *ActivityAPIUpdateSuite) {
			description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(err)
			s.Empty(description.GetPendingActivities())
			s.Equal(int32(2), startedActivityCount.Load())
		}, 3*time.Second, 100*time.Millisecond)

		var out string
		err = workflowRun.Get(ctx, &out)
		s.NoError(err)
	})
}
