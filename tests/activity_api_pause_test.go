package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// activityPauseAPI groups pause/unpause adapters so the same test body can run
// against both the legacy PauseActivity/UnpauseActivity API and the newer
// PauseActivityExecution/UnpauseActivityExecution API.
type activityPauseAPI struct {
	name    string
	pause   func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity, reason, requestID string) error
	unpause func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity string, resetAttempts bool) error
}

func pauseAPIs() []activityPauseAPI {
	return []activityPauseAPI{
		{
			name: "legacy-api",
			pause: func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity, reason, requestID string) error {
				_, err := s.FrontendClient().PauseActivity(ctx, &workflowservice.PauseActivityRequest{
					Namespace: s.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{WorkflowId: wfID},
					Activity:  &workflowservice.PauseActivityRequest_Id{Id: actID},
					Identity:  identity,
					Reason:    reason,
					RequestId: requestID,
				})
				return err
			},
			unpause: func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity string, resetAttempts bool) error {
				_, err := s.FrontendClient().UnpauseActivity(ctx, &workflowservice.UnpauseActivityRequest{
					Namespace:     s.Namespace().String(),
					Execution:     &commonpb.WorkflowExecution{WorkflowId: wfID},
					Activity:      &workflowservice.UnpauseActivityRequest_Id{Id: actID},
					Identity:      identity,
					ResetAttempts: resetAttempts,
				})
				return err
			},
		},
		{
			name: "execution-api",
			pause: func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity, reason, requestID string) error {
				_, err := s.FrontendClient().PauseActivityExecution(ctx, &workflowservice.PauseActivityExecutionRequest{
					Namespace:  s.Namespace().String(),
					WorkflowId: wfID,
					ActivityId: actID,
					Identity:   identity,
					Reason:     reason,
					RequestId:  requestID,
				})
				return err
			},
			unpause: func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity string, resetAttempts bool) error {
				_, err := s.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
					Namespace:     s.Namespace().String(),
					WorkflowId:    wfID,
					ActivityId:    actID,
					Identity:      identity,
					ResetAttempts: resetAttempts,
				})
				return err
			},
		},
	}
}

func TestActivityApiPauseClientTestSuite(t *testing.T) {
	t.Parallel()

	for _, api := range pauseAPIs() {
		t.Run(api.name, func(t *testing.T) {
			t.Parallel()

			t.Run("TestActivityPauseApi_WhileRunning", func(t *testing.T) {
				s := testcore.NewEnv(t)

				initialRetryInterval := 1 * time.Second
				scheduleToCloseTimeout := 30 * time.Minute
				startToCloseTimeout := 15 * time.Minute
				activityRetryPolicy := &temporal.RetryPolicy{
					InitialInterval:    initialRetryInterval,
					BackoffCoefficient: 1,
				}
				makeWorkflowFunc := func(activityFunction ActivityFunctions) WorkflowFunction {
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

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				activityPausedCn := make(chan struct{})
				var startedActivityCount atomic.Int32
				activityErr := errors.New("bad-luck-please-retry")

				activityFunction := func() (string, error) {
					startedActivityCount.Add(1)
					if startedActivityCount.Load() == 1 {
						s.WaitForChannel(activityPausedCn)
						return "", activityErr
					}
					return "done!", nil
				}

				workflowFn := makeWorkflowFunc(activityFunction)

				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
					TaskQueue: s.WorkerTaskQueue(),
				}

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				require.NoError(t, err)

				// wait for activity to start
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// pause activity
				testIdentity := "test-identity"
				testReason := "test-reason"
				requestID := "test-request-id"
				require.NoError(t, api.pause(ctx, s, workflowRun.GetID(), "activity-id", testIdentity, testReason, requestID))

				// make sure activity is paused on server while running on worker
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, description.PendingActivities[0].State)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// unblock the activity
				activityPausedCn <- struct{}{}
				// make sure activity is paused on server and completed on the worker
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				require.NoError(t, err)
				s.Len(description.PendingActivities, 1)
				s.True(description.PendingActivities[0].Paused)

				// wait long enough for activity to retry if pause is not working
				// Note: because activity is retried we expect the attempts to be incremented
				err = util.InterruptibleSleep(ctx, 2*time.Second)
				require.NoError(t, err)

				// make sure activity is not completed, and was not retried
				description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				require.NoError(t, err)
				s.Len(description.PendingActivities, 1)
				s.True(description.PendingActivities[0].Paused)
				s.Equal(int32(2), description.PendingActivities[0].Attempt)
				s.NotNil(description.PendingActivities[0].LastFailure)
				s.Equal(activityErr.Error(), description.PendingActivities[0].LastFailure.Message)
				s.NotNil(description.PendingActivities[0].PauseInfo)
				s.NotNil(description.PendingActivities[0].PauseInfo.GetManual())
				s.Equal(testIdentity, description.PendingActivities[0].PauseInfo.GetManual().Identity)
				s.Equal(testReason, description.PendingActivities[0].PauseInfo.GetManual().Reason)

				// unpause the activity
				require.NoError(t, api.unpause(ctx, s, workflowRun.GetID(), "activity-id", "", false))

				var out string
				err = workflowRun.Get(ctx, &out)

				require.NoError(t, err)
			})

			t.Run("TestActivityPauseApi_IncreaseAttemptsOnFailure", func(t *testing.T) {
				/*
				 * 1. Run an activity that runs forever
				 * 2. Pause the activity
				 * 3. Send a failure signal to the activity
				 * 4. Validate activity failed
				 * 5. Validate number of activity attempts increased
				 */
				s := testcore.NewEnv(t)

				initialRetryInterval := 1 * time.Second
				scheduleToCloseTimeout := 30 * time.Minute
				startToCloseTimeout := 15 * time.Minute
				activityRetryPolicy := &temporal.RetryPolicy{
					InitialInterval:    initialRetryInterval,
					BackoffCoefficient: 1,
				}
				makeWorkflowFunc := func(activityFunction ActivityFunctions) WorkflowFunction {
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

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				var startedActivityCount atomic.Int32
				activityPausedCn := make(chan struct{})
				activityErr := errors.New("activity-failed-while-paused")
				var shouldSucceed atomic.Bool

				activityFunction := func() (string, error) {
					startedActivityCount.Add(1)
					if startedActivityCount.Load() == 1 {
						s.WaitForChannel(activityPausedCn)
						return "", activityErr
					}
					if shouldSucceed.Load() {
						return "done!", nil
					}
					return "", activityErr
				}

				workflowFn := makeWorkflowFunc(activityFunction)

				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
					TaskQueue: s.WorkerTaskQueue(),
				}

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				require.NoError(t, err)

				// wait for activity to start
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// pause activity
				testIdentity := "test-identity"
				testReason := "test-reason"
				testRequestID := "test-request-id"
				require.NoError(t, api.pause(ctx, s, workflowRun.GetID(), "activity-id", testIdentity, testReason, testRequestID))

				// make sure activity is paused on server while running on worker
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, description.PendingActivities[0].State)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// End the activity
				activityPausedCn <- struct{}{}

				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.NotNil(t, description)
					require.Len(t, description.PendingActivities, 1)
					require.True(t, description.PendingActivities[0].Paused)
					require.Equal(t, int32(2), description.PendingActivities[0].Attempt)
					require.NotNil(t, description.PendingActivities[0].LastFailure)
					require.NotNil(t, description.PendingActivities[0].PauseInfo)
					require.NotNil(t, description.PendingActivities[0].PauseInfo.GetManual())
					require.Equal(t, testIdentity, description.PendingActivities[0].PauseInfo.GetManual().Identity)
					require.Equal(t, testReason, description.PendingActivities[0].PauseInfo.GetManual().Reason)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// Let the workflow finish gracefully
				// set the flag to make activity succeed on next attempt
				shouldSucceed.Store(true)

				// unpause the activity
				require.NoError(t, api.unpause(ctx, s, workflowRun.GetID(), "activity-id", "", false))

				// wait for activity to complete
				await.Require(s.Context(), t, func(t *await.T) {
					require.Equal(t, int32(2), startedActivityCount.Load())
				}, 5*time.Second, 100*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)

				require.NoError(t, err)
			})

			t.Run("TestActivityPauseApi_WhileWaiting", func(t *testing.T) {
				// In this case, pause happens when activity is in retry state.
				// Make sure that activity is paused and then unpaused.
				// Also check that activity will not be retried while unpaused.
				s := testcore.NewEnv(t)

				initialRetryInterval := 1 * time.Second
				scheduleToCloseTimeout := 30 * time.Minute
				startToCloseTimeout := 15 * time.Minute
				activityRetryPolicy := &temporal.RetryPolicy{
					InitialInterval:    initialRetryInterval,
					BackoffCoefficient: 1,
				}
				makeWorkflowFunc := func(activityFunction ActivityFunctions) WorkflowFunction {
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

				workflowFn := makeWorkflowFunc(activityFunction)

				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
					TaskQueue: s.WorkerTaskQueue(),
				}

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				require.NoError(t, err)

				// wait for activity to start
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 100*time.Millisecond)

				// pause activity
				testIdentity := "test-identity"
				testReason := "test-reason"
				testRequestID := "test-request-id"
				require.NoError(t, api.pause(ctx, s, workflowRun.GetID(), "activity-id", testIdentity, testReason, testRequestID))

				// wait long enough for activity to retry if pause is not working
				require.NoError(t, util.InterruptibleSleep(ctx, 2*time.Second))

				// make sure activity is not completed, and was not retried
				description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				require.NoError(t, err)
				s.Len(description.PendingActivities, 1)
				s.True(description.PendingActivities[0].Paused)
				s.Equal(int32(2), description.PendingActivities[0].Attempt)
				s.NotNil(description.PendingActivities[0].PauseInfo)
				s.NotNil(description.PendingActivities[0].PauseInfo.GetManual())
				s.Equal(testIdentity, description.PendingActivities[0].PauseInfo.GetManual().Identity)
				s.Equal(testReason, description.PendingActivities[0].PauseInfo.GetManual().Reason)

				// unpause the activity
				require.NoError(t, api.unpause(ctx, s, workflowRun.GetID(), "activity-id", "", false))

				// wait for activity to complete
				await.Require(s.Context(), t, func(t *await.T) {
					require.Equal(t, int32(2), startedActivityCount.Load())
				}, 5*time.Second, 100*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)

				require.NoError(t, err)
			})

			t.Run("TestActivityPauseApi_WhileRetryNoWait", func(t *testing.T) {
				// In this case, pause can happen when activity is in retry state.
				// Make sure that activity is paused and then unpaused.
				// Also tests noWait flag.
				s := testcore.NewEnv(t)

				initialRetryInterval := 30 * time.Second
				scheduleToCloseTimeout := 30 * time.Minute
				startToCloseTimeout := 15 * time.Minute
				activityRetryPolicy := &temporal.RetryPolicy{
					InitialInterval:    initialRetryInterval,
					BackoffCoefficient: 1,
				}
				makeWorkflowFunc := func(activityFunction ActivityFunctions) WorkflowFunction {
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

				workflowFn := makeWorkflowFunc(activityFunction)

				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
					TaskQueue: s.WorkerTaskQueue(),
				}

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				require.NoError(t, err)

				// wait for activity to start
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.GetPendingActivities(), 1)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 100*time.Millisecond)

				// pause activity
				testRequestID := "test-request-id"
				require.NoError(t, api.pause(ctx, s, workflowRun.GetID(), "activity-id", "", "", testRequestID))

				// unpause the activity
				require.NoError(t, api.unpause(ctx, s, workflowRun.GetID(), "activity-id", "", false))

				// wait for activity to complete. It should happen immediately since noWait is set
				await.Require(s.Context(), t, func(t *await.T) {
					require.Equal(t, int32(2), startedActivityCount.Load())
				}, 2*time.Second, 100*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)

				require.NoError(t, err)
			})

			t.Run("TestActivityPauseApi_WithReset", func(t *testing.T) {
				// pause/unpause the activity with reset option and noWait flag
				s := testcore.NewEnv(t)

				initialRetryInterval := 1 * time.Second
				scheduleToCloseTimeout := 30 * time.Minute
				startToCloseTimeout := 15 * time.Minute
				activityRetryPolicy := &temporal.RetryPolicy{
					InitialInterval:    initialRetryInterval,
					BackoffCoefficient: 1,
				}
				makeWorkflowFunc := func(activityFunction ActivityFunctions) WorkflowFunction {
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

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				var startedActivityCount atomic.Int32
				activityWasReset := false
				activityCompleteCn := make(chan struct{})

				activityFunction := func() (string, error) {
					startedActivityCount.Add(1)

					if !activityWasReset {
						activityErr := errors.New("bad-luck-please-retry")
						return "", activityErr
					}
					s.WaitForChannel(activityCompleteCn)
					return "done!", nil
				}

				workflowFn := makeWorkflowFunc(activityFunction)

				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
					TaskQueue: s.WorkerTaskQueue(),
				}

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				require.NoError(t, err)

				// wait for activity to start/fail few times
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.GetPendingActivities(), 1)
					require.Greater(t, startedActivityCount.Load(), int32(1))
				}, 5*time.Second, 100*time.Millisecond)

				// pause activity
				testRequestID := "test-request-id"
				require.NoError(t, api.pause(ctx, s, workflowRun.GetID(), "activity-id", "", "", testRequestID))

				// wait for activity to be in paused state and waiting for retry
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.GetPendingActivities(), 1)
					require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
					// also verify that the number of attempts was not reset
					require.Greater(t, description.PendingActivities[0].Attempt, int32(1))
				}, 5*time.Second, 100*time.Millisecond)

				activityWasReset = true

				// unpause the activity with reset
				require.NoError(t, api.unpause(ctx, s, workflowRun.GetID(), "activity-id", "", true))

				// wait for activity to be running
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.GetPendingActivities(), 1)
					require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
					// also verify that the number of attempts was reset
					require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
				}, 5*time.Second, 100*time.Millisecond)

				// let activity finish
				activityCompleteCn <- struct{}{}

				// wait for workflow to finish
				var out string
				err = workflowRun.Get(ctx, &out)

				require.NoError(t, err)
			})

			t.Run("TestActivityPauseApi_WhilePaused", func(t *testing.T) {
				s := testcore.NewEnv(t)

				initialRetryInterval := 1 * time.Second
				scheduleToCloseTimeout := 30 * time.Minute
				startToCloseTimeout := 15 * time.Minute
				activityRetryPolicy := &temporal.RetryPolicy{
					InitialInterval:    initialRetryInterval,
					BackoffCoefficient: 1,
				}
				makeWorkflowFunc := func(activityFunction ActivityFunctions) WorkflowFunction {
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

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				activityPausedCn := make(chan struct{})
				var startedActivityCount atomic.Int32
				activityErr := errors.New("bad-luck-please-retry")

				activityFunction := func() (string, error) {
					startedActivityCount.Add(1)
					if startedActivityCount.Load() == 1 {
						s.WaitForChannel(activityPausedCn)
						return "", activityErr
					}
					return "done!", nil
				}

				workflowFn := makeWorkflowFunc(activityFunction)

				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
					TaskQueue: s.WorkerTaskQueue(),
				}

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				require.NoError(t, err)

				// wait for activity to start
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// pause activity
				testIdentity := "test-identity"
				testReason := "test-reason"
				testRequestID := "test-request-id"
				require.NoError(t, api.pause(ctx, s, workflowRun.GetID(), "activity-id", testIdentity, testReason, testRequestID))

				// make sure activity is paused on server while running on worker
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, description.PendingActivities[0].State)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// A second pause with a different request ID must return FailedPrecondition.
				// The first pause was issued without a request ID (stored as ""), so any
				// non-empty request ID here is guaranteed to differ.
				err = api.pause(ctx, s, workflowRun.GetID(), "activity-id", testIdentity, testReason, testRequestID+"-2")
				var failedPreconditionErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &failedPreconditionErr)

				// unblock the activity
				activityPausedCn <- struct{}{}
				// make sure activity is paused on server and completed on the worker
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
					require.Equal(t, int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				require.NoError(t, err)
				s.Len(description.PendingActivities, 1)
				s.True(description.PendingActivities[0].Paused)

				// wait long enough for activity to retry if pause is not working
				// Note: because activity is retried we expect the attempts to be incremented
				err = util.InterruptibleSleep(ctx, 2*time.Second)
				require.NoError(t, err)

				// make sure activity is not completed, and was not retried
				description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				require.NoError(t, err)
				s.Len(description.PendingActivities, 1)
				s.True(description.PendingActivities[0].Paused)
				s.Equal(int32(2), description.PendingActivities[0].Attempt)
				s.NotNil(description.PendingActivities[0].LastFailure)
				s.Equal(activityErr.Error(), description.PendingActivities[0].LastFailure.Message)
				s.NotNil(description.PendingActivities[0].PauseInfo)
				s.NotNil(description.PendingActivities[0].PauseInfo.GetManual())
				s.Equal(testIdentity, description.PendingActivities[0].PauseInfo.GetManual().Identity)
				s.Equal(testReason, description.PendingActivities[0].PauseInfo.GetManual().Reason)

				// unpause the activity
				require.NoError(t, api.unpause(ctx, s, workflowRun.GetID(), "activity-id", "", false))

				var out string
				err = workflowRun.Get(ctx, &out)

				require.NoError(t, err)
			})

			t.Run("TestActivityPauseApi_SameRequestID_IsIdempotent", func(t *testing.T) {
				// Pausing an already-paused activity with the same request ID must succeed (no-op).
				s := testcore.NewEnv(t)

				scheduleToCloseTimeout := 30 * time.Minute
				startToCloseTimeout := 15 * time.Minute
				activityRetryPolicy := &temporal.RetryPolicy{
					InitialInterval:    30 * time.Second,
					BackoffCoefficient: 1,
				}
				makeWorkflowFunc := func(activityFunction ActivityFunctions) WorkflowFunction {
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

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				activityFunction := ActivityFunctions(func() (string, error) {
					return "", errors.New("fail-to-trigger-retry")
				})
				workflowFn := makeWorkflowFunc(activityFunction)
				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
					TaskQueue: s.WorkerTaskQueue(),
				}, workflowFn)
				require.NoError(t, err)

				// Wait for the first attempt to fail and the activity to enter retry backoff (attempt 2).
				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, int32(2), description.PendingActivities[0].Attempt)
				}, 5*time.Second, 100*time.Millisecond)

				// First pause with an explicit request ID.
				require.NoError(t, api.pause(ctx, s, workflowRun.GetID(), "activity-id", "identity", "reason", "my-pause-request-id"))

				await.Require(s.Context(), t, func(t *await.T) {
					description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(t, err)
					require.Len(t, description.PendingActivities, 1)
					require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
				}, 5*time.Second, 100*time.Millisecond)

				// Second pause with the same request ID — must succeed (idempotent no-op).
				require.NoError(t, api.pause(ctx, s, workflowRun.GetID(), "activity-id", "identity", "reason", "my-pause-request-id"))
			})

			t.Run("TestActivityPauseUpdateOptionsResetUnpause", func(t *testing.T) {
				// End-to-end test: pause → update-options → reset → unpause all work together.
				// Verifies that the updated options persist through a reset and that the activity
				// completes at attempt 1 with the new options after unpause.
				s := testcore.NewEnv(t)

				initialRetryInterval := 1 * time.Minute
				origScheduleToClose := 30 * time.Minute
				updatedScheduleToClose := 25 * time.Minute
				activityRetryPolicy := &temporal.RetryPolicy{
					InitialInterval:    initialRetryInterval,
					BackoffCoefficient: 1,
				}

				makeWorkflowFunc := func(activityFunction ActivityFunctions) WorkflowFunction {
					return func(ctx workflow.Context) error {
						var ret string
						return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
							ActivityID:             "activity-id",
							DisableEagerExecution:  true,
							StartToCloseTimeout:    15 * time.Minute,
							ScheduleToCloseTimeout: origScheduleToClose,
							RetryPolicy:            activityRetryPolicy,
						}), activityFunction).Get(ctx, &ret)
					}
				}

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				var activityWasReset atomic.Bool
				activityCompleteCh := make(chan struct{})

				activityFunction := func() (string, error) {
					if !activityWasReset.Load() {
						return "", errors.New("bad-luck-please-retry")
					}
					s.WaitForChannel(activityCompleteCh)
					return "done!", nil
				}

				workflowFn := makeWorkflowFunc(activityFunction)
				s.SdkWorker().RegisterWorkflow(workflowFn)
				s.SdkWorker().RegisterActivity(activityFunction)

				wfID := testcore.RandomizeStr("wf_id-" + t.Name())
				workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
					ID:        wfID,
					TaskQueue: s.WorkerTaskQueue(),
				}, workflowFn)
				require.NoError(t, err)

				// wait for activity to fail and enter retry backoff
				await.Require(s.Context(), t, func(c *await.T) {
					desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(c, err)
					require.Len(c, desc.PendingActivities, 1)
					require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.PendingActivities[0].State)
					require.Greater(c, desc.PendingActivities[0].Attempt, int32(1))
				}, 5*time.Second, 200*time.Millisecond)

				// step 1: pause
				require.NoError(t, api.pause(ctx, s, wfID, "activity-id", "", "", ""))

				await.Require(s.Context(), t, func(c *await.T) {
					desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(c, err)
					require.Len(c, desc.PendingActivities, 1)
					require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, desc.PendingActivities[0].State)
				}, 5*time.Second, 100*time.Millisecond)

				// step 2: update-options (reduce schedule-to-close timeout while paused)
				_, err = s.FrontendClient().UpdateActivityOptions(ctx, &workflowservice.UpdateActivityOptionsRequest{
					Namespace: s.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{WorkflowId: wfID},
					Activity:  &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity-id"},
					ActivityOptions: &activitypb.ActivityOptions{
						ScheduleToCloseTimeout: durationpb.New(updatedScheduleToClose),
					},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout"}},
				})
				require.NoError(t, err)

				await.Require(s.Context(), t, func(c *await.T) {
					desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(c, err)
					require.Len(c, desc.PendingActivities, 1)
					require.Equal(c, updatedScheduleToClose, desc.PendingActivities[0].ActivityOptions.GetScheduleToCloseTimeout().AsDuration())
				}, 5*time.Second, 100*time.Millisecond)

				// step 3: reset while paused — stays PAUSED (keepPaused=true), attempt resets to 1
				_, err = s.FrontendClient().ResetActivity(ctx, &workflowservice.ResetActivityRequest{
					Namespace:  s.Namespace().String(),
					Execution:  &commonpb.WorkflowExecution{WorkflowId: wfID},
					Activity:   &workflowservice.ResetActivityRequest_Id{Id: "activity-id"},
					KeepPaused: true,
				})
				require.NoError(t, err)

				await.Require(s.Context(), t, func(c *await.T) {
					desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(c, err)
					require.Len(c, desc.PendingActivities, 1)
					require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_PAUSED, desc.PendingActivities[0].State)
					require.Equal(c, int32(1), desc.PendingActivities[0].Attempt)
					// updated options must survive the reset
					require.Equal(c, updatedScheduleToClose, desc.PendingActivities[0].ActivityOptions.GetScheduleToCloseTimeout().AsDuration())
				}, 5*time.Second, 100*time.Millisecond)

				// step 4: unpause
				activityWasReset.Store(true)
				require.NoError(t, api.unpause(ctx, s, wfID, "activity-id", "", false))

				await.Require(s.Context(), t, func(c *await.T) {
					desc, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					require.NoError(c, err)
					require.Len(c, desc.PendingActivities, 1)
					require.Equal(c, enumspb.PENDING_ACTIVITY_STATE_STARTED, desc.PendingActivities[0].State)
					require.Equal(c, int32(1), desc.PendingActivities[0].Attempt)
				}, 5*time.Second, 100*time.Millisecond)

				activityCompleteCh <- struct{}{}

				var out string
				err = workflowRun.Get(ctx, &out)
				require.NoError(t, err)
			})
		})
	}
}
