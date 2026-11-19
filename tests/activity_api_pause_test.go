package tests

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	contextpropagationspb "go.temporal.io/server/api/contextpropagation/v1"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// activityPauseAPI groups pause/unpause adapters so the same test body can run
// against both the legacy PauseActivity/UnpauseActivity API and the newer
// PauseActivityExecution/UnpauseActivityExecution API.
type activityPauseAPI struct {
	name    string
	pause   func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity, reason, requestID string) error
	unpause func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity string) error
	// unpauseResettingAttempts is nil on an API with no reset_attempts flag. Only the deprecated
	// UnpauseActivity has one; UnpauseActivityExecution deliberately does not.
	unpauseResettingAttempts func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity string) error
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
			unpause: func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity string) error {
				_, err := s.FrontendClient().UnpauseActivity(ctx, &workflowservice.UnpauseActivityRequest{
					Namespace: s.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{WorkflowId: wfID},
					Activity:  &workflowservice.UnpauseActivityRequest_Id{Id: actID},
					Identity:  identity,
				})
				return err
			},
			unpauseResettingAttempts: func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity string) error {
				_, err := s.FrontendClient().UnpauseActivity(ctx, &workflowservice.UnpauseActivityRequest{
					Namespace:     s.Namespace().String(),
					Execution:     &commonpb.WorkflowExecution{WorkflowId: wfID},
					Activity:      &workflowservice.UnpauseActivityRequest_Id{Id: actID},
					Identity:      identity,
					ResetAttempts: true,
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
			unpause: func(ctx context.Context, s *testcore.TestEnv, wfID, actID, identity string) error {
				_, err := s.FrontendClient().UnpauseActivityExecution(ctx, &workflowservice.UnpauseActivityExecutionRequest{
					Namespace:  s.Namespace().String(),
					WorkflowId: wfID,
					ActivityId: actID,
					Identity:   identity,
				})
				return err
			},
		},
	}
}

type ActivityAPIPauseSuite struct {
	parallelsuite.Suite[*ActivityAPIPauseSuite]
}

func TestActivityAPIPauseSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityAPIPauseSuite{})
}

func (s *ActivityAPIPauseSuite) TestClient() {
	for _, api := range pauseAPIs() {
		s.Run(api.name, func(s *ActivityAPIPauseSuite) {

			s.Run("AfterWorkflowCompleted", func(s *ActivityAPIPauseSuite) {
				env := testcore.NewEnv(s.T())
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				workflowFn := func(workflow.Context) error { return nil }
				env.SdkWorker().RegisterWorkflow(workflowFn)

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
					TaskQueue: env.WorkerTaskQueue(),
				}, workflowFn)
				s.NoError(err)
				s.NoError(workflowRun.Get(ctx, nil))

				err = api.pause(ctx, env, workflowRun.GetID(), "activity-id", "test-identity", "test-reason", "test-request-id")
				s.ErrorContains(err, "workflow execution already completed")
			})

			s.Run("WhileRunning", func(s *ActivityAPIPauseSuite) {
				env := testcore.NewEnv(s.T())

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
						s.Rcv(activityPausedCn)
						return "", activityErr
					}
					return "done!", nil
				}

				workflowFn := makeWorkflowFunc(activityFunction)

				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
					TaskQueue: env.WorkerTaskQueue(),
				}

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// pause activity
				testIdentity := "test-identity"
				testReason := "test-reason"
				requestID := "test-request-id"
				s.NoError(api.pause(ctx, env, workflowRun.GetID(), "activity-id", testIdentity, testReason, requestID))

				// make sure activity is paused on server while running on worker
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, description.PendingActivities[0].State)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// unblock the activity
				activityPausedCn <- struct{}{}
				// make sure activity is paused on server and completed on the worker
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				s.NoError(err)
				s.Len(description.PendingActivities, 1)
				s.True(description.PendingActivities[0].Paused)

				// wait long enough for activity to retry if pause is not working
				// Note: because activity is retried we expect the attempts to be incremented
				err = util.InterruptibleSleep(ctx, 2*time.Second)
				s.NoError(err)

				// make sure activity is not completed, and was not retried
				description, err = env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				s.NoError(err)
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
				s.NoError(api.unpause(ctx, env, workflowRun.GetID(), "activity-id", ""))

				var out string
				err = workflowRun.Get(ctx, &out)

				s.NoError(err)
			})

			s.Run("IncreaseAttemptsOnFailure", func(s *ActivityAPIPauseSuite) {
				/*
				 * 1. Run an activity that runs forever
				 * 2. Pause the activity
				 * 3. Send a failure signal to the activity
				 * 4. Validate activity failed
				 * 5. Validate number of activity attempts increased
				 */
				env := testcore.NewEnv(s.T())

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
						s.Rcv(activityPausedCn)
						return "", activityErr
					}
					if shouldSucceed.Load() {
						return "done!", nil
					}
					return "", activityErr
				}

				workflowFn := makeWorkflowFunc(activityFunction)

				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
					TaskQueue: env.WorkerTaskQueue(),
				}

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// pause activity
				testIdentity := "test-identity"
				testReason := "test-reason"
				testRequestID := "test-request-id"
				s.NoError(api.pause(ctx, env, workflowRun.GetID(), "activity-id", testIdentity, testReason, testRequestID))

				// make sure activity is paused on server while running on worker
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, description.PendingActivities[0].State)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// End the activity
				activityPausedCn <- struct{}{}

				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.NotNil(description)
					s.Len(description.PendingActivities, 1)
					s.True(description.PendingActivities[0].Paused)
					s.Equal(int32(2), description.PendingActivities[0].Attempt)
					s.NotNil(description.PendingActivities[0].LastFailure)
					s.NotNil(description.PendingActivities[0].PauseInfo)
					s.NotNil(description.PendingActivities[0].PauseInfo.GetManual())
					s.Equal(testIdentity, description.PendingActivities[0].PauseInfo.GetManual().Identity)
					s.Equal(testReason, description.PendingActivities[0].PauseInfo.GetManual().Reason)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// Let the workflow finish gracefully
				// set the flag to make activity succeed on next attempt
				shouldSucceed.Store(true)

				// unpause the activity
				s.NoError(api.unpause(ctx, env, workflowRun.GetID(), "activity-id", ""))

				// wait for activity to complete
				s.Await(func(s *ActivityAPIPauseSuite) {
					s.Equal(int32(2), startedActivityCount.Load())
				}, 5*time.Second, 100*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)

				s.NoError(err)
			})

			s.Run("WhileWaiting", func(s *ActivityAPIPauseSuite) {
				// In this case, pause happens when activity is in retry state.
				// Make sure that activity is paused and then unpaused.
				// Also check that activity will not be retried while unpaused.
				env := testcore.NewEnv(s.T())

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

				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
					TaskQueue: env.WorkerTaskQueue(),
				}

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 100*time.Millisecond)

				// pause activity
				testIdentity := "test-identity"
				testReason := "test-reason"
				testRequestID := "test-request-id"
				s.NoError(api.pause(ctx, env, workflowRun.GetID(), "activity-id", testIdentity, testReason, testRequestID))

				// wait long enough for activity to retry if pause is not working
				s.NoError(util.InterruptibleSleep(ctx, 2*time.Second))

				// make sure activity is not completed, and was not retried
				description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				s.NoError(err)
				s.Len(description.PendingActivities, 1)
				s.True(description.PendingActivities[0].Paused)
				s.Equal(int32(2), description.PendingActivities[0].Attempt)
				s.NotNil(description.PendingActivities[0].PauseInfo)
				s.NotNil(description.PendingActivities[0].PauseInfo.GetManual())
				s.Equal(testIdentity, description.PendingActivities[0].PauseInfo.GetManual().Identity)
				s.Equal(testReason, description.PendingActivities[0].PauseInfo.GetManual().Reason)

				// unpause the activity
				s.NoError(api.unpause(ctx, env, workflowRun.GetID(), "activity-id", ""))

				// wait for activity to complete
				s.Await(func(s *ActivityAPIPauseSuite) {
					s.Equal(int32(2), startedActivityCount.Load())
				}, 5*time.Second, 100*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)

				s.NoError(err)
			})

			s.Run("WhileRetryNoWait", func(s *ActivityAPIPauseSuite) {
				// In this case, pause can happen when activity is in retry state.
				// Make sure that activity is paused and then unpaused.
				// Also tests noWait flag.
				env := testcore.NewEnv(s.T())

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

				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
					TaskQueue: env.WorkerTaskQueue(),
				}

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.GetPendingActivities(), 1)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 100*time.Millisecond)

				// pause activity
				testRequestID := "test-request-id"
				s.NoError(api.pause(ctx, env, workflowRun.GetID(), "activity-id", "", "", testRequestID))

				// unpause the activity
				s.NoError(api.unpause(ctx, env, workflowRun.GetID(), "activity-id", ""))

				// wait for activity to complete. It should happen immediately since noWait is set
				s.Await(func(s *ActivityAPIPauseSuite) {
					s.Equal(int32(2), startedActivityCount.Load())
				}, 2*time.Second, 100*time.Millisecond)

				var out string
				err = workflowRun.Get(ctx, &out)

				s.NoError(err)
			})

			s.Run("WithReset", func(s *ActivityAPIPauseSuite) {
				// pause/unpause the activity with reset option and noWait flag
				if api.unpauseResettingAttempts == nil {
					s.T().Skip("this API has no reset_attempts flag on unpause; Reset is the operation that restarts attempts")
				}
				env := testcore.NewEnv(s.T())

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
					s.Rcv(activityCompleteCn)
					return "done!", nil
				}

				workflowFn := makeWorkflowFunc(activityFunction)

				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
					TaskQueue: env.WorkerTaskQueue(),
				}

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start/fail few times
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.GetPendingActivities(), 1)
					s.Greater(startedActivityCount.Load(), int32(1))
				}, 5*time.Second, 100*time.Millisecond)

				// pause activity
				testRequestID := "test-request-id"
				s.NoError(api.pause(ctx, env, workflowRun.GetID(), "activity-id", "", "", testRequestID))

				// wait for activity to be in paused state and waiting for retry
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.GetPendingActivities(), 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
					// also verify that the number of attempts was not reset
					s.Greater(description.PendingActivities[0].Attempt, int32(1))
				}, 5*time.Second, 100*time.Millisecond)

				activityWasReset = true

				// unpause the activity with reset
				s.NoError(api.unpauseResettingAttempts(ctx, env, workflowRun.GetID(), "activity-id", ""))

				// wait for activity to be running
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.GetPendingActivities(), 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_STARTED, description.PendingActivities[0].State)
					// also verify that the number of attempts was reset
					s.Equal(int32(1), description.PendingActivities[0].Attempt)
				}, 5*time.Second, 100*time.Millisecond)

				// let activity finish
				activityCompleteCn <- struct{}{}

				// wait for workflow to finish
				var out string
				err = workflowRun.Get(ctx, &out)

				s.NoError(err)
			})

			s.Run("WhilePaused", func(s *ActivityAPIPauseSuite) {
				env := testcore.NewEnv(s.T())

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
						s.Rcv(activityPausedCn)
						return "", activityErr
					}
					return "done!", nil
				}

				workflowFn := makeWorkflowFunc(activityFunction)

				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowOptions := sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
					TaskQueue: env.WorkerTaskQueue(),
				}

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
				s.NoError(err)

				// wait for activity to start
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// pause activity
				testIdentity := "test-identity"
				testReason := "test-reason"
				testRequestID := "test-request-id"
				s.NoError(api.pause(ctx, env, workflowRun.GetID(), "activity-id", testIdentity, testReason, testRequestID))

				// make sure activity is paused on server while running on worker
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, description.PendingActivities[0].State)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				// A second pause with a different request ID must return FailedPrecondition.
				// The first pause was issued without a request ID (stored as ""), so any
				// non-empty request ID here is guaranteed to differ.
				err = api.pause(ctx, env, workflowRun.GetID(), "activity-id", testIdentity, testReason, testRequestID+"-2")
				var failedPreconditionErr *serviceerror.FailedPrecondition
				s.ErrorAs(err, &failedPreconditionErr)

				// unblock the activity
				activityPausedCn <- struct{}{}
				// make sure activity is paused on server and completed on the worker
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
					s.Equal(int32(1), startedActivityCount.Load())
				}, 5*time.Second, 500*time.Millisecond)

				description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				s.NoError(err)
				s.Len(description.PendingActivities, 1)
				s.True(description.PendingActivities[0].Paused)

				// wait long enough for activity to retry if pause is not working
				// Note: because activity is retried we expect the attempts to be incremented
				err = util.InterruptibleSleep(ctx, 2*time.Second)
				s.NoError(err)

				// make sure activity is not completed, and was not retried
				description, err = env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
				s.NoError(err)
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
				s.NoError(api.unpause(ctx, env, workflowRun.GetID(), "activity-id", ""))

				var out string
				err = workflowRun.Get(ctx, &out)

				s.NoError(err)
			})

			s.Run("SameRequestID_IsIdempotent", func(s *ActivityAPIPauseSuite) {
				// Pausing an already-paused activity with the same request ID must succeed (no-op).
				env := testcore.NewEnv(s.T())

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
				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
					TaskQueue: env.WorkerTaskQueue(),
				}, workflowFn)
				s.NoError(err)

				// Wait for the first attempt to fail and the activity to enter retry backoff (attempt 2).
				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(int32(2), description.PendingActivities[0].Attempt)
				}, 5*time.Second, 100*time.Millisecond)

				// First pause with an explicit request ID.
				s.NoError(api.pause(ctx, env, workflowRun.GetID(), "activity-id", "identity", "reason", "my-pause-request-id"))

				s.Await(func(s *ActivityAPIPauseSuite) {
					description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(description.PendingActivities, 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
				}, 5*time.Second, 100*time.Millisecond)

				// Second pause with the same request ID — must succeed (idempotent no-op).
				s.NoError(api.pause(ctx, env, workflowRun.GetID(), "activity-id", "identity", "reason", "my-pause-request-id"))
			})

			s.Run("UpdateOptionsResetUnpause", func(s *ActivityAPIPauseSuite) {
				// End-to-end test: pause → update-options → reset → unpause all work together.
				// Verifies that the updated options persist through a reset and that the activity
				// completes at attempt 1 with the new options after unpause.
				env := testcore.NewEnv(s.T())

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
					s.Rcv(activityCompleteCh)
					return "done!", nil
				}

				workflowFn := makeWorkflowFunc(activityFunction)
				env.SdkWorker().RegisterWorkflow(workflowFn)
				env.SdkWorker().RegisterActivity(activityFunction)

				wfID := testcore.RandomizeStr("wf_id-" + s.T().Name())
				workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
					ID:        wfID,
					TaskQueue: env.WorkerTaskQueue(),
				}, workflowFn)
				s.NoError(err)

				// wait for activity to fail and enter retry backoff
				s.Await(func(s *ActivityAPIPauseSuite) {
					desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(desc.PendingActivities, 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, desc.PendingActivities[0].State)
					s.Greater(desc.PendingActivities[0].Attempt, int32(1))
				}, 5*time.Second, 200*time.Millisecond)

				// step 1: pause
				s.NoError(api.pause(ctx, env, wfID, "activity-id", "", "", ""))

				s.Await(func(s *ActivityAPIPauseSuite) {
					desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(desc.PendingActivities, 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSED, desc.PendingActivities[0].State)
				}, 5*time.Second, 100*time.Millisecond)

				// step 2: update-options (reduce schedule-to-close timeout while paused)
				_, err = env.FrontendClient().UpdateActivityOptions(ctx, &workflowservice.UpdateActivityOptionsRequest{
					Namespace: env.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{WorkflowId: wfID},
					Activity:  &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity-id"},
					ActivityOptions: &activitypb.ActivityOptions{
						ScheduleToCloseTimeout: durationpb.New(updatedScheduleToClose),
					},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"schedule_to_close_timeout"}},
				})
				s.NoError(err)

				s.Await(func(s *ActivityAPIPauseSuite) {
					desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(desc.PendingActivities, 1)
					s.Equal(updatedScheduleToClose, desc.PendingActivities[0].ActivityOptions.GetScheduleToCloseTimeout().AsDuration())
				}, 5*time.Second, 100*time.Millisecond)

				// step 3: reset while paused — stays PAUSED (keepPaused=true), attempt resets to 1
				_, err = env.FrontendClient().ResetActivity(ctx, &workflowservice.ResetActivityRequest{
					Namespace:  env.Namespace().String(),
					Execution:  &commonpb.WorkflowExecution{WorkflowId: wfID},
					Activity:   &workflowservice.ResetActivityRequest_Id{Id: "activity-id"},
					KeepPaused: true,
				})
				s.NoError(err)

				s.Await(func(s *ActivityAPIPauseSuite) {
					desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(desc.PendingActivities, 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSED, desc.PendingActivities[0].State)
					s.Equal(int32(1), desc.PendingActivities[0].Attempt)
					// updated options must survive the reset
					s.Equal(updatedScheduleToClose, desc.PendingActivities[0].ActivityOptions.GetScheduleToCloseTimeout().AsDuration())
				}, 5*time.Second, 100*time.Millisecond)

				// step 4: unpause
				activityWasReset.Store(true)
				s.NoError(api.unpause(ctx, env, wfID, "activity-id", ""))

				s.Await(func(s *ActivityAPIPauseSuite) {
					desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
					s.NoError(err)
					s.Len(desc.PendingActivities, 1)
					s.Equal(enumspb.PENDING_ACTIVITY_STATE_STARTED, desc.PendingActivities[0].State)
					s.Equal(int32(1), desc.PendingActivities[0].Attempt)
				}, 5*time.Second, 100*time.Millisecond)

				activityCompleteCh <- struct{}{}

				var out string
				err = workflowRun.Get(ctx, &out)
				s.NoError(err)
			})
		})
	}
}

// TestAttributesToActivityInContextMetadata verifies end to end that PauseActivity
// marks the targeted activity so that the activity's type and task queue — not just the workflow's —
// are resolved from mutable state and propagated to the caller in the "contextmetadata-bin" gRPC
// trailer, the same way RecordActivityTaskHeartbeat does.
func (s *ActivityAPIPauseSuite) TestAttributesToActivityInContextMetadata() {
	const (
		activityID   = "activity-id"
		activityType = "PauseAttributionActivity"
	)

	// The frontend only emits context metadata trailers when this is enabled, and it is read when
	// the frontend interceptor is constructed, so it must be set at cluster startup.
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.FrontendContextMetadataSetTrailer, true))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityStartedCn := make(chan struct{}, 1)
	releaseActivityCn := make(chan struct{})
	// Block on the activity's own context rather than a suite helper: the helper calls FailNow on
	// timeout, which is only safe on the test goroutine, and the test's own timeout below already
	// reports a hang.
	activityFunction := func(activityCtx context.Context) (string, error) {
		activityStartedCn <- struct{}{}
		select {
		case <-releaseActivityCn:
			return "done!", nil
		case <-activityCtx.Done():
			return "", activityCtx.Err()
		}
	}
	workflowFn := func(wfCtx workflow.Context) error {
		var ret string
		return workflow.ExecuteActivity(workflow.WithActivityOptions(wfCtx, workflow.ActivityOptions{
			ActivityID:             activityID,
			DisableEagerExecution:  true,
			StartToCloseTimeout:    15 * time.Minute,
			ScheduleToCloseTimeout: 30 * time.Minute,
		}), activityType).Get(wfCtx, &ret)
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivityWithOptions(activityFunction, sdkactivity.RegisterOptions{Name: activityType})

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)

	s.Rcv(activityStartedCn)

	var trailer metadata.MD
	_, err = env.FrontendClient().PauseActivity(ctx, &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowRun.GetID()},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: activityID},
		Identity:  "test-identity",
		Reason:    "test-reason",
	}, grpc.Trailer(&trailer))
	s.NoError(err)

	entries := contextMetadataFromTrailer(s.T(), trailer)
	// Attribution is keyed by scheduled event ID, which the caller doesn't know, so look up the
	// single activity entry the way consumers do.
	var gotActivityType, gotActivityTaskQueue string
	for key, value := range entries {
		switch {
		case strings.HasPrefix(key, "activity-type-"):
			s.Empty(gotActivityType, "expected metadata for exactly one activity, got %v", entries)
			gotActivityType = value
		case strings.HasPrefix(key, "activity-task-queue-"):
			s.Empty(gotActivityTaskQueue, "expected metadata for exactly one activity, got %v", entries)
			gotActivityTaskQueue = value
		default:
			// Not activity-scoped (e.g. the workflow's own type and task queue).
		}
	}
	s.Equal(activityType, gotActivityType, "trailer entries: %v", entries)
	s.Equal(env.WorkerTaskQueue(), gotActivityTaskQueue, "trailer entries: %v", entries)
	// Workflow attribution is still emitted alongside the activity's.
	s.NotEmpty(entries[contextutil.MetadataKeyWorkflowType], "trailer entries: %v", entries)

	_, err = env.FrontendClient().UnpauseActivity(ctx, &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowRun.GetID()},
		Activity:  &workflowservice.UnpauseActivityRequest_Id{Id: activityID},
	})
	s.NoError(err)

	close(releaseActivityCn)
	s.NoError(workflowRun.Get(ctx, nil))
}

// contextMetadataFromTrailer decodes the ContextMetadata proto that ContextMetadataInterceptor
// writes to the "contextmetadata-bin" gRPC trailer.
func contextMetadataFromTrailer(t *testing.T, trailer metadata.MD) map[string]string {
	t.Helper()
	values := trailer.Get("contextmetadata-bin")
	require.Len(t, values, 1, "expected exactly one contextmetadata-bin trailer, got trailer %v", trailer)
	var metadataProto contextpropagationspb.ContextMetadata
	require.NoError(t, proto.Unmarshal([]byte(values[0]), &metadataProto))
	return metadataProto.GetEntries()
}
