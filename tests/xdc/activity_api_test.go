package xdc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type (
	ActivityFunctions func() (string, error)
	WorkflowFunction  func(context2 workflow.Context) error

	ActivityApiStateReplicationSuite struct {
		xdcBaseSuite
	}
)

func TestActivityApiStateReplicationSuite(t *testing.T) {
	s := new(ActivityApiStateReplicationSuite)
	suite.Run(t, s)
}

func (s *ActivityApiStateReplicationSuite) SetupSuite() {
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]any)
	}
	s.setupSuite()
}

func (s *ActivityApiStateReplicationSuite) SetupTest() {
	s.setupTest()
}

func (s *ActivityApiStateReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *ActivityApiStateReplicationSuite) TestPauseActivityFailover() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityPausedCn := make(chan struct{})
	var activityWasPaused atomic.Bool
	var startedActivityCount atomic.Int32

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		if activityWasPaused.Load() == false {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}
		s.waitForChannel(ctx, activityPausedCn)
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	ns := s.createGlobalNamespace()
	activeSDKClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[0].Host().FrontendGRPCAddress(),
		Namespace: ns,
		Logger:    log.NewSdkLogger(s.logger),
	})
	s.NoError(err)

	taskQueue := testcore.RandomizeStr("tq")
	worker1 := sdkworker.New(activeSDKClient, taskQueue, sdkworker.Options{})

	worker1.RegisterWorkflow(workflowFn)
	worker1.RegisterActivity(activityFunction)

	s.NoError(worker1.Start())
	defer worker1.Stop()

	// start a workflow
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wfid-" + s.T().Name()),
		TaskQueue: taskQueue,
	}

	workflowRun, err := activeSDKClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// wait for activity to start/fail few times
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Greater(t, startedActivityCount.Load(), int32(2))
	}, 5*time.Second, 200*time.Millisecond)

	// pause the activity in cluster0
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	}
	pauseResp, err := s.clusters[0].Host().FrontendClient().PauseActivity(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(pauseResp)

	// verify activity is paused is cluster0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.True(t, description.PendingActivities[0].Paused)
	}, 5*time.Second, 200*time.Millisecond)

	// update the activity properties in cluster0
	updateRequest := &workflowservice.UpdateActivityOptionsRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity-id"},
		ActivityOptions: &activitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval: durationpb.New(2 * time.Second),
				MaximumAttempts: 10,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval", "retry_policy.maximum_attempts"}},
	}
	respUpdate, err := s.clusters[0].Host().FrontendClient().UpdateActivityOptions(ctx, updateRequest)
	s.NoError(err)
	s.NotNil(respUpdate)

	// verify activity is updated in cluster0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.GetPendingActivities())
		if description.GetPendingActivities() != nil {
			require.Len(t, description.PendingActivities, 1)
			require.True(t, description.PendingActivities[0].Paused)
			require.Nil(t, description.PendingActivities[0].CurrentRetryInterval)
		}
	}, 5*time.Second, 200*time.Millisecond)

	// reset the activity in cluster0, while keeping it paused
	resetRequest := &workflowservice.ResetActivityRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity:   &workflowservice.ResetActivityRequest_Id{Id: "activity-id"},
		KeepPaused: true,
	}
	resetResp, err := s.clusters[0].Host().FrontendClient().ResetActivity(ctx, resetRequest)
	s.NoError(err)
	s.NotNil(resetResp)

	// verify activity is reset, updated and paused in cluster0
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := activeSDKClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.GetPendingActivities())
		if description.GetPendingActivities() != nil {
			require.Len(t, description.PendingActivities, 1)
			require.True(t, description.PendingActivities[0].Paused)
			require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
			require.Nil(t, description.PendingActivities[0].CurrentRetryInterval)
		}
	}, 5*time.Second, 200*time.Millisecond)

	// stop worker1 so cluster0 won't make any progress on the activity (just in case)
	worker1.Stop()

	// failover to standby cluster
	s.failover(ns, 0, s.clusters[1].ClusterName(), 2)

	// get standby client
	standbyClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[1].Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)
	s.NotNil(standbyClient)

	// verify activity is still paused in cluster1
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := standbyClient.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.GetPendingActivities())
		if description.GetPendingActivities() != nil {
			require.Len(t, description.PendingActivities, 1)
			require.True(t, description.PendingActivities[0].Paused)
			require.Nil(t, description.PendingActivities[0].CurrentRetryInterval)
			require.Equal(t, int32(10), description.PendingActivities[0].MaximumAttempts)
		}
	}, 5*time.Second, 200*time.Millisecond)

	// start worker2
	worker2 := sdkworker.New(standbyClient, taskQueue, sdkworker.Options{})
	worker2.RegisterWorkflow(workflowFn)
	worker2.RegisterActivity(activityFunction)
	s.NoError(worker2.Start())
	defer worker2.Stop()

	// let the activity make progress once unpaused
	activityWasPaused.Store(true)

	// unpause the activity in cluster1
	unpauseRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Id{Id: "activity-id"},
	}
	unpauseResp, err := s.clusters[1].Host().FrontendClient().UnpauseActivity(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// unblock the activity
	activityPausedCn <- struct{}{}

	// wait for activity to finish
	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)
}

func (s *ActivityApiStateReplicationSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
	initialRetryInterval := 1 * time.Second
	scheduleToCloseTimeout := 30 * time.Minute
	startToCloseTimeout := 15 * time.Minute

	activityRetryPolicy := &temporal.RetryPolicy{
		InitialInterval:    initialRetryInterval,
		BackoffCoefficient: 1,
	}

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

func (s *ActivityApiStateReplicationSuite) waitForChannel(ctx context.Context, ch chan struct{}) {
	s.T().Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		s.FailNow("context timeout while waiting for channel")
	}
}

// Standalone activities use state-based replication; keep this setup separate from the WFA suite.
type StandaloneActivityStateReplicationSuite struct {
	xdcBaseSuite
}

func TestStandaloneActivityStateReplicationSuite(t *testing.T) {
	t.Parallel()

	s := new(StandaloneActivityStateReplicationSuite)
	s.enableTransitionHistory = true
	suite.Run(t, s)
}

func (s *StandaloneActivityStateReplicationSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableChasm.Key():                         true,
		activity.Enabled.Key():                                  true,
		activity.StandaloneAsyncHandoffEnabled.Key():            true,
		activity.EnableStandaloneActivityOperatorCommands.Key(): true,
	}
	s.setupSuite(testcore.WithDCRedirectionPolicy(config.DCRedirectionPolicy{Policy: interceptor.DCRedirectionPolicyAllAPIsForwarding}))
}

func (s *StandaloneActivityStateReplicationSuite) SetupTest() {
	s.setupTest()
}

func (s *StandaloneActivityStateReplicationSuite) TearDownSuite() {
	s.tearDownSuite()
}

// Failover must preserve live option changes, accepted handoff and pending retry state.
func (s *StandaloneActivityStateReplicationSuite) TestAsyncHandoffFailover() {
	for _, testCase := range []struct {
		name                   string
		handoffRetry           bool
		expectedRunState       enumspb.PendingActivityState
		expectedHandoffAttempt int32
	}{
		{name: "AwaitingCompletion", expectedRunState: enumspb.PENDING_ACTIVITY_STATE_AWAITING_COMPLETION, expectedHandoffAttempt: 1},
		{name: "StartToCloseExpiresDuringHandoffBackoff", handoffRetry: true, expectedRunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, expectedHandoffAttempt: 2},
	} {
		s.Run(testCase.name, func() {
			t := s.T()
			ctx := testcontext.For(t, testcontext.WithTimeout(90*time.Second))
			ns := s.createGlobalNamespace()
			tv := testvars.New(t)
			request := &workflowservice.StartActivityExecutionRequest{
				Namespace: ns, ActivityId: tv.ActivityID(), ActivityType: tv.ActivityType(), TaskQueue: tv.TaskQueue(),
				StartToCloseTimeout: durationpb.New(time.Hour),
				RetryPolicy:         &commonpb.RetryPolicy{InitialInterval: durationpb.New(time.Second), BackoffCoefficient: 1, MaximumAttempts: 3},
				AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
					StartToHandoffTimeout: durationpb.New(time.Hour), HandoffToCloseTimeout: durationpb.New(time.Hour),
					RetryPolicy: &commonpb.RetryPolicy{InitialInterval: durationpb.New(time.Hour), MaximumAttempts: 3},
				},
			}
			if !testCase.handoffRetry {
				request.AsyncHandoffOptions = nil
			}

			started, err := s.clusters[0].FrontendClient().StartActivityExecution(ctx, request)
			require.NoError(t, err)

			poll := func(client workflowservice.WorkflowServiceClient) *workflowservice.PollActivityTaskQueueResponse {
				t.Helper()
				task, err := client.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
					Namespace: ns, TaskQueue: request.TaskQueue, Identity: tv.WorkerIdentity(),
				})
				require.NoError(t, err)
				require.NotEmpty(t, task.GetTaskToken())
				return task
			}
			task := poll(s.clusters[0].FrontendClient())
			handoff := &workflowservice.RespondActivityTaskHandoffRequest{Namespace: ns, TaskToken: task.GetTaskToken()}
			if testCase.handoffRetry {
				// The hour-long backoff keeps the retry pending throughout replication and failover.
				_, err = s.clusters[0].FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: ns, TaskToken: task.GetTaskToken(),
					Failure: &failurepb.Failure{
						Message: "retry before failover",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "retryable"},
						},
					},
				})
			} else {
				// The standby frontend forwards the handoff to the namespace's active cluster.
				_, err = s.clusters[1].FrontendClient().RespondActivityTaskHandoff(ctx, handoff)
			}
			require.NoError(t, err)

			describe := &workflowservice.DescribeActivityExecutionRequest{
				Namespace: ns, ActivityId: request.ActivityId, RunId: started.GetRunId(),
				IncludeOutcome: true, IncludeLastFailure: true,
			}
			before, err := s.clusters[0].FrontendClient().DescribeActivityExecution(ctx, describe)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedRunState, before.GetInfo().GetRunState())
			require.EqualValues(t, 1, before.GetInfo().GetAttempt())
			require.Equal(t, !testCase.handoffRetry, before.GetInfo().GetAwaitingCompletion())
			if !testCase.handoffRetry {
				require.Zero(t, before.GetInfo().GetHandoffAttempt())
				require.Nil(t, before.GetInfo().GetAsyncHandoffOptions())
				require.NotNil(t, before.GetInfo().GetAsyncHandoffTime())
			} else {
				require.Equal(t, testCase.expectedHandoffAttempt, before.GetInfo().GetHandoffAttempt())
			}

			// Convert reporting-only handoff to configured completion, or replace the pending retry's policy.
			updated, err := s.clusters[0].FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
				Namespace: ns, ActivityId: request.ActivityId, RunId: started.GetRunId(),
				ActivityOptions: &activitypb.ActivityOptions{
					StartToCloseTimeout: durationpb.New(30 * time.Minute),
					AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
						StartToHandoffTimeout: durationpb.New(30 * time.Minute),
						HandoffToCloseTimeout: durationpb.New(30 * time.Minute),
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval: durationpb.New(time.Nanosecond), MaximumAttempts: 3,
						},
					},
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"start_to_close_timeout", "async_handoff_options"}},
			})
			require.NoError(t, err)
			afterUpdate, err := s.clusters[0].FrontendClient().DescribeActivityExecution(ctx, describe)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedHandoffAttempt, afterUpdate.GetInfo().GetHandoffAttempt())
			protorequire.ProtoEqual(t, before.GetInfo().GetAsyncHandoffTime(), afterUpdate.GetInfo().GetAsyncHandoffTime())
			protorequire.ProtoEqual(t, updated.GetActivityOptions().GetAsyncHandoffOptions(), afterUpdate.GetInfo().GetAsyncHandoffOptions())
			require.Equal(t, 30*time.Minute, afterUpdate.GetInfo().GetStartToCloseTimeout().AsDuration())
			if testCase.handoffRetry {
				// The revised delay has elapsed. Describe hides it, but no worker picks up the queued retry.
				require.Nil(t, afterUpdate.GetInfo().GetCurrentRetryInterval())
				protorequire.ProtoEqual(t, before.GetInfo().GetLastAttemptCompleteTime(), afterUpdate.GetInfo().GetLastAttemptCompleteTime())
			}
			before = afterUpdate

			// Wait for the application state on standby before changing namespace ownership.
			await.Require(ctx, t, func(t *await.T) {
				// Read standby state directly instead of forwarding this check to the active cluster.
				standbyContext := metadata.AppendToOutgoingContext(t.Context(), interceptor.DCRedirectionContextHeaderName, "false")
				replicated, err := s.clusters[1].FrontendClient().DescribeActivityExecution(standbyContext, describe)
				require.NoError(t, err)
				require.Equal(t, testCase.expectedRunState, replicated.GetInfo().GetRunState())
				require.EqualValues(t, 1, replicated.GetInfo().GetAttempt())
				require.Equal(t, testCase.expectedHandoffAttempt, replicated.GetInfo().GetHandoffAttempt())
				require.Equal(t, before.GetInfo().GetAwaitingCompletion(), replicated.GetInfo().GetAwaitingCompletion())
				protorequire.ProtoEqual(t, before.GetInfo().GetAsyncHandoffTime(), replicated.GetInfo().GetAsyncHandoffTime())
				protorequire.ProtoEqual(t, before.GetInfo().GetAsyncHandoffOptions(), replicated.GetInfo().GetAsyncHandoffOptions())
				protorequire.ProtoEqual(t, before.GetInfo().GetStartToCloseTimeout(), replicated.GetInfo().GetStartToCloseTimeout())
				protorequire.ProtoEqual(t, before.GetInfo().GetCurrentRetryInterval(), replicated.GetInfo().GetCurrentRetryInterval())
				protorequire.ProtoEqual(t, before.GetInfo().GetLastAttemptCompleteTime(), replicated.GetInfo().GetLastAttemptCompleteTime())
			}, 15*time.Second, 100*time.Millisecond)

			s.failover(ns, 0, s.clusters[1].ClusterName(), 2)
			client := s.clusters[1].FrontendClient()
			if testCase.handoffRetry {
				_, err = client.RespondActivityTaskHandoff(ctx, handoff)
				require.ErrorAs(t, err, new(*serviceerror.NotFound))

				// The revised retry must dispatch after failover without advancing the activity attempt.
				task = poll(client)
				require.EqualValues(t, 1, task.GetAttempt())
				require.EqualValues(t, 2, task.GetHandoffAttempt())
				protorequire.ProtoEqual(t, before.GetInfo().GetAsyncHandoffOptions(), task.GetAsyncHandoffOptions())
				protorequire.ProtoEqual(t, before.GetInfo().GetStartToCloseTimeout(), task.GetStartToCloseTimeout())

				// Put that delivery back in a long backoff to check its replicated carried clock.
				_, err = client.RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: ns, TaskToken: task.GetTaskToken(),
					Failure: &failurepb.Failure{
						Message: "retry after failover",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
								Type: "retryable", NextRetryDelay: durationpb.New(time.Hour),
							},
						},
					},
				})
				require.NoError(t, err)

				// Expire the replicated carried clock while no worker can pick up the retry.
				_, err = client.UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace: ns, ActivityId: request.ActivityId, RunId: started.GetRunId(),
					ActivityOptions: &activitypb.ActivityOptions{StartToCloseTimeout: durationpb.New(time.Nanosecond)},
					UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"start_to_close_timeout"}},
				})
				require.NoError(t, err)
				await.Require(ctx, t, func(t *await.T) {
					response, err := client.DescribeActivityExecution(t.Context(), describe)
					require.NoError(t, err)
					require.EqualValues(t, 2, response.GetInfo().GetAttempt())
					require.EqualValues(t, 1, response.GetInfo().GetHandoffAttempt())
					require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, response.GetInfo().GetRunState())
					require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, response.GetInfo().GetLastFailure().GetTimeoutFailureInfo().GetTimeoutType())
				}, 15*time.Second, 100*time.Millisecond)

				// The edit capped both phase timeouts; restore all three before the next pickup.
				restored, err := client.UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
					Namespace: ns, ActivityId: request.ActivityId, RunId: started.GetRunId(),
					ActivityOptions: &activitypb.ActivityOptions{
						StartToCloseTimeout: durationpb.New(time.Hour),
						AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
							StartToHandoffTimeout: durationpb.New(time.Hour),
							HandoffToCloseTimeout: durationpb.New(time.Hour),
						},
					},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
						"start_to_close_timeout",
						"async_handoff_options.start_to_handoff_timeout",
						"async_handoff_options.handoff_to_close_timeout",
					}},
				})
				require.NoError(t, err)
				protorequire.ProtoEqual(t, request.GetStartToCloseTimeout(), restored.GetActivityOptions().GetStartToCloseTimeout())
				protorequire.ProtoEqual(t, request.GetAsyncHandoffOptions().GetStartToHandoffTimeout(), restored.GetActivityOptions().GetAsyncHandoffOptions().GetStartToHandoffTimeout())
				protorequire.ProtoEqual(t, request.GetAsyncHandoffOptions().GetHandoffToCloseTimeout(), restored.GetActivityOptions().GetAsyncHandoffOptions().GetHandoffToCloseTimeout())

				task = poll(client)
				require.EqualValues(t, 2, task.GetAttempt())
				require.EqualValues(t, 1, task.GetHandoffAttempt())
			} else {
				// The original token still works, and duplicate handoff retains the accepted timestamp.
				_, err = client.RespondActivityTaskHandoff(ctx, handoff)
				require.NoError(t, err)
				after, err := client.DescribeActivityExecution(ctx, describe)
				require.NoError(t, err)
				protorequire.ProtoEqual(t, before.GetInfo().GetAsyncHandoffTime(), after.GetInfo().GetAsyncHandoffTime())
				protorequire.ProtoEqual(t, before.GetInfo().GetAsyncHandoffOptions(), after.GetInfo().GetAsyncHandoffOptions())
				protorequire.ProtoEqual(t, before.GetInfo().GetStartToCloseTimeout(), after.GetInfo().GetStartToCloseTimeout())
			}

			result := payloads.EncodeString("done")
			_, err = client.RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
				Namespace: ns, TaskToken: task.GetTaskToken(), Result: result,
			})
			require.NoError(t, err)
			for _, cluster := range s.clusters {
				await.Require(ctx, t, func(t *await.T) {
					closed, err := cluster.FrontendClient().DescribeActivityExecution(t.Context(), describe)
					require.NoError(t, err)
					require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, closed.GetInfo().GetStatus())
					require.False(t, closed.GetInfo().GetAwaitingCompletion())
					protorequire.ProtoEqual(t, result, closed.GetOutcome().GetResult())
				}, 15*time.Second, 100*time.Millisecond)
			}
		})
	}
}
