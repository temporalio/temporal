package tests

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ActivityParitySuite pins down behavior parity between workflow activities and
// Standalone Activity Executions. Each Test* method covers one parity scenario,
// with a WorkflowActivity and a StandaloneActivity subtest that exercise the
// same scenario through each activity flavor's APIs.
type ActivityParitySuite struct {
	parallelsuite.Suite[*ActivityParitySuite]
}

func TestActivityParitySuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityParitySuite{})
}

type activityParityEnv struct {
	*testcore.TestEnv
}

func (s *ActivityParitySuite) newTestEnv(opts ...testcore.TestOption) *activityParityEnv {
	env := &activityParityEnv{
		TestEnv: testcore.NewEnv(s.T(), opts...),
	}
	nsValues := func(value any) []dynamicconfig.ConstrainedValue {
		return []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: value},
			{Constraints: dynamicconfig.Constraints{Namespace: env.ExternalNamespace().String()}, Value: value},
		}
	}
	cluster := env.GetTestCluster()
	cluster.OverrideDynamicConfig(s.T(), dynamicconfig.EnableChasm, nsValues(true))
	cluster.OverrideDynamicConfig(s.T(), activity.Enabled, nsValues(true))
	cluster.OverrideDynamicConfig(s.T(), activity.EnableCallbacks, nsValues(true))
	return env
}

// TestPauseAtRetryLimit pins down the terminal behavior of pausing an activity on its
// last permitted retry attempt: once the retry budget (MaximumAttempts) is exhausted,
// the activity terminates rather than remain pending+paused forever.
func (s *ActivityParitySuite) TestPauseAtRetryLimit() {
	const maximumAttempts = 2

	s.Run("WorkflowActivity", func(s *ActivityParitySuite) {
		env := s.newTestEnv()

		var startedActivityCount atomic.Int32
		unblockLastAttempt := make(chan struct{})

		activityFunction := func() (string, error) {
			n := startedActivityCount.Add(1)
			if n < maximumAttempts {
				return "", errors.New("retryable failure") //nolint:err113
			}
			<-unblockLastAttempt
			return "", errors.New("final retryable failure") //nolint:err113
		}

		workflowFn := func(ctx workflow.Context) error {
			var ret string
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				ActivityID:            "activity-id",
				DisableEagerExecution: true,
				StartToCloseTimeout:   time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Millisecond,
					BackoffCoefficient: 1,
					MaximumAttempts:    maximumAttempts,
				},
			}), activityFunction).Get(ctx, &ret)
			return err
		}

		env.SdkWorker().RegisterWorkflow(workflowFn)
		env.SdkWorker().RegisterActivity(activityFunction)

		workflowOptions := sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr(s.T().Name()),
			TaskQueue: env.WorkerTaskQueue(),
		}
		workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
		s.NoError(err)

		// Wait for the last permitted attempt to be running.
		s.Await(func(s *ActivityParitySuite) {
			s.EqualValues(maximumAttempts, startedActivityCount.Load())
		}, 10*time.Second, 50*time.Millisecond)

		// Pause while the activity is running its last permitted attempt.
		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			WorkflowId: workflowRun.GetID(),
			ActivityId: "activity-id",
			Identity:   "test-identity",
			Reason:     "pause on last attempt",
		})
		s.NoError(err)

		s.Await(func(s *ActivityParitySuite) {
			description, dErr := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(dErr)
			s.Len(description.GetPendingActivities(), 1)
			s.True(description.PendingActivities[0].Paused)
		}, 5*time.Second, 50*time.Millisecond)

		// Let the last permitted attempt fail while the pause is in effect.
		close(unblockLastAttempt)

		// The retry budget is exhausted: the activity (and the workflow) must terminate,
		// matching SAA's behavior, instead of remaining pending+paused forever.
		err = workflowRun.Get(s.Context(), nil)
		var wfExecutionError *temporal.WorkflowExecutionError
		s.ErrorAs(err, &wfExecutionError)
		var activityError *temporal.ActivityError
		s.ErrorAs(wfExecutionError, &activityError)
		s.Equal(enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, activityError.RetryState())
	})

	s.Run("StandaloneActivity", func(s *ActivityParitySuite) {
		env := s.newTestEnv()

		activityID := testcore.RandomizeStr(s.T().Name())
		taskQueue := testcore.RandomizeStr(s.T().Name())

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:              env.Namespace().String(),
			ActivityId:             activityID,
			ActivityType:           env.Tv().ActivityType(),
			TaskQueue:              &taskqueuepb.TaskQueue{Name: taskQueue},
			ScheduleToCloseTimeout: durationpb.New(time.Minute),
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:    durationpb.New(time.Millisecond),
				BackoffCoefficient: 1,
				MaximumAttempts:    maximumAttempts,
			},
		})
		s.NoError(err)
		runID := startResp.RunId

		retryableFailure := func(msg string) *failurepb.Failure {
			return &failurepb.Failure{
				Message: msg,
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			}
		}

		// Drive to the last permitted attempt.
		var pollResp *workflowservice.PollActivityTaskQueueResponse
		for attempt := int32(1); attempt <= maximumAttempts; attempt++ {
			pollResp, err = env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			})
			s.NoError(err)
			s.EqualValues(attempt, pollResp.Attempt)

			if attempt < maximumAttempts {
				_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: env.Namespace().String(),
					TaskToken: pollResp.TaskToken,
					Failure:   retryableFailure("retryable failure"),
				})
				s.NoError(err)
			}
		}

		// Pause while the last permitted attempt is running.
		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
			Identity:   "test-identity",
			Reason:     "pause on last attempt",
		})
		s.NoError(err)

		descResp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		s.NoError(err)
		s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, descResp.GetInfo().GetRunState())

		// Fail the last permitted attempt while the pause is in effect. The retry budget is
		// exhausted, so the activity must terminate rather than remain pending+paused.
		_, err = env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Failure:   retryableFailure("final retryable failure"),
		})
		s.NoError(err)

		descResp, err = env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		s.NoError(err)
		s.Equal(enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, descResp.GetInfo().GetStatus())
	})
}
