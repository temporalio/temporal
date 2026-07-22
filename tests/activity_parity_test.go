package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ActivityParitySuite pins down behavior parity between workflow activities and Standalone
// Activity Executions. Each Test* method covers one parity scenario, with a WorkflowActivity and
// a StandaloneActivity subtest that exercise the same scenario through each activity flavor's
// APIs.
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
	return env
}

// TestExponentialBackoffOverflowCapsToMaximumInterval pins down retry behavior when the
// exponential backoff calculation overflows: with a very large BackoffCoefficient, the interval
// computed for the second retry exceeds what an int64 duration can represent. A correct
// implementation must fall back to the policy's MaximumInterval, exactly as it does when the
// (non-overflowed) computed interval merely exceeds MaximumInterval. If an overflow instead
// produces a nonpositive interval that is not replaced with MaximumInterval, the retry is
// persisted with a ~zero backoff and is treated as already due, dispatching immediately and
// turning backoff overflow into a tight, immediate retry loop.
func (s *ActivityParitySuite) TestExponentialBackoffOverflowCapsToMaximumInterval() {
	const (
		initialInterval = time.Second
		hugeCoefficient = 1e18
		maximumInterval = 5 * time.Second
		// Below maximumInterval, so a task appearing within this window means the retry
		// dispatched without waiting out the (correctly capped) backoff.
		belowBackoffPoll = common.MinLongPollTimeout + time.Second
	)
	retryPolicy := &commonpb.RetryPolicy{
		InitialInterval:    durationpb.New(initialInterval),
		BackoffCoefficient: hugeCoefficient,
		MaximumInterval:    durationpb.New(maximumInterval),
		MaximumAttempts:    0,
	}
	retryableFailure := &failurepb.Failure{
		Message: "retry-error",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "retry-error", NonRetryable: false},
		},
	}
	// Allows for small scheduling/processing jitter; still tight enough to fail hard against a
	// buggy near-zero interval, which would be off by the entire ~5s maximumInterval.
	assertCappedToMaximumInterval := func(s *ActivityParitySuite, actual *durationpb.Duration) {
		s.NotNil(actual, "overflowed backoff must still report a retry interval")
		s.InDelta(maximumInterval.Seconds(), actual.AsDuration().Seconds(), 2,
			"overflowed backoff should be capped to ~MaximumInterval, not a near-zero, hot-loop-inducing value")
	}

	s.Run("WorkflowActivity", func(s *ActivityParitySuite) {
		env := s.newTestEnv()
		ctx := s.Context()

		activityID := testcore.RandomizeStr(s.T().Name())
		workflowID := testcore.RandomizeStr(s.T().Name())
		taskQueue := &taskqueuepb.TaskQueue{
			Name: testcore.RandomizeStr(s.T().Name()),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}

		startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           env.Namespace().String(),
			WorkflowId:          workflowID,
			WorkflowType:        &commonpb.WorkflowType{Name: "backoff-overflow-workflow"},
			TaskQueue:           taskQueue,
			WorkflowRunTimeout:  durationpb.New(time.Minute),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            defaultIdentity,
		})
		s.NoError(err)

		wtResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  defaultIdentity,
		})
		s.NoError(err)
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: wtResp.GetTaskToken(),
			Identity:  defaultIdentity,
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:          activityID,
						ActivityType:        &commonpb.ActivityType{Name: "backoff-overflow-activity"},
						TaskQueue:           taskQueue,
						Input:               payloads.EncodeString("input"),
						StartToCloseTimeout: durationpb.New(time.Minute),
						RetryPolicy:         retryPolicy,
					},
				},
			}},
		})
		s.NoError(err)

		pollActivity := func() []byte {
			resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: taskQueue,
				Identity:  defaultIdentity,
			})
			s.NoError(err)
			s.NotEmpty(resp.GetTaskToken())
			return resp.GetTaskToken()
		}
		failActivity := func(token []byte) {
			_, err := env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
				Namespace: env.Namespace().String(),
				TaskToken: token,
				Identity:  defaultIdentity,
				Failure:   retryableFailure,
			})
			s.NoError(err)
		}

		// Attempt 1: exponent 0, interval == InitialInterval; not overflowed.
		failActivity(pollActivity())
		// Attempt 2: exponent 1, InitialInterval * hugeCoefficient^1 overflows int64 nanoseconds.
		failActivity(pollActivity())

		describeResp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: startResp.GetRunId()},
		})
		s.NoError(err)
		s.Len(describeResp.GetPendingActivities(), 1)
		pending := describeResp.GetPendingActivities()[0]
		s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, pending.GetState())
		s.EqualValues(3, pending.GetAttempt())
		s.NotNil(pending.GetNextAttemptScheduleTime(), "overflowed backoff must still report a future dispatch time")
		s.True(pending.GetNextAttemptScheduleTime().AsTime().After(time.Now()), "future retry dispatch time")
		assertCappedToMaximumInterval(s, pending.GetCurrentRetryInterval())

		pollCtx, cancel := context.WithTimeout(ctx, belowBackoffPoll)
		defer cancel()
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  defaultIdentity,
		})
		if err == nil {
			s.Empty(pollResp.GetTaskToken(), "overflowed backoff must not dispatch before MaximumInterval elapses")
		} else {
			s.True(common.IsContextDeadlineExceededErr(err), "unexpected poll error: %v", err)
		}
	})

	s.Run("StandaloneActivity", func(s *ActivityParitySuite) {
		env := s.newTestEnv()
		ctx := s.Context()

		activityID := testcore.RandomizeStr(s.T().Name())
		taskQueue := &taskqueuepb.TaskQueue{
			Name: testcore.RandomizeStr(s.T().Name()),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}

		_, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          activityID,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            defaultIdentity,
			Input:               defaultInput,
			TaskQueue:           taskQueue,
			StartToCloseTimeout: durationpb.New(time.Minute),
			RetryPolicy:         retryPolicy,
		})
		s.NoError(err)

		pollActivity := func() []byte {
			resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: taskQueue,
				Identity:  defaultIdentity,
			})
			s.NoError(err)
			s.NotEmpty(resp.GetTaskToken())
			return resp.GetTaskToken()
		}
		failActivity := func(token []byte) {
			_, err := env.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
				Namespace: env.Namespace().String(),
				TaskToken: token,
				Identity:  defaultIdentity,
				Failure:   retryableFailure,
			})
			s.NoError(err)
		}

		// Attempt 1: exponent 0, interval == InitialInterval; not overflowed.
		failActivity(pollActivity())
		// Attempt 2: exponent 1, InitialInterval * hugeCoefficient^1 overflows int64 nanoseconds.
		failActivity(pollActivity())

		describeResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
		})
		s.NoError(err)
		info := describeResp.GetInfo()
		s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		s.EqualValues(3, info.GetAttempt())
		s.NotNil(info.GetNextAttemptScheduleTime(), "overflowed backoff must still report a future dispatch time")
		s.True(info.GetNextAttemptScheduleTime().AsTime().After(time.Now()), "future retry dispatch time")
		assertCappedToMaximumInterval(s, info.GetCurrentRetryInterval())

		pollCtx, cancel := context.WithTimeout(ctx, belowBackoffPoll)
		defer cancel()
		pollResp, err := env.FrontendClient().PollActivityTaskQueue(pollCtx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: taskQueue,
			Identity:  defaultIdentity,
		})
		if err == nil {
			s.Empty(pollResp.GetTaskToken(), "overflowed backoff must not dispatch before MaximumInterval elapses")
		} else {
			s.True(common.IsContextDeadlineExceededErr(err), "unexpected poll error: %v", err)
		}
	})
}
