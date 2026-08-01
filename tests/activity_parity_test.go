package tests

// SAA <-> WFA parity tests

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type activityParityTestSuite struct {
	parallelsuite.Suite[*activityParityTestSuite]
}

func TestActivityParityTestSuite(t *testing.T) {
	parallelsuite.Run(t, &activityParityTestSuite{})
}

func newActivityParityEnv(t *testing.T) *testcore.TestEnv {
	env := testcore.NewEnv(t)
	nsValues := func(value any) []dynamicconfig.ConstrainedValue {
		return []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: value},
		}
	}
	cluster := env.GetTestCluster()
	cluster.OverrideDynamicConfig(t, dynamicconfig.EnableChasm, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.Enabled, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.EnableStandaloneActivityOperatorCommands, nsValues(true))
	return env
}

// nextRetryDelayOverride is a worker-supplied next_retry_delay, distinct from
// activityLongDuration so the reported interval cannot be confused with the policy's.
const nextRetryDelayOverride = 10 * time.Second

// The retry policy's NonRetryableErrorTypes Must be respected. In particular, a StartToClose or
// Heartbeat timeout whose type is listed in the retry policy's NonRetryableErrorTypes using the
// special TemporalTimeout: syntax must fail the activity terminally (TimedOut) when it fires,
// rather than retrying.
func (s *activityParityTestSuite) TestNonRetryableErrorTypes() {
	env := newActivityParityEnv(s.T())

	testTimeoutWhileAttemptInProgress := func(t *testing.T, timeout model.Event) {
		trace := []model.Event{model.Poll, timeout}
		cfg := activityConfig{
			MaxAttempts:            2,
			NonRetryableErrorTypes: []string{retrypolicy.TimeoutFailureTypePrefix + timeoutType(timeout).String()},
		}

		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equalf(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				newWFADriver(t, env, cfg).driveTrace(t, trace).terminalStatus(t),
				"a %s timeout marked non-retryable must fail the activity terminally, not retry it", timeoutType(timeout))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equalf(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				newSAADriver(t, env, cfg).driveTrace(t, trace).terminalStatus(t),
				"a %s timeout marked non-retryable must fail the activity terminally, not retry it", timeoutType(timeout))
		})
	}

	s.Run("StartToClose", func(s *activityParityTestSuite) {
		t := s.T()
		testTimeoutWhileAttemptInProgress(t, model.StartToCloseElapses)
	})
	s.Run("Heartbeat", func(s *activityParityTestSuite) {
		t := s.T()
		testTimeoutWhileAttemptInProgress(t, model.HeartbeatElapses)
	})
}

// A terminal timeout must chain the application failure that drove its retries as its Cause, so an SDK
// can expose the real failure.
func (s *activityParityTestSuite) TestTimeoutPreservesUnderlyingFailureCause() {
	env := newActivityParityEnv(s.T())

	assertCausePreserved := func(
		t *testing.T,
		cfg activityConfig,
		trace []model.Event,
	) {
		const message = "the terminal timeout must chain the underlying application failure as its Cause"
		t.Run("WorkflowActivity", func(t *testing.T) {
			activity := newWFADriver(t, env, cfg).driveTrace(t, trace)
			timeout, ok := errors.AsType[*temporal.TimeoutError](activity.run.Get(activity.d.ctx, nil))
			require.True(t, ok)
			appErr, ok := errors.AsType[*temporal.ApplicationError](timeout.Unwrap())
			require.True(t, ok)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, activity.terminalStatus(t), message)
			require.Equal(t, "TestFailure", appErr.Type(), message)
			require.Equal(t, "test failure", appErr.Message(), message)
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			activity := newSAADriver(t, env, cfg).driveTrace(t, trace)
			cause := activity.describe(t).GetOutcome().GetFailure().GetCause()
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, activity.terminalStatus(t), message)
			require.Equal(t, "TestFailure", cause.GetApplicationFailureInfo().GetType(), message)
			require.Equal(t, "test failure", cause.GetMessage(), message)
		})
	}

	s.Run("StartToClose", func(s *activityParityTestSuite) {
		assertCausePreserved(s.T(), activityConfig{MaxAttempts: 2},
			[]model.Event{
				model.Poll,
				model.FailRetryably,
				model.BackoffElapses,
				model.Poll,
				model.StartToCloseElapses,
			},
		)
	})
	s.Run("Heartbeat", func(s *activityParityTestSuite) {
		assertCausePreserved(s.T(), activityConfig{MaxAttempts: 2},
			[]model.Event{
				model.Poll,
				model.FailRetryably,
				model.BackoffElapses,
				model.Poll,
				model.HeartbeatElapses,
			},
		)
	})
	s.Run("ScheduleToClose", func(s *activityParityTestSuite) {
		assertCausePreserved(s.T(), activityConfig{},
			[]model.Event{
				model.Poll,
				model.FailRetryably,
				model.ScheduleToCloseElapses,
			},
		)
	})
}

// A retried timeout replaces the prior attempt's failure instead of chaining it, keeping the stored
// failure bounded across retries.
func (s *activityParityTestSuite) TestRetriedTimeoutDoesNotChainPriorFailure() {
	env := newActivityParityEnv(s.T())
	for _, timeout := range []model.Event{model.StartToCloseElapses, model.HeartbeatElapses} {
		s.Run(timeout.Type.String(), func(s *activityParityTestSuite) {
			cfg := activityConfig{MaxAttempts: 3}
			trace := []model.Event{
				model.Poll,
				model.FailRetryably,
				model.BackoffElapses,
				model.Poll,
				timeout,
			}
			s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
				t := s.T()
				activity := newWFADriver(t, env, cfg).driveTrace(t, trace)
				lastFailure := activity.pendingActivityInfo(t).GetLastFailure()
				s.Require().NotNil(lastFailure)
				s.Require().Nil(lastFailure.GetCause())
			})
			s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
				t := s.T()
				activity := newSAADriver(t, env, cfg).driveTrace(t, trace)
				lastFailure := activity.describe(t).GetInfo().GetLastFailure()
				s.Require().NotNil(lastFailure)
				s.Require().Nil(lastFailure.GetCause())
			})
		})
	}
}

// current_retry_interval and next_attempt_schedule_time are reported while a retry is backing off
// (before it is dispatched to Matching), and for next_attempt_schedule_time also during start delay
// (SAA only). Once the attempt is dispatched, or while the activity is paused, both are nil.
func (s *activityParityTestSuite) TestCurrentRetryIntervalAndNextAttemptScheduleTime() {
	env := newActivityParityEnv(s.T())

	// both drives a trace through both implementations, asserting each reports expected.
	both := func(t *testing.T, cfg activityConfig, trace []model.Event, expected activityInfo) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t))
		})
	}

	// First attempt within its start delay (SAA only): the pending dispatch is in the future and is
	// not a retry.
	s.Run("StartDelayPending", func(s *activityParityTestSuite) {
		t := s.T()
		cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration, StartDelay: activityLongDuration}
		info := newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.Equal(t, info.GetExecutionTime().AsTime(), info.GetNextAttemptScheduleTime().AsTime(),
			"during a start delay, NextAttemptScheduleTime is the pending dispatch time (schedule+delay)")
		require.Nil(t, info.GetCurrentRetryInterval(), "the first attempt is not a retry")
	})

	// First attempt running: no pending next dispatch, and no retry interval reported while running.
	s.Run("FirstAttemptRunning", func(s *activityParityTestSuite) {
		t := s.T()
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}, []model.Event{model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  1,
			})
	})

	// Backing off before the retry is dispatched: both the interval and the next-attempt schedule time
	// are populated.
	s.Run("BackingOff", func(s *activityParityTestSuite) {
		t := s.T()
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}, []model.Event{model.Poll, model.FailRetryably},
			activityInfo{
				RunState:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:                    2,
				CurrentRetryInterval:       activityLongDuration,
				NextAttemptScheduleTimeSet: true,
			})
	})

	// Backing off after a worker-supplied next_retry_delay: the reported interval is the worker's
	// override.
	s.Run("NextRetryDelayOverride", func(s *activityParityTestSuite) {
		t := s.T()
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration, NextRetryDelay: nextRetryDelayOverride},
			[]model.Event{model.Poll, model.FailRetryably},
			activityInfo{
				RunState:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:                    2,
				CurrentRetryInterval:       nextRetryDelayOverride,
				NextAttemptScheduleTimeSet: true,
			})
	})

	// Once the retry's dispatch deadline is due, both fields are nil. This projection does not by
	// itself prove that the dispatch task reached Matching; the following running-attempt cases prove
	// that with a Poll.
	s.Run("RetryDue", func(s *activityParityTestSuite) {
		t := s.T()
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:  2,
			})
	})

	// Retry attempt running with a further retry still permitted (max 3): nothing pending while running.
	s.Run("RetryAttemptRunning", func(s *activityParityTestSuite) {
		t := s.T()
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  2,
			})
	})

	// Final attempt running with no retry remaining (max 2): still nothing pending while running.
	s.Run("FinalAttemptRunning", func(s *activityParityTestSuite) {
		t := s.T()
		both(t, activityConfig{MaxAttempts: 2, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  2,
			})
	})

	// Paused while still backing off: dispatch will not occur while paused, so neither the interval nor
	// the next-attempt schedule time should be reported.
	s.Run("PausedBeforeDispatch", func(s *activityParityTestSuite) {
		t := s.T()
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}, []model.Event{model.Poll, model.FailRetryably, model.Pause},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED,
				Attempt:  2,
			})
	})

	// Paused after the retry was dispatched: the dispatched code path already nils both fields, and the
	// pause preserves that. No field of ActivityExecutionInfo or PendingActivityInfo distinguishes this
	// from PausedBeforeDispatch in either implementation, so the two subtests differ in the state they reach,
	// not in what they assert.
	s.Run("PausedAfterDispatch", func(s *activityParityTestSuite) {
		t := s.T()
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Pause},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED,
				Attempt:  2,
			})
	})
}

// Resetting a running paused activity with keepPaused preserves the pending pause while the worker
// still owns its task. Describe must continue to report PAUSE_REQUESTED until that worker yields.
func (s *activityParityTestSuite) TestPauseRequestedAfterResetKeepPaused() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.Pause, model.ResetKeepPaused}
	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED,
			newWFADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t).RunState)
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED,
			newSAADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t).RunState)
	})
}

// TestCancel drives a running activity through cancellation in both implementations. RequestCancel uses the
// standalone activity RPC for SAA and workflow cancellation for WFA; the worker then acknowledges the
// request with RespondActivityTaskCanceled.
func (s *activityParityTestSuite) TestCancel() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.RequestCancel, model.RespondCanceled}
	cfg := activityConfig{MaxAttempts: 1}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED,
			newWFADriver(t, env, cfg).driveTrace(t, trace).terminalStatus(t))
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED,
			newSAADriver(t, env, cfg).driveTrace(t, trace).terminalStatus(t))
	})
}

func (s *activityParityTestSuite) TestCompleteByID_BeforeAnyWorkerStarts() {
	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := env.Tv()

		we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           env.Namespace().String(),
			WorkflowId:          tv.WorkflowID(),
			WorkflowType:        tv.WorkflowType(),
			TaskQueue:           tv.TaskQueue(),
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            tv.WorkerIdentity(),
		})
		s.NoError(err)

		// Schedule the activity, but no poller ever calls PollActivityTaskQueue for it, so it
		// remains Scheduled indefinitely.
		_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             tv.ActivityID(),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								Input:                  payloads.EncodeString("input"),
								ScheduleToCloseTimeout: durationpb.New(time.Minute),
								StartToCloseTimeout:    durationpb.New(time.Minute),
							},
						},
					}},
				}, nil
			})
		s.NoError(err)

		_, err = env.FrontendClient().RespondActivityTaskCompletedById(s.Context(), &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace:  env.Namespace().String(),
			WorkflowId: tv.WorkflowID(),
			RunId:      we.GetRunId(),
			ActivityId: tv.ActivityID(),
			Result:     payloads.EncodeString("result"),
			Identity:   tv.WorkerIdentity(),
		})
		s.NoError(err, "force-completing a scheduled (never-started) workflow activity by ID must succeed")

		// Drain the resulting workflow task and complete the workflow to confirm the completion
		// was actually applied, not just accepted and dropped.
		_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
							CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
								Result: payloads.EncodeString("done"),
							},
						},
					}},
				}, nil
			})
		s.NoError(err)

		descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: we.GetRunId()},
		})
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.GetWorkflowExecutionInfo().GetStatus())
	})

	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		env := testcore.NewEnv(s.T(),
			testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
			testcore.WithDynamicConfig(activity.Enabled, true),
		)
		tv := env.Tv()

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          tv.ActivityID(),
			ActivityType:        tv.ActivityType(),
			Identity:            tv.WorkerIdentity(),
			Input:               payloads.EncodeString("input"),
			TaskQueue:           tv.TaskQueue(),
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           uuid.NewString(),
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		// No poller ever calls PollActivityTaskQueue for it, so it remains Scheduled indefinitely.
		descBefore, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: tv.ActivityID(),
			RunId:      startResp.GetRunId(),
		})
		s.NoError(err)
		s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, descBefore.GetInfo().GetRunState())

		_, err = env.FrontendClient().RespondActivityTaskCompletedById(s.Context(), &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace:  env.Namespace().String(),
			RunId:      startResp.GetRunId(),
			ActivityId: tv.ActivityID(),
			Result:     payloads.EncodeString("result"),
			Identity:   tv.WorkerIdentity(),
		})
		s.NoError(err, "force-completing a scheduled (never-started) standalone activity by ID must succeed, matching workflow-activity behavior")

		descAfter, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     tv.ActivityID(),
			RunId:          startResp.GetRunId(),
			IncludeOutcome: true,
		})
		s.NoError(err)
		s.Equal(enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, descAfter.GetInfo().GetStatus())
		s.NotNil(descAfter.GetInfo().GetLastStartedTime(),
			"a force-completed activity must still record a started time, even though no worker ever started it")
	})
}

// TestCompleteByID_WhilePaused asserts that force-completing an activity by ID also works while
// the activity is Paused (a never-started activity that had a pause requested before any worker
// picked it up) — Paused has no attempt in progress, just like Scheduled.
func (s *activityParityTestSuite) TestCompleteByID_WhilePaused() {
	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		env := testcore.NewEnv(s.T())
		tv := env.Tv()

		we, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:           uuid.NewString(),
			Namespace:           env.Namespace().String(),
			WorkflowId:          tv.WorkflowID(),
			WorkflowType:        tv.WorkflowType(),
			TaskQueue:           tv.TaskQueue(),
			WorkflowRunTimeout:  durationpb.New(100 * time.Second),
			WorkflowTaskTimeout: durationpb.New(10 * time.Second),
			Identity:            tv.WorkerIdentity(),
		})
		s.NoError(err)

		// Schedule the activity, but no poller ever calls PollActivityTaskQueue for it, so it
		// remains Scheduled indefinitely.
		_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             tv.ActivityID(),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								Input:                  payloads.EncodeString("input"),
								ScheduleToCloseTimeout: durationpb.New(time.Minute),
								StartToCloseTimeout:    durationpb.New(time.Minute),
							},
						},
					}},
				}, nil
			})
		s.NoError(err)

		_, err = env.FrontendClient().PauseActivity(s.Context(), &workflowservice.PauseActivityRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
			Activity:  &workflowservice.PauseActivityRequest_Id{Id: tv.ActivityID()},
			Identity:  tv.WorkerIdentity(),
			RequestId: uuid.NewString(),
		})
		s.NoError(err)

		_, err = env.FrontendClient().RespondActivityTaskCompletedById(s.Context(), &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace:  env.Namespace().String(),
			WorkflowId: tv.WorkflowID(),
			RunId:      we.GetRunId(),
			ActivityId: tv.ActivityID(),
			Result:     payloads.EncodeString("result"),
			Identity:   tv.WorkerIdentity(),
		})
		s.NoError(err, "force-completing a paused (never-started) workflow activity by ID must succeed")

		// Drain the resulting workflow task and complete the workflow to confirm the completion
		// was actually applied, not just accepted and dropped.
		_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv,
			func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
				return &workflowservice.RespondWorkflowTaskCompletedRequest{
					Commands: []*commandpb.Command{{
						CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
							CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
								Result: payloads.EncodeString("done"),
							},
						},
					}},
				}, nil
			})
		s.NoError(err)

		descResp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: we.GetRunId()},
		})
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, descResp.GetWorkflowExecutionInfo().GetStatus())
	})

	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		env := testcore.NewEnv(s.T(),
			testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
			testcore.WithDynamicConfig(activity.Enabled, true),
			testcore.WithDynamicConfig(activity.EnableStandaloneActivityOperatorCommands, true),
		)
		tv := env.Tv()

		startResp, err := env.FrontendClient().StartActivityExecution(s.Context(), &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          tv.ActivityID(),
			ActivityType:        tv.ActivityType(),
			Identity:            tv.WorkerIdentity(),
			Input:               payloads.EncodeString("input"),
			TaskQueue:           tv.TaskQueue(),
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           uuid.NewString(),
		})
		s.NoError(err)
		s.True(startResp.GetStarted())

		// No poller ever calls PollActivityTaskQueue for it, so it remains Scheduled until paused.
		_, err = env.FrontendClient().PauseActivityExecution(s.Context(), &workflowservice.PauseActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: tv.ActivityID(),
			RunId:      startResp.GetRunId(),
			Identity:   tv.WorkerIdentity(),
			Reason:     "test-pause",
		})
		s.NoError(err)

		descBefore, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: tv.ActivityID(),
			RunId:      startResp.GetRunId(),
		})
		s.NoError(err)
		s.Equal(enumspb.PENDING_ACTIVITY_STATE_PAUSED, descBefore.GetInfo().GetRunState())

		_, err = env.FrontendClient().RespondActivityTaskCompletedById(s.Context(), &workflowservice.RespondActivityTaskCompletedByIdRequest{
			Namespace:  env.Namespace().String(),
			RunId:      startResp.GetRunId(),
			ActivityId: tv.ActivityID(),
			Result:     payloads.EncodeString("result"),
			Identity:   tv.WorkerIdentity(),
		})
		s.NoError(err, "force-completing a paused (never-started) standalone activity by ID must succeed, matching workflow-activity behavior")

		descAfter, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
			Namespace:      env.Namespace().String(),
			ActivityId:     tv.ActivityID(),
			RunId:          startResp.GetRunId(),
			IncludeOutcome: true,
		})
		s.NoError(err)
		s.Equal(enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, descAfter.GetInfo().GetStatus())
		s.NotNil(descAfter.GetInfo().GetLastStartedTime(),
			"a force-completed activity must still record a started time, even though no worker ever started it")
	})
}
