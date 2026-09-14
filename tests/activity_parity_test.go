package tests

// SAA <-> WFA parity tests

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/tests/testcore"
)

type activityParityTestSuite struct {
	parallelsuite.Suite[*activityParityTestSuite]
}

func TestActivityParityTestSuite(t *testing.T) {
	parallelsuite.Run(t, &activityParityTestSuite{})
}

// newActivityParityEnv is a test env with standalone activity enabled.
func newActivityParityEnv(t *testing.T) *testcore.TestEnv {
	env := testcore.NewEnv(t)
	nsValues := func(value any) []dynamicconfig.ConstrainedValue {
		return []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: value},
		}
	}
	cluster := env.GetTestCluster()
	// No child partitions => no fetching from root => stay within matching's namespace rate limit.
	cluster.OverrideDynamicConfig(t, dynamicconfig.MatchingNumTaskqueueReadPartitions, nsValues(1))
	cluster.OverrideDynamicConfig(t, dynamicconfig.MatchingNumTaskqueueWritePartitions, nsValues(1))
	cluster.OverrideDynamicConfig(t, dynamicconfig.EnableChasm, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.Enabled, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.EnableStandaloneActivityOperatorCommands, nsValues(true))
	return env
}

func assertActivityTaskNotCancelRequested(t *testing.T, err error) {
	var invalidArgumentErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgumentErr)
	require.Equal(t, consts.ErrActivityTaskNotCancelRequested.Error(), invalidArgumentErr.Message)
}

// nextRetryDelayOverride is a worker-supplied next_retry_delay, distinct from
// activityLongDuration so the reported interval cannot be confused with the policy's.
const nextRetryDelayOverride = 10 * time.Second

// A StartToClose or Heartbeat timeout whose type is listed in the retry policy's NonRetryableErrorTypes
// must fail the activity terminally (TimedOut) when it fires, rather than retrying.
func (s *activityParityTestSuite) TestNonRetryableErrorTypes() {
	env := newActivityParityEnv(s.T())

	testTimeoutWhileAttemptInProgress := func(t *testing.T, timeout model.Event) {
		trace := []model.Event{model.Poll, timeout}
		cfg := activityConfig{
			MaxAttempts:            3,
			NonRetryableErrorTypes: []string{retrypolicy.TimeoutFailureTypePrefix + timeoutType(timeout).String()},
		}

		expected := activityTerminalProjection{
			Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			FailureType: timeoutType(timeout).String(),
			RetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
		}
		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equalf(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t),
				"a %s timeout marked non-retryable must fail the activity terminally, not retry it", timeoutType(timeout))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equalf(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t),
				"a %s timeout marked non-retryable must fail the activity terminally, not retry it", timeoutType(timeout))
		})
	}

	// A timeout can also arrive as a failure reported through RespondActivityTaskFailedById, rather
	// than from the server's own timer. That API is for a service completing an activity out of
	// band; a worker uses the task-token form. Either way the failure is classified against the same
	// NonRetryableErrorTypes list, so it too closes the activity rather than retrying it. WFA
	// surfaces a reported timeout as timed out where SAA surfaces it as failed, so terminal status
	// itself is not parity.
	testTimeoutReportedByID := func(t *testing.T, timeout enumspb.TimeoutType, failureType model.FailureType) {
		trace := []model.Event{
			model.Poll,
			{Type: model.RespondFailedByIDType, Failure: &model.Failure{Type: failureType}},
		}
		cfg := activityConfig{
			MaxAttempts:            2,
			NonRetryableErrorTypes: []string{retrypolicy.TimeoutFailureTypePrefix + timeout.String()},
		}
		const message = "a %s timeout marked non-retryable must close the activity, not retry it"

		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equalf(t, activityTerminalProjection{
				Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				FailureType: timeout.String(),
				RetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
			}, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t), message, timeout)
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equalf(t, activityTerminalProjection{
				Status:      enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				FailureType: timeout.String(),
				RetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
			}, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t), message, timeout)
		})
	}

	s.Run("StartToClose", func(s *activityParityTestSuite) {
		t := s.T()
		testTimeoutWhileAttemptInProgress(t, model.StartToCloseElapses)
	})
	s.Run("Heartbeat", func(s *activityParityTestSuite) {
		testTimeoutWhileAttemptInProgress(s.T(), model.HeartbeatElapses)
	})
	s.Run("StartToCloseReportedByID", func(s *activityParityTestSuite) {
		testTimeoutReportedByID(s.T(), enumspb.TIMEOUT_TYPE_START_TO_CLOSE, model.StartToCloseTimeoutFailureType)
	})
	s.Run("HeartbeatReportedByID", func(s *activityParityTestSuite) {
		testTimeoutReportedByID(s.T(), enumspb.TIMEOUT_TYPE_HEARTBEAT, model.HeartbeatTimeoutFailureType)
	})
}

// A retryable ServerFailure reported through RespondActivityTaskFailedById must be retried, exactly
// like a retryable ApplicationFailure. Only the by-ID API accepts a non-application failure (the
// by-token API rejects one), so this is the path a ServerFailure reaches the handler through.
func (s *activityParityTestSuite) TestRetryableServerFailureIsRetried() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
	trace := []model.Event{
		model.Poll,
		{Type: model.RespondFailedByIDType, Failure: &model.Failure{Type: model.ServerFailureType}},
	}
	// A second attempt is scheduled and backing off, rather than the activity going terminal.
	expected := activityInfo{
		RunState:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                    2,
		CurrentRetryInterval:       activityLongDuration,
		NextAttemptScheduleTimeSet: true,
	}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t),
			"a retryable ServerFailure must schedule another attempt, not fail terminally")
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t),
			"a retryable ServerFailure must schedule another attempt, not fail terminally")
	})
}

// Failures submitted through RespondActivityTaskFailedById use the same retry classification for
// standalone and workflow activities, including non-application failures a worker synthesizes.
func (s *activityParityTestSuite) TestSyntheticFailuresHaveRetryParity() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
	retryableFailures := []struct {
		name        string
		failureType model.FailureType
	}{
		{name: "StartToCloseTimeout", failureType: model.StartToCloseTimeoutFailureType},
		{name: "HeartbeatTimeout", failureType: model.HeartbeatTimeoutFailureType},
		{name: "UnknownFailure", failureType: model.UnknownFailureType},
	}
	wantRetry := activityInfo{
		RunState:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                    2,
		CurrentRetryInterval:       activityLongDuration,
		NextAttemptScheduleTimeSet: true,
	}

	for _, tc := range retryableFailures {
		s.Run(tc.name, func(s *activityParityTestSuite) {
			trace := []model.Event{
				model.Poll,
				{Type: model.RespondFailedByIDType, Failure: &model.Failure{Type: tc.failureType}},
			}
			s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
				t := s.T()
				require.Equal(t, wantRetry, newWFADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t))
			})
			s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
				t := s.T()
				require.Equal(t, wantRetry, newSAADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t))
			})
		})
	}

	// Both implementations close these failures without retrying. WFA surfaces them as timed out,
	// while SAA surfaces worker-reported timeouts as failed, so terminal status itself is not parity.
	nonRetryableTimeouts := []struct {
		name        string
		failureType model.FailureType
		timeoutType enumspb.TimeoutType
	}{
		{
			name:        "ScheduleToStartTimeout",
			failureType: model.ScheduleToStartTimeoutFailureType,
			timeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		},
		{
			name:        "ScheduleToCloseTimeout",
			failureType: model.ScheduleToCloseTimeoutFailureType,
			timeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		},
	}
	for _, tc := range nonRetryableTimeouts {
		s.Run(tc.name, func(s *activityParityTestSuite) {
			trace := []model.Event{
				model.Poll,
				{Type: model.RespondFailedByIDType, Failure: &model.Failure{Type: tc.failureType}},
			}
			s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
				t := s.T()
				require.Equal(t, activityTerminalProjection{
					Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
					FailureType: tc.timeoutType.String(),
					RetryState:  enumspb.RETRY_STATE_TIMEOUT,
				}, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
			})
			s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
				t := s.T()
				require.Equal(t, activityTerminalProjection{
					Status:      enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
					FailureType: tc.timeoutType.String(),
					RetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
				}, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
			})
		})
	}
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
			timeout, ok := errors.AsType[*temporal.TimeoutError](activity.run.Get(activity.testContext(), nil))
			require.True(t, ok)
			appErr, ok := errors.AsType[*temporal.ApplicationError](timeout.Unwrap())
			require.True(t, ok)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, activity.terminal(t).Status, message)
			require.Equal(t, "TestFailure", appErr.Type(), message)
			require.Equal(t, "test failure", appErr.Message(), message)
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			activity := newSAADriver(t, env, cfg).driveTrace(t, trace)
			cause := activity.describe(t).GetOutcome().GetFailure().GetCause()
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, activity.terminal(t).Status, message)
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

// A RespondActivityTaskFailed with an omitted Failure is retryable, for parity with workflow
// activities: rather than closing the activity with no consumable outcome, the server backs it off
// for another attempt.
func (s *activityParityTestSuite) TestNilFailureIsRetryable() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
	trace := []model.Event{model.Poll, {Type: model.RespondFailedType}}
	want := activityInfo{
		RunState:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                    2,
		CurrentRetryInterval:       activityLongDuration,
		NextAttemptScheduleTimeSet: true,
	}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, want, newWFADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t))
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, want, newSAADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t))
	})
}

// A standalone activity that exhausts its retries after failing without a Failure must still close
// with a consumable terminal failure. Otherwise PollActivityExecution returns a nil outcome and a
// client cannot tell the closed activity apart from one that simply has no result yet, so it polls
// forever. Workflow activities have no equivalent gap because the SDK surfaces an ActivityError even
// for a nil cause, so this is a standalone-only assertion.
func (s *activityParityTestSuite) TestNilFailureExhaustedClosesWithConsumableOutcome() {
	env := newActivityParityEnv(s.T())
	t := s.T()
	cfg := activityConfig{MaxAttempts: 1}
	trace := []model.Event{model.Poll, {Type: model.RespondFailedType}}

	h := newSAADriver(t, env, cfg).driveTrace(t, trace)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, h.terminal(t).Status)
	require.NotNil(t, h.describe(t).GetOutcome().GetFailure(),
		"a standalone activity that failed without worker-supplied details must still expose a terminal failure")
}

// current_retry_interval and next_attempt_schedule_time are reported while a retry is backing off
// (before it is dispatched to Matching), and for next_attempt_schedule_time also during start delay
// (SAA only). Once the attempt is dispatched, or while the activity is paused, both are nil.
func (s *activityParityTestSuite) TestCurrentRetryIntervalAndNextAttemptScheduleTime() {
	t := s.T()
	env := newActivityParityEnv(t)

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
	t.Run("StartDelayPending", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration, StartDelay: activityLongDuration}
		info := newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.Equal(t, info.GetExecutionTime().AsTime(), info.GetNextAttemptScheduleTime().AsTime(),
			"during a start delay, NextAttemptScheduleTime is the pending dispatch time (schedule+delay)")
		require.Nil(t, info.GetCurrentRetryInterval(), "the first attempt is not a retry")
	})

	// First attempt running: no pending next dispatch, and no retry interval reported while running.
	t.Run("FirstAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}, []model.Event{model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  1,
			})
	})

	// Backing off before the retry is dispatched: both the interval and the next-attempt schedule time
	// are populated.
	t.Run("BackingOff", func(t *testing.T) {
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
	t.Run("NextRetryDelayOverride", func(t *testing.T) {
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
	t.Run("RetryDue", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:  2,
			})
	})

	// Retry attempt running with a further retry still permitted (max 3): nothing pending while running.
	t.Run("RetryAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  2,
			})
	})

	// Final attempt running with no retry remaining (max 2): still nothing pending while running.
	t.Run("FinalAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 2, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  2,
			})
	})

	// Paused while still backing off: dispatch will not occur while paused, so neither the interval nor
	// the next-attempt schedule time should be reported.
	t.Run("PausedBeforeDispatch", func(t *testing.T) {
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
	t.Run("PausedAfterDispatch", func(t *testing.T) {
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

// TestParityStartToCloseTimeout ports a slice of Test_ActivityTimeouts: a started attempt exceeds its
// StartToClose timeout and, with no retries left, the activity ends TIMED_OUT with the StartToClose
// TimeoutType.
//
// The failure message differs by construction — SAA carries a proto message, WFA's SDK TimeoutError
// formats its own — so TimeoutType is the shared discriminant.
func (s *activityParityTestSuite) TestParityStartToCloseTimeout() {
	t := s.T()
	env := newActivityParityEnv(t)
	trace := []model.Event{model.Poll, model.StartToCloseElapses}
	expected := activityTerminalProjection{
		Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String(),
		RetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
	}

	cfg := activityConfig{MaxAttempts: 1}

	t.Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestParityScheduleToCloseTimeout ports the schedule-to-close slice of Test_ActivityTimeouts: the
// activity is started, then its ScheduleToClose deadline elapses while it runs, so it ends TIMED_OUT
// with the ScheduleToClose TimeoutType. The trace polls first because a never-started activity that
// hits the deadline times out as ScheduleToStart instead, in both implementations.
func (s *activityParityTestSuite) TestParityScheduleToCloseTimeout() {
	t := s.T()
	env := newActivityParityEnv(t)
	trace := []model.Event{model.Poll, {Type: model.ScheduleToCloseElapsesType}}
	expected := activityTerminalProjection{
		Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
		RetryState: enumspb.RETRY_STATE_TIMEOUT,
	}

	cfg := activityConfig{MaxAttempts: 1}

	t.Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestParityScheduleToStartDeadlineWhilePaused: schedule-to-start bounds how long an attempt may wait
// for a worker to pick it up, and a pause is the server declining to offer it to any worker, so the
// paused interval is not time the activity spent waiting on workers and must not consume that
// deadline. Both implementations suppress it, so this is a parity claim rather than a survey entry —
// the disparities in this family are the two per-attempt clocks that run while an attempt is in flight
// (P01, P02) and schedule-to-close, which bounds the activity rather than one attempt's wait (P03).
func (s *activityParityTestSuite) TestParityScheduleToStartDeadlineWhilePaused() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration, ScheduleToStart: activityShortTimeout}
	paused := activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 1}

	parityDrive(s.T(), env, cfg, []model.Event{model.Pause},
		func(t *testing.T, a parityActivity) {
			// Waiting is the point: nothing observable happens when a deadline is correctly ignored, so
			// the test has to outlive the deadline before it can assert the activity survived it.
			time.Sleep(cfg.ScheduleToStart + activityShortTimeout) //nolint:forbidigo
			require.Equal(t, paused, a.activityInfo(t),
				"a paused activity must outlive a schedule-to-start deadline it was not permitted to wait on")
			// The dispatch must also still be there to release, which the run state alone does not say.
			a.driveEvent(t, model.Unpause)
			a.driveEvent(t, model.Poll)
		})
}

// TestParityTimeoutPreservesUnderlyingFailureCause ports TestTimeoutPreservesUnderlyingFailureCause:
// when a timeout closes an activity whose retries were driven by an application failure, the terminal
// TimedOut failure must chain that application failure as its Cause, so an SDK can expose the real
// failure. See mutable_state_impl.go AddActivityTaskTimedOutEvent and temporalio/temporal#3667.
func (s *activityParityTestSuite) TestParityTimeoutPreservesUnderlyingFailureCause() {
	t := s.T()
	env := newActivityParityEnv(t)

	// The application failure driven on attempt 1; see activityFailure. The terminal timeout must chain it
	// verbatim, both Type and Message.
	wantCause := failureCause{Type: "TestFailure", Message: "test failure"}

	// assertCausePreserved drives the trace in both implementations and asserts each ends TIMED_OUT with the given
	// timeout type and retry state, chaining wantCause.
	assertCausePreserved := func(
		t *testing.T, cfg activityConfig, trace []model.Event, timeoutType enumspb.TimeoutType, retryState enumspb.RetryState,
	) {
		expected := activityTerminalProjection{
			Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: timeoutType.String(), RetryState: retryState,
		}
		const chained = "the terminal timeout must chain the underlying application failure as its Cause"
		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, expected, a.terminal(t))
			require.Equal(t, wantCause, a.terminalCause(t), chained)
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, expected, a.terminal(t))
			require.Equal(t, wantCause, a.terminalCause(t), chained)
		})
	}

	// Retries exhausted by a StartToClose timeout on the final attempt (attempt 1 failed retryably).
	t.Run("StartToClose", func(t *testing.T) {
		assertCausePreserved(t, activityConfig{MaxAttempts: 2},
			[]model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.StartToCloseElapses},
			enumspb.TIMEOUT_TYPE_START_TO_CLOSE, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED)
	})
	// Retries exhausted by a Heartbeat timeout on the final attempt: the attempt starts but never
	// heartbeats. A distinct code path that must chain the same cause.
	t.Run("Heartbeat", func(t *testing.T) {
		assertCausePreserved(t, activityConfig{MaxAttempts: 2},
			[]model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.HeartbeatElapses},
			enumspb.TIMEOUT_TYPE_HEARTBEAT, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED)
	})
	// Schedule-to-close deadline closes the activity while it backs off to retry. A third code path.
	t.Run("ScheduleToClose", func(t *testing.T) {
		assertCausePreserved(t, activityConfig{},
			[]model.Event{model.Poll, model.FailRetryably, model.ScheduleToCloseElapses},
			enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, enumspb.RETRY_STATE_TIMEOUT)
	})
}

// TestParityTimeoutTypeOnInsufficientTimeForRetry ports the HeartbeatWithScheduleToClose slice of
// Test_ActivityTimeouts: a heartbeat timeout fires on a started attempt, but the retry interval cannot
// fit before the schedule-to-close deadline, so retries are given up and the terminal timeout is
// reported as ScheduleToClose rather than Heartbeat.
func (s *activityParityTestSuite) TestParityTimeoutTypeOnInsufficientTimeForRetry() {
	t := s.T()
	env := newActivityParityEnv(t)
	trace := []model.Event{model.Poll, {Type: model.HeartbeatElapsesType}}
	// Heartbeat fires at ~2s; the 30s retry cannot fit before the 10s schedule-to-close deadline.
	const retryInterval, scheduleToClose = 30 * time.Second, 10 * time.Second
	expected := activityTerminalProjection{
		Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
		RetryState: enumspb.RETRY_STATE_TIMEOUT,
	}

	cfg := activityConfig{
		MaxAttempts: 2, RetryInterval: retryInterval, ScheduleToClose: scheduleToClose,
	}

	t.Run("WorkflowActivity", func(t *testing.T) {
		// A workflow activity stops reporting the timeout that ended its attempt the moment it closes:
		// the terminal error carries ScheduleToClose and no cause. The standalone implementation keeps the
		// attempt's failure, so only it can drive HeartbeatElapses here.
		t.Skip("a closed workflow activity does not report the timeout that ended its attempt")
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestParityBackoffCoefficient: with a coefficient above 1 each retry waits longer than the last. The
// interval for attempt N is InitialInterval * coefficient^(N-2), so the first backoff is the initial
// interval and the second is that times the coefficient. Observed during the second backoff, before it
// dispatches.
func (s *activityParityTestSuite) TestParityBackoffCoefficient() {
	t := s.T()
	env := newActivityParityEnv(t)
	const initialInterval, maxInterval = 5 * time.Second, 30 * time.Second
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.FailRetryably}
	expected := activityInfo{
		RunState:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                    3,
		CurrentRetryInterval:       2 * initialInterval,
		NextAttemptScheduleTimeSet: true,
	}

	cfg := activityConfig{
		MaxAttempts: 4, RetryInterval: initialInterval, BackoffCoefficient: 2.0, MaxRetryInterval: maxInterval,
	}

	// Elapsing the longer second backoff also exercises the driver's wait, which must take its deadline
	// from the server rather than from the configured interval.
	t.Run("WorkflowActivity", func(t *testing.T) {
		a := newWFADriver(t, env, cfg).driveTrace(t, trace)
		require.Equal(t, expected, a.activityInfo(t))
		a.driveEvent(t, model.BackoffElapses)
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		a := newSAADriver(t, env, cfg).driveTrace(t, trace)
		require.Equal(t, expected, a.activityInfo(t))
		a.driveEvent(t, model.BackoffElapses)
	})
}

var heartbeatWant = firstPayloadData(activityRecordedHeartbeatDetails)

// TestParityHeartbeat ports the core of TestActivityHeartBeatWorkflow_Success: a worker polls the
// activity and heartbeats a checkpoint payload, the checkpoint is readable while it runs, then the
// worker completes it.
func (s *activityParityTestSuite) TestParityHeartbeat() {
	t := s.T()
	env := newActivityParityEnv(t)
	trace := []model.Event{model.Poll, {Type: model.HeartbeatType}}
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}

	t.Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		a := d.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Complete)
		require.Equal(t, expected, a.terminal(t))
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		a := d.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Complete)
		require.Equal(t, expected, a.terminal(t))
	})
}

// TestParityHeartbeatTimeout ports the core of TestActivityHeartBeatWorkflow_Timeout: a started attempt
// heartbeats nothing within its HeartbeatTimeout and, with no retries left, ends TIMED_OUT with the
// Heartbeat TimeoutType.
func (s *activityParityTestSuite) TestParityHeartbeatTimeout() {
	t := s.T()
	env := newActivityParityEnv(t)
	trace := []model.Event{model.Poll, {Type: model.HeartbeatElapsesType}}
	expected := activityTerminalProjection{
		Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_HEARTBEAT.String(),
		RetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
	}

	cfg := activityConfig{MaxAttempts: 1}

	t.Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestParityRetry: an attempt fails retryably, the backoff elapses, the next attempt fails
// non-retryably, and the activity ends FAILED with the application failure type.
func (s *activityParityTestSuite) TestParityRetry() {
	t := s.T()
	env := newActivityParityEnv(t)
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.FailNonRetryably}
	expected := activityTerminalProjection{
		Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, FailureType: "TestFailure",
		RetryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
	}

	t.Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		require.Equal(t, expected, d.driveTrace(t, trace).terminal(t))
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		require.Equal(t, expected, d.driveTrace(t, trace).terminal(t))
	})
}

// TestParityCompleteAfterRetry: attempt 1 fails retryably, the backoff elapses, and attempt 2
// completes. The counterpart of TestWFASAARetry, which ends in a non-retryable failure.
func (s *activityParityTestSuite) TestParityCompleteAfterRetry() {
	t := s.T()
	env := newActivityParityEnv(t)
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.Complete}
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}

	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow}

	t.Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestParityCancel ports the core of TestTryActivityCancellationFromWorkflow: a running activity is
// cancel-requested, the worker acknowledges with RespondActivityTaskCanceled, and the activity ends
// CANCELED. The RequestCancel event realizes differently in each implementation — SAA's direct
// RequestCancelActivityExecution RPC vs WFA's signal-then-RequestCancelActivity — which the drivers
// hide.
func (s *activityParityTestSuite) TestParityCancel() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.RequestCancel, {Type: model.RespondCanceledType}}
	cfg := activityConfig{MaxAttempts: 1}
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

func (s *activityParityTestSuite) TestRespondCanceledWithoutRequest() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 1}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		handle := newWFADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
		assertActivityTaskNotCancelRequested(t, handle.rpc(t, model.RespondCanceled))
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		handle := newSAADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
		assertActivityTaskNotCancelRequested(t, handle.rpc(t, model.RespondCanceled))
	})
}

func (s *activityParityTestSuite) TestRespondCanceledByIDWithoutRequest() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 1}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		handle := newWFADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
		assertActivityTaskNotCancelRequested(t, handle.respondCanceledByID())
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		handle := newSAADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
		assertActivityTaskNotCancelRequested(t, handle.respondCanceledByID())
	})
}

// Unpausing an activity that was never paused must be rejected with FailedPrecondition on both
// surfaces. Workflow activities are served by the legacy unpause path, which silently skips a
// non-paused activity and reports success.
func (s *activityParityTestSuite) TestUnpauseWithoutPause() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 1}

	assertUnpauseRejected := func(t testing.TB, err error) {
		var failedPreconditionErr *serviceerror.FailedPrecondition
		require.ErrorAs(t, err, &failedPreconditionErr)
	}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		t.Skip("WFA should reject unpause on non-paused activity as FailedPrecondition")
		handle := newWFADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
		assertUnpauseRejected(t, handle.rpc(t, model.Unpause))
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		handle := newSAADriver(t, env, cfg).driveTrace(t, []model.Event{model.Poll})
		assertUnpauseRejected(t, handle.rpc(t, model.Unpause))
	})
}

// Force-completing an activity by ID must work whenever no attempt is in progress: while Scheduled
// and never started, and while Paused before any worker picked it up.
func (s *activityParityTestSuite) TestCompleteByID() {
	type testCase struct {
		name  string
		trace []model.Event
	}
	cfg := activityConfig{MaxAttempts: 1}
	testCases := []testCase{
		{
			name:  "BeforeStarted",
			trace: []model.Event{model.CompleteByID},
		},
		{
			name:  "WhilePaused",
			trace: []model.Event{model.Pause, model.CompleteByID},
		},
	}
	env := newActivityParityEnv(s.T())
	for _, tc := range testCases {
		s.Run(tc.name, func(s *activityParityTestSuite) {
			s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
				t := s.T()
				require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, newWFADriver(t, env, cfg).driveTrace(t, tc.trace).terminal(t).Status)
			})
			s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
				t := s.T()
				a := newSAADriver(t, env, cfg).driveTrace(t, tc.trace)
				require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, a.terminal(t).Status)
				require.NotNil(t, a.describe(t).GetInfo().GetLastStartedTime(),
					"a force-completed activity must still record a started time, even though no worker ever started it")
			})
		})
	}
}

// When a worker fails an attempt it can send a final checkpoint payload to be stored as the last
// heartbeat details. This must be persisted for a retryable failure.
func (s *activityParityTestSuite) TestLastHeartbeatDetailsPersistedOnAttemptFailure() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 2}

	expected := activityMarshalPayloads(activityHeartbeatDetails)
	for _, eventType := range []model.EventType{model.RespondFailedType, model.RespondFailedByIDType} {
		s.Run(eventType.String(), func(s *activityParityTestSuite) {
			trace := []model.Event{model.Poll, {Type: eventType, Failure: &model.Failure{}, HasHeartbeatDetails: true}}
			s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
				t := s.T()
				require.Equal(t, expected,
					newWFADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t).LastHeartbeatDetails)
			})
			s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
				t := s.T()
				require.Equal(t, expected,
					newSAADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t).LastHeartbeatDetails)
			})
		})
	}
}

// Reset rewinds the attempt counter but carries the heartbeat checkpoint into the new attempt, so a
// long-running activity resumes from where it got to. reset_heartbeat opts out of that: the
// checkpoint is discarded, and the new attempt starts with none.
func (s *activityParityTestSuite) TestResetHeartbeatDetails() {
	env := newActivityParityEnv(s.T())
	// A long retry interval holds the activity in backoff, so a reset lands while it is SCHEDULED and
	// no dispatched attempt can pick the checkpoint up before it is read.
	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
	recorded := activityMarshalPayloads(activityRecordedHeartbeatDetails)

	for _, tc := range []struct {
		name                     string
		resetEvent               model.Event
		expectedHeartbeatDetails []byte
	}{
		{name: "Keep", resetEvent: model.Reset, expectedHeartbeatDetails: recorded},
		{name: "Clear", resetEvent: model.ResetClearingHeartbeat, expectedHeartbeatDetails: nil},
	} {
		s.Run(tc.name, func(s *activityParityTestSuite) {
			// Reset with no attempt in progress: takes effect at once.
			s.Run("WhileScheduled", func(s *activityParityTestSuite) {
				trace := []model.Event{model.Poll, model.Heartbeat, model.FailRetryably, tc.resetEvent}
				s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
					t := s.T()
					require.Equal(t, tc.expectedHeartbeatDetails,
						newWFADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t).LastHeartbeatDetails)
				})
				s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
					t := s.T()
					require.Equal(t, tc.expectedHeartbeatDetails,
						newSAADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t).LastHeartbeatDetails)
				})
			})
			// Reset while a worker owns the attempt: deferred until that worker yields
			s.Run("WhileStarted", func(s *activityParityTestSuite) {
				trace := []model.Event{model.Poll, model.Heartbeat, tc.resetEvent}
				s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
					t := s.T()
					t.Skip("WFA Reset currently has known differences from SAA Reset")
					handle := newWFADriver(t, env, cfg).driveTrace(t, trace)
					require.Equal(t, recorded, handle.activityInfo(t).LastHeartbeatDetails,
						"the running attempt must keep reporting its checkpoint until it yields")
					handle.driveEvent(t, model.FailRetryably)
					require.Equal(t, tc.expectedHeartbeatDetails, handle.activityInfo(t).LastHeartbeatDetails)
				})
				s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
					t := s.T()
					handle := newSAADriver(t, env, cfg).driveTrace(t, trace)
					require.Equal(t, recorded, handle.activityInfo(t).LastHeartbeatDetails,
						"the running attempt must keep reporting its checkpoint until it yields")
					handle.driveEvent(t, model.FailRetryably)
					require.Equal(t, tc.expectedHeartbeatDetails, handle.activityInfo(t).LastHeartbeatDetails)
				})
			})
		})
	}
}

// A retryable failure payload is truncated to MutableStateActivityFailureSizeLimitError,
// but final failure is not.
func (s *activityParityTestSuite) TestRetryableFailureTruncation() {
	env := newActivityParityEnv(s.T())

	assertTruncated := func(t *testing.T, failure *failurepb.Failure) {
		t.Helper()
		require.NotNil(t, failure, "the attempt's failure must be retained for the retry")
		require.Equal(t, common.FailureReasonFailureExceedsLimit, failure.GetMessage())
		require.NotNil(t, failure.GetServerFailureInfo(),
			"the wrapper is a server failure")
		cause := failure.GetCause()
		require.Equal(t, "TestFailure", cause.GetApplicationFailureInfo().GetType(),
			"the actual failure is wrapped as the cause")
		require.LessOrEqual(t, cause.Size(), activityFailureSizeLimit,
			"the wrapped caused must be trucated to the size limit")
		require.NotEmpty(t, cause.GetMessage())
		require.True(t, strings.HasPrefix(activityLargeFailureMessage, cause.GetMessage()),
			"the truncation is at the end of the failure payload")
		require.Less(t, len(cause.GetMessage()), len(activityLargeFailureMessage),
			"the truncation must reduce the failure payload size")
	}

	assertWhole := func(t *testing.T, failure *failurepb.Failure) {
		t.Helper()
		require.NotNil(t, failure)
		require.Nil(t, failure.GetServerFailureInfo(),
			"a final failure is not replaced by the size-limit stand-in")
		require.Equal(t, "TestFailure", failure.GetApplicationFailureInfo().GetType())
		require.Equal(t, activityLargeFailureMessage, failure.GetMessage(),
			"a final failure must not be truncated")
	}

	s.Run("RetryableAttemptFailure", func(s *activityParityTestSuite) {
		t := s.T()
		cfg := activityConfig{MaxAttempts: 2, RetryInterval: activityLongDuration}
		fail := model.Event{Type: model.RespondFailedType, Failure: &model.Failure{LargeMessage: true}}
		trace := []model.Event{model.Poll, fail}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			assertTruncated(t, a.pendingActivityInfo(t).GetLastFailure())
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			assertTruncated(t, a.describe(t).GetInfo().GetLastFailure())
		})
	})

	// Different than TestTerminalRetryState because earlier failure is trucated but not the final.
	s.Run("FinalFailure", func(s *activityParityTestSuite) {
		t := s.T()
		cfg := activityConfig{MaxAttempts: 2}
		fail := model.Event{Type: model.RespondFailedType, Failure: &model.Failure{LargeMessage: true}}
		trace := []model.Event{model.Poll, fail, model.BackoffElapses, model.Poll, fail}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, activityTerminalProjection{
				Status:      enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				FailureType: "TestFailure",
				RetryState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			}, a.terminal(t))
			appErr, ok := errors.AsType[*temporal.ApplicationError](errors.Unwrap(a.run.Get(a.testContext(), nil)))
			require.True(t, ok)
			require.Equal(t, "TestFailure", appErr.Type())
			require.Equal(t, activityLargeFailureMessage, appErr.Message(),
				"a final failure must not be truncated")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, activityTerminalProjection{
				Status:      enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				FailureType: "TestFailure",
				RetryState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			}, a.terminal(t))
			describeResp := a.describe(t)
			assertWhole(t, describeResp.GetOutcome().GetFailure())
			assertWhole(t, describeResp.GetInfo().GetLastFailure())
		})
	})
}

func (s *activityParityTestSuite) TestTerminalRetryState() {
	env := newActivityParityEnv(s.T())

	testCases := []struct {
		name     string
		cfg      activityConfig
		trace    []model.Event
		expected activityTerminalProjection
	}{
		// Terminal status FAILED
		{
			// Terminal status FAILED; there were no more retries due to non-retryable failure from
			// worker, despite a second attempt being available.
			name:  "NonRetryableFailure",
			cfg:   activityConfig{MaxAttempts: 2},
			trace: []model.Event{model.Poll, model.FailNonRetryably},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				RetryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
			},
		},
		{
			// Terminal status FAILED; there were no more retries due to retries exhausted, despite
			// retryable failure from worker.
			name:  "MaximumAttemptsReachedAfterFailure",
			cfg:   activityConfig{MaxAttempts: 1},
			trace: []model.Event{model.Poll, model.FailRetryably},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				RetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			},
		},
		{
			// Terminal status FAILED; there were no more retries due to timeout: the retry backoff
			// would run past the schedule-to-close deadline, so server times it out without
			// waiting.
			name: "FailureRetryPreventedByScheduleToClose",
			cfg: activityConfig{
				RetryInterval:   activityLongDuration,
				ScheduleToClose: time.Hour,
			},
			trace: []model.Event{model.Poll, model.FailRetryably},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				RetryState: enumspb.RETRY_STATE_TIMEOUT,
			},
		},
		{
			// As FailureRetryPreventedByScheduleToClose, except that it is the worker's
			// next_retry_delay rather than the policy's interval that would run past the
			// schedule-to-close deadline: the policy's interval on its own leaves ample time.
			name: "NextRetryDelayRetryPreventedByScheduleToClose",
			cfg: activityConfig{
				NextRetryDelay:  activityLongDuration,
				ScheduleToClose: time.Hour,
			},
			trace: []model.Event{model.Poll, model.FailRetryably},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				RetryState: enumspb.RETRY_STATE_TIMEOUT,
			},
		},
		{
			// Terminal status FAILED; there were no more retries due to cancel-requested; retries
			// are also exhausted, but cancellation has precedence as the reason.
			name:  "CancelRequestedBeforeFailure",
			cfg:   activityConfig{MaxAttempts: 1},
			trace: []model.Event{model.Poll, model.RequestCancel, model.FailRetryably},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				RetryState: enumspb.RETRY_STATE_CANCEL_REQUESTED,
			},
		},
		// Terminal status TIMED_OUT
		{
			// Terminal status TIMED_OUT; there were no more retries due to timeout, because no
			// worker polled hence schedule-to-start timeout.
			name:  "ScheduleToStartTimeout",
			trace: []model.Event{model.ScheduleToStartElapses},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				RetryState: enumspb.RETRY_STATE_TIMEOUT,
			},
		},
		{
			// Terminal status TIMED_OUT; there were no more retries due to timeout, because worker
			// held on to attempt until schedule-to-close fired.
			name:  "ScheduleToCloseTimeout",
			trace: []model.Event{model.Poll, model.ScheduleToCloseElapses},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				RetryState: enumspb.RETRY_STATE_TIMEOUT,
			},
		},
		{
			// Terminal status TIMED_OUT; there were no more retries due to retries exhausted,
			// despite the start-to-close timeout being retryable.
			name:  "MaximumAttemptsReachedAfterAttemptTimeout",
			cfg:   activityConfig{MaxAttempts: 1},
			trace: []model.Event{model.Poll, model.StartToCloseElapses},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				RetryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			},
		},
		{
			// Similar to FailureRetryPreventedByScheduleToClose, but we cause it to terminate in
			// TIMED_OUT by letting start-to-close elapse (we can't use an explicit
			// model.StartToCloseElapses in the trace because the two drivers see it differently)
			name: "AttemptTimeoutRetryPreventedByScheduleToClose",
			cfg: activityConfig{
				RetryInterval:   activityLongDuration,
				StartToClose:    activityShortTimeout,
				ScheduleToClose: time.Hour,
			},
			trace: []model.Event{model.Poll},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				RetryState: enumspb.RETRY_STATE_TIMEOUT,
			},
		},
		{
			// Terminal status TIMED_OUT; there were no more retries due to cancellation pending
			// when the start-to-close deadline elapsed. Retries are also exhausted, but
			// cancellation has precedence as the reason.
			name:  "CancelRequestedBeforeAttemptTimeout",
			cfg:   activityConfig{MaxAttempts: 1},
			trace: []model.Event{model.Poll, model.RequestCancel, model.StartToCloseElapses},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				RetryState: enumspb.RETRY_STATE_CANCEL_REQUESTED,
			},
		},
		{
			// Terminal status TIMED_OUT; there were no more retries due to cancellation pending
			// when the schedule-to-close deadline elapses. No MaxAttempts is needed here, unlike
			// the previous case (CancelRequestedBeforeAttemptTimeout): a schedule-to-close timeout
			// closes the activity without consulting the retry policy at all.
			name:  "CancelRequestedBeforeScheduleToCloseTimeout",
			trace: []model.Event{model.Poll, model.RequestCancel, model.ScheduleToCloseElapses},
			expected: activityTerminalProjection{
				Status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				RetryState: enumspb.RETRY_STATE_CANCEL_REQUESTED,
			},
		},
	}

	// This test is about how an activity stops retrying, so it compares the status and the retry state
	// only. The failure discriminant of each terminal state is the subject of the timeout and failure
	// parity tests above.
	assertRetryState := func(t *testing.T, expected activityTerminalProjection, got activityTerminalProjection) {
		require.Equal(t, expected, activityTerminalProjection{Status: got.Status, RetryState: got.RetryState})
	}

	for _, tc := range testCases {
		s.Run(tc.name, func(s *activityParityTestSuite) {
			t := s.T()
			t.Run("WorkflowActivity", func(t *testing.T) {
				assertRetryState(t, tc.expected, newWFADriver(t, env, tc.cfg).driveTrace(t, tc.trace).terminal(t))
			})
			t.Run("StandaloneActivity", func(t *testing.T) {
				assertRetryState(t, tc.expected, newSAADriver(t, env, tc.cfg).driveTrace(t, tc.trace).terminal(t))
			})
		})
	}
}

// TestPendingResetIsInertUntilTheAttemptEnds holds the claims about a reset that a worker's attempt is
// holding back which are true of both surfaces, and so have no entry in the WFA/SAA parity survey: while
// the attempt runs the pending reset is invisible and dispatches nothing, and it does not stand between
// the activity and its schedule-to-close deadline or a cancellation.
func (s *activityParityTestSuite) TestPendingResetIsInertUntilTheAttemptEnds() {
	t := s.T()
	env := newActivityParityEnv(t)
	cfg := activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration}

	// The attempt keeps its identity, no second task is dispatched, and the worker learns of the reset
	// only by heartbeating.
	t.Run("WhileTheAttemptRuns", func(t *testing.T) {
		parityDrive(t, env, cfg, []model.Event{model.Poll, model.Reset},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1},
					a.activityInfo(t), "a deferred reset must not disturb the attempt it will supersede")
				require.Nil(t, a.pollForTask(t, activityDriverTimeout),
					"the reset attempt must not be dispatched while a worker still owns the attempt it supersedes")
				a.driveEvent(t, model.Heartbeat)
				require.Equal(t, model.HeartbeatFlags{ActivityReset: true}, a.heartbeatFlags(),
					"a heartbeat during a pending reset must tell the worker its attempt has been superseded")
			})
	})

	// The schedule-to-close deadline is the user's absolute bound on the activity, and a pending reset
	// does not extend it.
	t.Run("ScheduleToCloseDeadlinePasses", func(t *testing.T) {
		parityDrive(t, env, cfg, []model.Event{model.Poll, model.Reset, model.ScheduleToCloseElapses},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityTerminalProjection{
					Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
					FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
					RetryState:  enumspb.RETRY_STATE_TIMEOUT,
				}, a.terminal(t))
			})
	})

	// Cancellation supersedes the pending reset: the worker's acknowledgement closes the activity rather
	// than starting it over.
	t.Run("CancellationIsAcknowledged", func(t *testing.T) {
		parityDrive(t, env, cfg,
			[]model.Event{model.Poll, model.Reset, model.RequestCancel, model.RespondCanceled},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED},
					a.terminal(t))
			})
	})
}

// TestResetSubstitutesForUnpauseFlags asks whether Reset covers what the unpause reset_attempts flag
// covered. An activity is paused part-way through its retry budget and then reset rather than
// unpaused; it must come back on attempt 1, dispatchable, with no retry backoff left to wait out.
// Reset also clears the heartbeat details, covering the other flag, which this projection does not
// observe.
func (s *activityParityTestSuite) TestResetSubstitutesForUnpauseFlags() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.FailRetryably, model.Pause, model.Reset}
	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
	expected := activityInfo{
		RunState:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                    1,
		CurrentRetryInterval:       0,
		NextAttemptScheduleTimeSet: false,
	}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t))
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).activityInfo(t))
	})
}
