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

func assertActivityTaskNotCancelRequested(t *testing.T, err error) {
	var invalidArgumentErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgumentErr)
	require.Equal(t, consts.ErrActivityTaskNotCancelRequested.Error(), invalidArgumentErr.Message)
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
			require.Equalf(t, activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				retryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
			}, newWFADriver(t, env, cfg).driveTrace(t, trace).terminalOutcome(t),
				"a %s timeout marked non-retryable must fail the activity terminally, not retry it", timeoutType(timeout))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equalf(t, activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				retryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
			}, newSAADriver(t, env, cfg).driveTrace(t, trace).terminalOutcome(t),
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

// A retryable ServerFailure reported through RespondActivityTaskFailedById must be retried, exactly
// like a retryable ApplicationFailure. Only the by-ID API accepts a non-application failure (the
// by-token API rejects one), so this is the path a ServerFailure reaches the handler through.
func (s *activityParityTestSuite) TestRetryableServerFailureIsRetried() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
	trace := []model.Event{model.Poll, model.FailByIDRetryablyWithServerFailure}
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
		name  string
		event model.Event
	}{
		{name: "StartToCloseTimeout", event: model.FailByIDRetryablyWithStartToCloseTimeoutFailure},
		{name: "HeartbeatTimeout", event: model.FailByIDRetryablyWithHeartbeatTimeoutFailure},
		{name: "UnknownFailure", event: model.FailByIDRetryablyWithUnknownFailure},
	}
	wantRetry := activityInfo{
		RunState:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                    2,
		CurrentRetryInterval:       activityLongDuration,
		NextAttemptScheduleTimeSet: true,
	}

	for _, tc := range retryableFailures {
		s.Run(tc.name, func(s *activityParityTestSuite) {
			trace := []model.Event{model.Poll, tc.event}
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
		name  string
		event model.Event
	}{
		{name: "ScheduleToStartTimeout", event: model.FailByIDWithScheduleToStartTimeoutFailure},
		{name: "ScheduleToCloseTimeout", event: model.FailByIDWithScheduleToCloseTimeoutFailure},
	}
	for _, tc := range nonRetryableTimeouts {
		s.Run(tc.name, func(s *activityParityTestSuite) {
			trace := []model.Event{model.Poll, tc.event}
			s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
				t := s.T()
				require.Equal(t, activityTerminalOutcome{
					status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
					retryState: enumspb.RETRY_STATE_TIMEOUT,
				}, newWFADriver(t, env, cfg).driveTrace(t, trace).terminalOutcome(t))
			})
			s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
				t := s.T()
				require.Equal(t, activityTerminalOutcome{
					status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
					retryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
				}, newSAADriver(t, env, cfg).driveTrace(t, trace).terminalOutcome(t))
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
			timeout, ok := errors.AsType[*temporal.TimeoutError](activity.run.Get(activity.d.ctx, nil))
			require.True(t, ok)
			appErr, ok := errors.AsType[*temporal.ApplicationError](timeout.Unwrap())
			require.True(t, ok)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, activity.terminalOutcome(t).status, message)
			require.Equal(t, "TestFailure", appErr.Type(), message)
			require.Equal(t, "test failure", appErr.Message(), message)
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			activity := newSAADriver(t, env, cfg).driveTrace(t, trace)
			cause := activity.describe(t).GetOutcome().GetFailure().GetCause()
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, activity.terminalOutcome(t).status, message)
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
	trace := []model.Event{model.Poll, model.FailWithoutFailure}
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
	trace := []model.Event{model.Poll, model.FailWithoutFailure}

	h := newSAADriver(t, env, cfg).driveTrace(t, trace)
	require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, h.terminalOutcome(t).status)
	require.NotNil(t, h.describe(t).GetOutcome().GetFailure(),
		"a standalone activity that failed without worker-supplied details must still expose a terminal failure")
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
	expected := activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminalOutcome(t))
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminalOutcome(t))
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
				require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, newWFADriver(t, env, cfg).driveTrace(t, tc.trace).terminalOutcome(t).status)
			})
			s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
				t := s.T()
				a := newSAADriver(t, env, cfg).driveTrace(t, tc.trace)
				require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, a.terminalOutcome(t).status)
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
			trace := []model.Event{model.Poll, {Type: eventType, Failure: &model.Failure{Retryable: true}, HasHeartbeatDetails: true}}
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
		fail := model.Event{Type: model.RespondFailedType, Failure: &model.Failure{Retryable: true, LargeMessage: true}}
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
		fail := model.Event{Type: model.RespondFailedType, Failure: &model.Failure{Retryable: true, LargeMessage: true}}
		trace := []model.Event{model.Poll, fail, model.BackoffElapses, model.Poll, fail}

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				retryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			}, a.terminalOutcome(t))
			appErr, ok := errors.AsType[*temporal.ApplicationError](errors.Unwrap(a.run.Get(a.d.ctx, nil)))
			require.True(t, ok)
			require.Equal(t, "TestFailure", appErr.Type())
			require.Equal(t, activityLargeFailureMessage, appErr.Message(),
				"a final failure must not be truncated")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				retryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			}, a.terminalOutcome(t))
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
		expected activityTerminalOutcome
	}{
		// Terminal status FAILED
		{
			// Terminal status FAILED; there were no more retries due to non-retryable failure from
			// worker, despite a second attempt being available.
			name:  "NonRetryableFailure",
			cfg:   activityConfig{MaxAttempts: 2},
			trace: []model.Event{model.Poll, model.FailNonRetryably},
			expected: activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				retryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
			},
		},
		{
			// Terminal status FAILED; there were no more retries due to retries exhausted, despite
			// retryable failure from worker.
			name:  "MaximumAttemptsReachedAfterFailure",
			cfg:   activityConfig{MaxAttempts: 1},
			trace: []model.Event{model.Poll, model.FailRetryably},
			expected: activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				retryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
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
			expected: activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				retryState: enumspb.RETRY_STATE_TIMEOUT,
			},
		},
		{
			// Terminal status FAILED; there were no more retries due to cancel-requested; retries
			// are also exhausted, but cancellation has precedence as the reason.
			name:  "CancelRequestedBeforeFailure",
			cfg:   activityConfig{MaxAttempts: 1},
			trace: []model.Event{model.Poll, model.RequestCancel, model.FailRetryably},
			expected: activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				retryState: enumspb.RETRY_STATE_CANCEL_REQUESTED,
			},
		},
		// Terminal status TIMED_OUT
		{
			// Terminal status TIMED_OUT; there were no more retries due to timeout, because no
			// worker polled hence schedule-to-start timeout.
			name:  "ScheduleToStartTimeout",
			trace: []model.Event{model.ScheduleToStartElapses},
			expected: activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				retryState: enumspb.RETRY_STATE_TIMEOUT,
			},
		},
		{
			// Terminal status TIMED_OUT; there were no more retries due to timeout, because worker
			// held on to attempt until schedule-to-close fired.
			name:  "ScheduleToCloseTimeout",
			trace: []model.Event{model.Poll, model.ScheduleToCloseElapses},
			expected: activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				retryState: enumspb.RETRY_STATE_TIMEOUT,
			},
		},
		{
			// Terminal status TIMED_OUT; there were no more retries due to retries exhausted,
			// despite the start-to-close timeout being retryable.
			name:  "MaximumAttemptsReachedAfterAttemptTimeout",
			cfg:   activityConfig{MaxAttempts: 1},
			trace: []model.Event{model.Poll, model.StartToCloseElapses},
			expected: activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				retryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
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
			expected: activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				retryState: enumspb.RETRY_STATE_TIMEOUT,
			},
		},
		{
			// Terminal status TIMED_OUT; there were no more retries due to cancellation pending
			// when the start-to-close deadline elapsed. Retries are also exhausted, but
			// cancellation has precedence as the reason.
			name:  "CancelRequestedBeforeAttemptTimeout",
			cfg:   activityConfig{MaxAttempts: 1},
			trace: []model.Event{model.Poll, model.RequestCancel, model.StartToCloseElapses},
			expected: activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				retryState: enumspb.RETRY_STATE_CANCEL_REQUESTED,
			},
		},
		{
			// Terminal status TIMED_OUT; there were no more retries due to cancellation pending
			// when the schedule-to-close deadline elapses. No MaxAttempts is needed here, unlike
			// the previous case (CancelRequestedBeforeAttemptTimeout): a schedule-to-close timeout
			// closes the activity without consulting the retry policy at all.
			name:  "CancelRequestedBeforeScheduleToCloseTimeout",
			trace: []model.Event{model.Poll, model.RequestCancel, model.ScheduleToCloseElapses},
			expected: activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				retryState: enumspb.RETRY_STATE_CANCEL_REQUESTED,
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func(s *activityParityTestSuite) {
			t := s.T()
			t.Run("WorkflowActivity", func(t *testing.T) {
				require.Equal(t, tc.expected, newWFADriver(t, env, tc.cfg).driveTrace(t, tc.trace).terminalOutcome(t))
			})
			t.Run("StandaloneActivity", func(t *testing.T) {
				require.Equal(t, tc.expected, newSAADriver(t, env, tc.cfg).driveTrace(t, tc.trace).terminalOutcome(t))
			})
		})
	}
}
