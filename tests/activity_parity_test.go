package tests

// SAA <-> WFA parity tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/testing/parallelsuite"
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
	cluster.OverrideDynamicConfig(t, dynamicconfig.EnableChasm, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.Enabled, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.StartDelayEnabled, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.EnableStandaloneActivityOperatorCommands, nsValues(true))
	return env
}

// nextRetryDelayOverride is a worker-supplied next_retry_delay, distinct from
// activityLongDuration so the reported interval cannot be confused with the policy's.
const nextRetryDelayOverride = 10 * time.Second

// A StartToClose or Heartbeat timeout whose type is listed in the retry policy's NonRetryableErrorTypes
// must fail the activity terminally (TimedOut) when it fires, rather than retrying.
func (s *activityParityTestSuite) TestParityNonRetryableErrorTypes() {
	env := newActivityParityEnv(s.T())

	testTimeoutWhileAttemptInProgress := func(t *testing.T, timeout model.Event) {
		trace := []model.Event{model.Poll, timeout}
		cfg := activityConfig{
			MaxAttempts:            3,
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

	s.T().Run("StartToClose", func(t *testing.T) {
		testTimeoutWhileAttemptInProgress(t, model.StartToCloseElapses)
	})
	s.T().Run("Heartbeat", func(t *testing.T) {
		testTimeoutWhileAttemptInProgress(t, model.HeartbeatElapses)
	})
}

// current_retry_interval and next_attempt_schedule_time are reported while a retry is backing off
// (before it is dispatched to Matching), and for next_attempt_schedule_time also during start delay
// (SAA only). Once the attempt is dispatched, or while the activity is paused, both are nil.
func (s *activityParityTestSuite) TestParityCurrentRetryIntervalAndNextAttemptScheduleTime() {
	env := newActivityParityEnv(s.T())

	// both drives a trace through both surfaces, asserting each reports expected.
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
	s.T().Run("StartDelayPending", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration, StartDelay: activityLongDuration}
		info := newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.Equal(t, info.GetExecutionTime().AsTime(), info.GetNextAttemptScheduleTime().AsTime(),
			"during a start delay, NextAttemptScheduleTime is the pending dispatch time (schedule+delay)")
		require.Nil(t, info.GetCurrentRetryInterval(), "the first attempt is not a retry")
	})

	// First attempt running: no pending next dispatch, and no retry interval reported while running.
	s.T().Run("FirstAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}, []model.Event{model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  1,
			})
	})

	// Backing off before the retry is dispatched: both the interval and the next-attempt schedule time
	// are populated.
	s.T().Run("BackingOff", func(t *testing.T) {
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
	s.T().Run("NextRetryDelayOverride", func(t *testing.T) {
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
	s.T().Run("RetryDue", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:  2,
			})
	})

	// Retry attempt running with a further retry still permitted (max 3): nothing pending while running.
	s.T().Run("RetryAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  2,
			})
	})

	// Final attempt running with no retry remaining (max 2): still nothing pending while running.
	s.T().Run("FinalAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 2, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  2,
			})
	})

	// Paused while still backing off: dispatch will not occur while paused, so neither the interval nor
	// the next-attempt schedule time should be reported.
	s.T().Run("PausedBeforeDispatch", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}, []model.Event{model.Poll, model.FailRetryably, model.Pause},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED,
				Attempt:  2,
			})
	})

	// Paused after the retry was dispatched: the dispatched code path already nils both fields, and the
	// pause preserves that. No field of ActivityExecutionInfo or PendingActivityInfo distinguishes this
	// from PausedBeforeDispatch on either surface, so the two subtests differ in the state they reach,
	// not in what they assert.
	s.T().Run("PausedAfterDispatch", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortDispatchDelay}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Pause},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED,
				Attempt:  2,
			})
	})
}

// TestParityStartToCloseTimeout ports a slice of Test_ActivityTimeouts: a started attempt exceeds its
// StartToClose timeout and, with no retries left, the activity ends TIMED_OUT with the StartToClose
// TimeoutType.
//
// The failure message differs by construction — SAA carries a proto message, WFA's SDK TimeoutError
// formats its own — so TimeoutType is the cross-surface discriminant.
func (s *activityParityTestSuite) TestParityStartToCloseTimeout() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.StartToCloseElapses}
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()}

	cfg := activityConfig{MaxAttempts: 1}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestParityScheduleToCloseTimeout ports the schedule-to-close slice of Test_ActivityTimeouts: the
// activity is started, then its ScheduleToClose deadline elapses while it runs, so it ends TIMED_OUT
// with the ScheduleToClose TimeoutType. The trace polls first because a never-started activity that
// hits the deadline times out as ScheduleToStart instead, on both surfaces.
func (s *activityParityTestSuite) TestParityScheduleToCloseTimeout() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, {Type: model.ScheduleToCloseElapsesType}}
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()}

	cfg := activityConfig{MaxAttempts: 1}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestParityTimeoutPreservesUnderlyingFailureCause ports TestTimeoutPreservesUnderlyingFailureCause:
// when a timeout closes an activity whose retries were driven by an application failure, the terminal
// TimedOut failure must chain that application failure as its Cause, so an SDK can surface the real
// failure. See mutable_state_impl.go AddActivityTaskTimedOutEvent and temporalio/temporal#3667.
func (s *activityParityTestSuite) TestParityTimeoutPreservesUnderlyingFailureCause() {
	env := newActivityParityEnv(s.T())

	// The application failure driven on attempt 1; see activityFailure. The terminal timeout must chain it
	// verbatim, both Type and Message.
	wantCause := failureCause{Type: "drive", Message: "drive"}

	// assertCausePreserved drives the trace on both surfaces and asserts each ends TIMED_OUT with the given
	// timeout type, chaining wantCause.
	assertCausePreserved := func(t *testing.T, cfg activityConfig, trace []model.Event, timeoutType enumspb.TimeoutType) {
		expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: timeoutType.String()}
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
	s.T().Run("StartToClose", func(t *testing.T) {
		assertCausePreserved(t, activityConfig{MaxAttempts: 2},
			[]model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.StartToCloseElapses}, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	})
	// Retries exhausted by a Heartbeat timeout on the final attempt: the attempt starts but never
	// heartbeats. A distinct code path that must chain the same cause.
	s.T().Run("Heartbeat", func(t *testing.T) {
		assertCausePreserved(t, activityConfig{MaxAttempts: 2},
			[]model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.HeartbeatElapses}, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	})
	// Schedule-to-close deadline closes the activity while it backs off to retry. A third code path.
	s.T().Run("ScheduleToClose", func(t *testing.T) {
		assertCausePreserved(t, activityConfig{},
			[]model.Event{model.Poll, model.FailRetryably, model.ScheduleToCloseElapses}, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
	})
}

// TestParityTimeoutTypeOnInsufficientTimeForRetry ports the HeartbeatWithScheduleToClose slice of
// Test_ActivityTimeouts: a heartbeat timeout fires on a started attempt, but the retry interval cannot
// fit before the schedule-to-close deadline, so retries are given up and the terminal timeout is
// reported as ScheduleToClose rather than Heartbeat.
func (s *activityParityTestSuite) TestParityTimeoutTypeOnInsufficientTimeForRetry() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, {Type: model.HeartbeatElapsesType}}
	// Heartbeat fires at ~2s; the 30s retry cannot fit before the 10s schedule-to-close deadline.
	const retryInterval, scheduleToClose = 30 * time.Second, 10 * time.Second
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()}

	cfg := activityConfig{
		MaxAttempts: 2, RetryInterval: retryInterval, ScheduleToClose: scheduleToClose,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		// A workflow activity stops reporting the timeout that ended its attempt the moment it closes:
		// the terminal error carries ScheduleToClose and no cause. The standalone surface keeps the
		// attempt's failure, so only it can drive HeartbeatElapses here.
		t.Skip("a closed workflow activity does not report the timeout that ended its attempt")
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestParityBackoffCoefficient: with a coefficient above 1 each retry waits longer than the last. The
// interval for attempt N is InitialInterval * coefficient^(N-2), so the first backoff is the initial
// interval and the second is that times the coefficient. Observed during the second backoff, before it
// dispatches.
func (s *activityParityTestSuite) TestParityBackoffCoefficient() {
	env := newActivityParityEnv(s.T())
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
	s.T().Run("WorkflowActivity", func(t *testing.T) {
		a := newWFADriver(t, env, cfg).driveTrace(t, trace)
		require.Equal(t, expected, a.activityInfo(t))
		a.driveEvent(t, model.BackoffElapses)
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		a := newSAADriver(t, env, cfg).driveTrace(t, trace)
		require.Equal(t, expected, a.activityInfo(t))
		a.driveEvent(t, model.BackoffElapses)
	})
}

var heartbeatWant = []byte(`"hb"`) // == activityHeartbeatDetails

// TestParityHeartbeat ports the core of TestActivityHeartBeatWorkflow_Success: a worker polls the
// activity and heartbeats a checkpoint payload, the checkpoint is readable while it runs, then the
// worker completes it.
func (s *activityParityTestSuite) TestParityHeartbeat() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, {Type: model.HeartbeatType}}
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		a := d.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Complete)
		require.Equal(t, expected, a.terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
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
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, {Type: model.HeartbeatElapsesType}}
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_HEARTBEAT.String()}

	cfg := activityConfig{MaxAttempts: 1}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestParityRetry: an attempt fails retryably, the backoff elapses, the next attempt fails
// non-retryably, and the activity ends FAILED with the application failure type.
func (s *activityParityTestSuite) TestParityRetry() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.FailNonRetryably}
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, FailureType: "drive"}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		require.Equal(t, expected, d.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		d := newSAADriver(t, env, activityConfig{MaxAttempts: 3, RetryInterval: 2 * time.Second})
		require.Equal(t, expected, d.driveTrace(t, trace).terminal(t))
	})
}

// TestParityCompleteAfterRetry: attempt 1 fails retryably, the backoff elapses, and attempt 2
// completes. The counterpart of TestWFASAARetry, which ends in a non-retryable failure.
func (s *activityParityTestSuite) TestParityCompleteAfterRetry() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.Complete}
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}

	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}

// TestParityCancel ports the core of TestTryActivityCancellationFromWorkflow: a running activity is
// cancel-requested, the worker acknowledges with RespondActivityTaskCanceled, and the activity ends
// CANCELED. The RequestCancel event realizes differently per surface — SAA's direct
// RequestCancelActivityExecution RPC vs WFA's signal-then-RequestCancelActivity — which the drivers
// hide.
func (s *activityParityTestSuite) TestParityCancel() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll, model.RequestCancel, {Type: model.RespondCanceledType}}
	expected := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}

	cfg := activityConfig{MaxAttempts: 1}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t))
	})
}
