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

const (
	// backingOffInterval is long enough to observe an activity while it is still backing off.
	backingOffInterval = 30 * time.Second
	// dispatchInterval is short enough that the backoff elapses and the retry dispatches within the test.
	dispatchInterval = 1 * time.Second
	// nextRetryDelayOverride is a worker-supplied next_retry_delay, distinct from backingOffInterval so
	// the reported interval cannot be confused with the policy's.
	nextRetryDelayOverride = 10 * time.Second
	// startDelay keeps a first attempt pending dispatch for the whole test.
	startDelay = time.Hour
)

// A StartToClose or Heartbeat timeout whose type is listed in the retry policy's NonRetryableErrorTypes
// must fail the activity terminally (TimedOut) when it fires, rather than retrying.
func (s *activityParityTestSuite) TestParityNonRetryableTimeout() {
	env := newActivityParityEnv(s.T())

	both := func(t *testing.T, cfg activityConfig, elapses model.Event, timeoutType enumspb.TimeoutType) {
		trace := []model.Event{model.Poll, elapses}
		cfg.MaxAttempts = 3
		cfg.NonRetryableErrorTypes = []string{retrypolicy.TimeoutFailureTypePrefix + timeoutType.String()}

		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				newWFADriver(t, env, cfg).driveTrace(t, trace).terminal(t).Status,
				"a non-retryable timeout must fail the activity terminally, not retry it")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				newSAADriver(t, env, cfg).driveTrace(t, trace).terminal(t).Status,
				"a non-retryable timeout must fail the activity terminally, not retry it")
		})
	}

	s.T().Run("StartToClose", func(t *testing.T) {
		both(t, activityConfig{StartToClose: activityShortTimeout}, model.StartToCloseElapses, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	})
	s.T().Run("Heartbeat", func(t *testing.T) {
		both(t, activityConfig{Heartbeat: activityShortTimeout}, model.HeartbeatElapses, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	})
}

// current_retry_interval and next_attempt_schedule_time are reported while a retry is backing off
// (before it is dispatched to Matching), and for next_attempt_schedule_time also during start delay
// (SAA only). Once the attempt is dispatched, or while the activity is paused, both are nil.
func (s *activityParityTestSuite) TestParityCurrentRetryInterval() {
	env := newActivityParityEnv(s.T())

	// both drives a trace through both surfaces, asserting each reports expected.
	both := func(t *testing.T, cfg activityConfig, trace []model.Event, expected activityInfoProjection) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).projection(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).projection(t))
		})
	}

	// First attempt within its start delay (SAA only): the pending dispatch is in the future and is
	// not a retry.
	s.T().Run("StartDelayPending", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 3, RetryInterval: backingOffInterval, StartDelay: startDelay}
		info := newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.Equal(t, info.GetExecutionTime().AsTime(), info.GetNextAttemptScheduleTime().AsTime(),
			"during a start delay, NextAttemptScheduleTime is the pending dispatch time (schedule+delay)")
		require.Nil(t, info.GetCurrentRetryInterval(), "the first attempt is not a retry")
	})

	// First attempt running: no pending next dispatch, and no retry interval reported while running.
	s.T().Run("FirstAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: backingOffInterval}, []model.Event{model.Poll},
			activityInfoProjection{
				State:   enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt: 1,
			})
	})

	// Backing off before the retry is dispatched: both the interval and the next-attempt schedule time
	// are populated.
	s.T().Run("BackingOff", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: backingOffInterval}, []model.Event{model.Poll, model.FailRetryably},
			activityInfoProjection{
				State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:                2,
				CurrentRetryInterval:   backingOffInterval,
				NextAttemptScheduleSet: true,
			})
	})

	// Backing off after a worker-supplied next_retry_delay: the reported interval is the worker's
	// override.
	s.T().Run("NextRetryDelayOverride", func(t *testing.T) {
		trace := []model.Event{model.Poll, model.FailRetryably}
		expected := activityInfoProjection{
			State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
			Attempt:                2,
			CurrentRetryInterval:   nextRetryDelayOverride,
			NextAttemptScheduleSet: true,
		}
		cfg := activityConfig{MaxAttempts: 3, RetryInterval: backingOffInterval, NextRetryDelay: nextRetryDelayOverride}
		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equal(t, expected, newWFADriver(t, env, cfg).driveTrace(t, trace).projection(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equal(t, expected, newSAADriver(t, env, cfg).driveTrace(t, trace).projection(t))
		})
	})

	// Retry dispatched to Matching but not yet polled: both fields are nil.
	s.T().Run("RetryDispatched", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: dispatchInterval}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses},
			activityInfoProjection{
				State:   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt: 2,
			})
	})

	// Retry attempt running with a further retry still permitted (max 3): nothing pending while running.
	s.T().Run("RetryAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: dispatchInterval}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfoProjection{
				State:   enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt: 2,
			})
	})

	// Final attempt running with no retry remaining (max 2): still nothing pending while running.
	s.T().Run("FinalAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 2, RetryInterval: dispatchInterval}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfoProjection{
				State:   enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt: 2,
			})
	})

	// Paused while still backing off: dispatch will not occur while paused, so neither the interval nor
	// the next-attempt schedule time should be reported.
	s.T().Run("PausedBeforeDispatch", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: backingOffInterval}, []model.Event{model.Poll, model.FailRetryably, model.Pause},
			activityInfoProjection{
				State:   enumspb.PENDING_ACTIVITY_STATE_PAUSED,
				Attempt: 2,
			})
	})

	// Paused after the retry was dispatched: the dispatched code path already nils both fields, and the
	// pause preserves that.
	s.T().Run("PausedAfterDispatch", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: dispatchInterval}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Pause},
			activityInfoProjection{
				State:   enumspb.PENDING_ACTIVITY_STATE_PAUSED,
				Attempt: 2,
			})
	})
}
