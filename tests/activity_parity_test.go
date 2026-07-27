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

type activityDriverErrorRecorder struct {
	failed bool
}

func (r *activityDriverErrorRecorder) Errorf(string, ...any) {
	r.failed = true
}

func (r *activityDriverErrorRecorder) FailNow() {
	r.failed = true
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
	cluster.OverrideDynamicConfig(t, activity.StartDelayEnabled, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.EnableStandaloneActivityOperatorCommands, nsValues(true))
	return env
}

// nextRetryDelayOverride is a worker-supplied next_retry_delay, distinct from
// activityLongRetryInterval so the reported interval cannot be confused with the policy's.
const nextRetryDelayOverride = 10 * time.Second

// The retry policy's NonRetryableErrorTypes Must be respected. In particular, a StartToClose or
// Heartbeat timeout whose type is listed in the retry policy's NonRetryableErrorTypes using the
// special TemporalTimeout: syntax must fail the activity terminally (TimedOut) when it fires,
// rather than retrying.
func (s *activityParityTestSuite) TestParityNonRetryableErrorTypes() {
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
		cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongRetryInterval, StartDelay: activityLongStartDelay}
		info := newSAADriver(t, env, cfg).driveTrace(t, nil).describe(t).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.Equal(t, info.GetExecutionTime().AsTime(), info.GetNextAttemptScheduleTime().AsTime(),
			"during a start delay, NextAttemptScheduleTime is the pending dispatch time (schedule+delay)")
		require.Nil(t, info.GetCurrentRetryInterval(), "the first attempt is not a retry")
	})

	// First attempt running: no pending next dispatch, and no retry interval reported while running.
	s.T().Run("FirstAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongRetryInterval}, []model.Event{model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  1,
			})
	})

	// Backing off before the retry is dispatched: both the interval and the next-attempt schedule time
	// are populated.
	s.T().Run("BackingOff", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongRetryInterval}, []model.Event{model.Poll, model.FailRetryably},
			activityInfo{
				RunState:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:                    2,
				CurrentRetryInterval:       activityLongRetryInterval,
				NextAttemptScheduleTimeSet: true,
			})
	})

	// Backing off after a worker-supplied next_retry_delay: the reported interval is the worker's
	// override.
	s.T().Run("NextRetryDelayOverride", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongRetryInterval, NextRetryDelay: nextRetryDelayOverride},
			[]model.Event{model.Poll, model.FailRetryably},
			activityInfo{
				RunState:                   enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:                    2,
				CurrentRetryInterval:       nextRetryDelayOverride,
				NextAttemptScheduleTimeSet: true,
			})
	})

	// Retry dispatched to Matching but not yet polled: both fields are nil.
	s.T().Run("RetryDispatched", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortRetryInterval}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:  2,
			})
	})

	// Retry attempt running with a further retry still permitted (max 3): nothing pending while running.
	s.T().Run("RetryAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortRetryInterval}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  2,
			})
	})

	// Final attempt running with no retry remaining (max 2): still nothing pending while running.
	s.T().Run("FinalAttemptRunning", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 2, RetryInterval: activityShortRetryInterval}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED,
				Attempt:  2,
			})
	})

	// Paused while still backing off: dispatch will not occur while paused, so neither the interval nor
	// the next-attempt schedule time should be reported.
	s.T().Run("PausedBeforeDispatch", func(t *testing.T) {
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityLongRetryInterval}, []model.Event{model.Poll, model.FailRetryably, model.Pause},
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
		both(t, activityConfig{MaxAttempts: 3, RetryInterval: activityShortRetryInterval}, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Pause},
			activityInfo{
				RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED,
				Attempt:  2,
			})
	})
}

func (s *activityParityTestSuite) TestDriversRejectBackoffElapseWhenScheduleToCloseWins() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{
		MaxAttempts:     3,
		RetryInterval:   activityLongRetryInterval,
		ScheduleToClose: activityShortTimeout,
	}
	trace := []model.Event{model.Poll, model.FailRetryably}

	tests := map[string]func(*testing.T) func(require.TestingT, model.Event){
		"WorkflowActivity": func(t *testing.T) func(require.TestingT, model.Event) {
			return newWFADriver(t, env, cfg).driveTrace(t, trace).driveEvent
		},
		"StandaloneActivity": func(t *testing.T) func(require.TestingT, model.Event) {
			return newSAADriver(t, env, cfg).driveTrace(t, trace).driveEvent
		},
	}
	for name, setup := range tests {
		s.Run(name, func(s *activityParityTestSuite) {
			t := s.T()
			drive := setup(t)
			recorder := &activityDriverErrorRecorder{}
			drive(recorder, model.BackoffElapses)
			require.True(t, recorder.failed,
				"BackoffElapses must fail when ScheduleToClose removes the activity before its retry time")
		})
	}
}

func (s *activityParityTestSuite) TestDriversRejectBackoffElapseWithoutPendingBackoff() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.Poll}

	tests := map[string]func(*testing.T) func(require.TestingT, model.Event){
		"WorkflowActivity": func(t *testing.T) func(require.TestingT, model.Event) {
			return newWFADriver(t, env, activityConfig{}).driveTrace(t, trace).driveEvent
		},
		"StandaloneActivity": func(t *testing.T) func(require.TestingT, model.Event) {
			return newSAADriver(t, env, activityConfig{}).driveTrace(t, trace).driveEvent
		},
	}
	for name, setup := range tests {
		s.Run(name, func(s *activityParityTestSuite) {
			t := s.T()
			drive := setup(t)
			recorder := &activityDriverErrorRecorder{}
			drive(recorder, model.BackoffElapses)
			require.True(t, recorder.failed,
				"BackoffElapses must fail while the first attempt is running and no backoff is pending")
		})
	}
}

func (s *activityParityTestSuite) TestDriversRejectTimeoutElapseWhenDifferentTimeoutWins() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{
		MaxAttempts:  1,
		StartToClose: activityShortTimeout,
		Heartbeat:    2 * activityShortTimeout,
	}
	trace := []model.Event{model.Poll}

	tests := map[string]func(*testing.T) func(require.TestingT, model.Event){
		"WorkflowActivity": func(t *testing.T) func(require.TestingT, model.Event) {
			return newWFADriver(t, env, cfg).driveTrace(t, trace).driveEvent
		},
		"StandaloneActivity": func(t *testing.T) func(require.TestingT, model.Event) {
			return newSAADriver(t, env, cfg).driveTrace(t, trace).driveEvent
		},
	}
	for name, setup := range tests {
		s.Run(name, func(s *activityParityTestSuite) {
			t := s.T()
			drive := setup(t)
			recorder := &activityDriverErrorRecorder{}
			drive(recorder, model.HeartbeatElapses)
			require.True(t, recorder.failed,
				"HeartbeatElapses must fail when StartToClose times out first")
		})
	}
}

func (s *activityParityTestSuite) TestDriversAllowTimeoutsOnSeparateAttempts() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{
		MaxAttempts:   2,
		RetryInterval: activityShortRetryInterval,
	}
	trace := []model.Event{
		model.Poll,
		model.StartToCloseElapses,
		model.BackoffElapses,
		model.Poll,
		model.StartToCloseElapses,
	}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			newWFADriver(t, env, cfg).driveTrace(t, trace).terminalStatus(t))
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			newSAADriver(t, env, cfg).driveTrace(t, trace).terminalStatus(t))
	})
}

func (s *activityParityTestSuite) TestDriversAcceptTimeoutElapseThatAlreadyOccurred() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{
		MaxAttempts:  1,
		StartToClose: activityShortTimeout,
	}
	trace := []model.Event{model.Poll}

	tests := map[string]func(*testing.T) func(require.TestingT, model.Event){
		"WorkflowActivity": func(t *testing.T) func(require.TestingT, model.Event) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, a.terminalStatus(t))
			return a.driveEvent
		},
		"StandaloneActivity": func(t *testing.T) func(require.TestingT, model.Event) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, a.terminalStatus(t))
			return a.driveEvent
		},
	}
	for name, setup := range tests {
		s.Run(name, func(s *activityParityTestSuite) {
			t := s.T()
			setup(t)(t, model.StartToCloseElapses)
		})
	}
}

func (s *activityParityTestSuite) TestDriversDoNotLeakInferredTimeoutsAcrossTraces() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 1}
	trace := []model.Event{model.Poll, model.StartToCloseElapses}

	tests := map[string]func(*testing.T) time.Duration{
		"WorkflowActivity": func(t *testing.T) time.Duration {
			d := newWFADriver(t, env, cfg)
			d.driveTrace(t, trace)
			return d.cfg.StartToClose
		},
		"StandaloneActivity": func(t *testing.T) time.Duration {
			d := newSAADriver(t, env, cfg)
			d.driveTrace(t, trace)
			return d.cfg.StartToClose
		},
	}
	for name, setup := range tests {
		s.Run(name, func(s *activityParityTestSuite) {
			t := s.T()
			require.Zero(t, setup(t),
				"a timeout inferred for one trace must not become configuration for the next")
		})
	}
}

func (s *activityParityTestSuite) TestWFADriverWaitsForActivityToBeScheduled() {
	t := s.T()
	env := newActivityParityEnv(t)

	info := newWFADriver(t, env, activityConfig{}).driveTrace(t, nil).activityInfo(t)
	require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.RunState)
	require.Equal(t, int32(1), info.Attempt)
}

func (s *activityParityTestSuite) TestParityActivityInput() {
	env := newActivityParityEnv(s.T())

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		a := newWFADriver(t, env, activityConfig{}).driveTrace(t, nil)
		task := a.pollForTask(t, activityDriverPositivePollTimeout)
		require.NotNil(t, task)
		require.Equal(t, "Input", testcore.DecodeString(t, task.GetInput()))
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		a := newSAADriver(t, env, activityConfig{}).driveTrace(t, nil)
		task := a.pollForTask(t, activityDriverPositivePollTimeout)
		require.NotNil(t, task)
		require.Equal(t, "Input", testcore.DecodeString(t, task.GetInput()))
	})
}
