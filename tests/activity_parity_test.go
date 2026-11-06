package tests

// SAA <-> WFA parity tests

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type activityParityTestSuite struct {
	parallelsuite.Suite[*activityParityTestSuite]
}

func TestActivityParityTestSuite(t *testing.T) {
	parallelsuite.Run(t, &activityParityTestSuite{})
}

func newActivityParityEnv(t *testing.T, opts ...testcore.TestOption) *testcore.TestEnv {
	env := testcore.NewEnv(t, opts...)
	nsValues := func(value any) []dynamicconfig.ConstrainedValue {
		return []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: value},
		}
	}
	cluster := env.GetTestCluster()
	cluster.OverrideDynamicConfig(t, dynamicconfig.EnableChasm, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.Enabled, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.StandaloneAsyncHandoffEnabled, nsValues(true))
	cluster.OverrideDynamicConfig(t, dynamicconfig.EnableActivityEagerExecution, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.EnableEagerStart, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.EnableStandaloneActivityOperatorCommands, nsValues(true))
	return env
}

func (s *activityParityTestSuite) TestEagerActivityStartParity() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{
		EagerStart:    true,
		MaxAttempts:   2,
		RetryInterval: activityShortDispatchDelay,
	}

	// Both implementations return the first task already started. A retry of that task must then
	// return to the ordinary Matching dispatch path before its second attempt starts.
	trace := []model.Event{
		model.Heartbeat,
		model.FailRetryably,
		model.BackoffElapses,
		model.Poll,
		model.Complete,
	}

	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		newWFADriver(s.T(), env, cfg).driveTrace(s.T(), trace)
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		newSAADriver(s.T(), env, cfg).driveTrace(s.T(), trace)
	})
}

func (s *activityParityTestSuite) TestEagerActivityMetricParity() {
	type captureResult struct {
		recordings []*metricstest.CapturedRecording
		taskQueue  string
	}

	capture := func(t *testing.T, standalone, breakdownByTaskQueue bool) captureResult {
		env := newActivityParityEnv(t)
		env.GetTestCluster().OverrideDynamicConfig(t, dynamicconfig.MetricsBreakdownByTaskQueue,
			[]dynamicconfig.ConstrainedValue{{
				Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()},
				Value:       breakdownByTaskQueue,
			}})
		metricCapture := env.StartNamespaceMetricCapture()

		cfg := activityConfig{EagerStart: true}
		var taskQueue string
		if standalone {
			taskQueue = newSAADriver(t, env, cfg).start(t, cfg).taskQueue
		} else {
			taskQueue = newWFADriver(t, env, cfg).start(t, cfg).taskQueue
		}

		return captureResult{
			recordings: metricCapture.Metric(metrics.ActivityEagerExecutionCounter.Name()),
			taskQueue:  taskQueue,
		}
	}

	tagKeys := func(recording *metricstest.CapturedRecording) []string {
		keys := make([]string, 0, len(recording.Tags))
		for key := range recording.Tags {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		return keys
	}

	for _, breakdownByTaskQueue := range []bool{true, false} {
		name := "TaskQueueBreakdownEnabled"
		if !breakdownByTaskQueue {
			name = "TaskQueueBreakdownDisabled"
		}
		s.Run(name, func(s *activityParityTestSuite) {
			t := s.T()
			wfa := capture(t, false, breakdownByTaskQueue)
			saa := capture(t, true, breakdownByTaskQueue)
			require.Len(t, wfa.recordings, 1)
			require.Len(t, saa.recordings, 1)
			require.Equal(t, tagKeys(wfa.recordings[0]), tagKeys(saa.recordings[0]),
				"WFA and SAA eager activity metrics must use the same label keys")
			require.Equal(t, "Activity", wfa.recordings[0].Tags[metrics.TaskTypeTagName])
			require.Equal(t, "Activity", saa.recordings[0].Tags[metrics.TaskTypeTagName])

			expectedTaskQueue := "__omitted__"
			if breakdownByTaskQueue {
				require.Equal(t, wfa.taskQueue, wfa.recordings[0].Tags["taskqueue"])
				require.Equal(t, saa.taskQueue, saa.recordings[0].Tags["taskqueue"])
			} else {
				require.Equal(t, expectedTaskQueue, wfa.recordings[0].Tags["taskqueue"])
				require.Equal(t, expectedTaskQueue, saa.recordings[0].Tags["taskqueue"])
			}
		})
	}
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
			require.Equalf(t, activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				retryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
			}, newWFADriver(t, env, cfg).driveTrace(t, trace).terminalOutcome(t), message, timeout)
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equalf(t, activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				retryState: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
			}, newSAADriver(t, env, cfg).driveTrace(t, trace).terminalOutcome(t), message, timeout)
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
	}{
		{name: "ScheduleToStartTimeout", failureType: model.ScheduleToStartTimeoutFailureType},
		{name: "ScheduleToCloseTimeout", failureType: model.ScheduleToCloseTimeoutFailureType},
	}
	for _, tc := range nonRetryableTimeouts {
		s.Run(tc.name, func(s *activityParityTestSuite) {
			trace := []model.Event{
				model.Poll,
				{Type: model.RespondFailedByIDType, Failure: &model.Failure{Type: tc.failureType}},
			}
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
			timeout, ok := errors.AsType[*temporal.TimeoutError](activity.run.Get(activity.testContext(), nil))
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

	// Once the retry is due, pause keeps both fields absent even if dispatch has not reached Matching.
	s.Run("PausedWhenRetryDue", func(s *activityParityTestSuite) {
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

// TestActivityCore covers the core-activity (A) entries of the WFA/SAA parity survey.
func (s *activityParityTestSuite) TestActivityCore() {
	t := s.T()
	env := newActivityParityEnv(t)

	// A02: an operator command naming an activity that has already closed answers NotFound. Workflow
	// answers that uniformly across Signal, Update, UpdateOptions, Terminate, Pause and Unpause, and
	// consistency across execution types is what settles it.
	t.Run("OperatorCommandAfterClose", func(t *testing.T) {
		parityDriveOutlivingActivity(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Complete},
			func(t *testing.T, a parityActivity) {
				for _, e := range []model.Event{model.Pause, model.Unpause, model.Reset, model.UpdateOptions} {
					a.driveEvent(t, e)
				}
			})

		// Terminate and RequestCancel are standalone-activity commands: a workflow activity has no
		// terminate at all, and its cancellation is a workflow command rather than a call an operator
		// can make. So they are asserted on the standalone surface alone.
		t.Run("StandaloneActivityCommands", func(t *testing.T) {
			a := newSAADriver(t, env, activityConfig{MaxAttempts: 1}).
				driveTrace(t, []model.Event{model.Poll, model.Complete})
			for _, e := range []model.Event{model.Terminate, model.RequestCancel} {
				a.driveEvent(t, e)
			}
		})
	})
}

// parityActivity is the driver contract shared by workflow and standalone activity tests.
// TODO some of these methods will be consolidated when WFA supports handoff
type parityActivity interface {
	driveEvent(testing.TB, model.Event)
	terminalOutcome(require.TestingT) activityTerminalOutcome
	handoffInfo(require.TestingT) activityHandoffInfo
	taskResponse() *workflowservice.PollActivityTaskQueueResponse
	pollForTask(require.TestingT, time.Duration) *workflowservice.PollActivityTaskQueueResponse
	activityInfo(require.TestingT) activityInfo
	timeoutInfo(require.TestingT) activityTimeoutInfo
	terminalFailureInfo(require.TestingT) activityFailureInfo
}

// parityDriveOutlivingActivity drives trace through both implementations and hands each resulting
// activity to check. It is for a test that acts on the activity after it has closed: the WFA wrapper
// workflow must outlive its activity, or the RPC under test is answered about a workflow that no
// longer exists, which says nothing about how a closed activity behaves.
func parityDriveOutlivingActivity(
	t *testing.T,
	env *testcore.TestEnv,
	cfg activityConfig,
	trace []model.Event,
	check func(*testing.T, parityActivity),
) {
	t.Run("WorkflowActivity", func(t *testing.T) {
		d := newWFADriver(t, env, cfg)
		d.holdOpen = true
		check(t, d.driveTrace(t, trace))
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		check(t, newSAADriver(t, env, cfg).driveTrace(t, trace))
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
			require.Equal(t, activityTerminalOutcome{
				status:     enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
				retryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
			}, a.terminalOutcome(t))
			appErr, ok := errors.AsType[*temporal.ApplicationError](errors.Unwrap(a.run.Get(a.testContext(), nil)))
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
			// As FailureRetryPreventedByScheduleToClose, except that it is the worker's
			// next_retry_delay rather than the policy's interval that would run past the
			// schedule-to-close deadline: the policy's interval on its own leaves ample time.
			name: "NextRetryDelayRetryPreventedByScheduleToClose",
			cfg: activityConfig{
				NextRetryDelay:  activityLongDuration,
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

// Handoff lifecycle tests use raw task RPCs so they do not depend on SDK handoff support.
// WFA branches are placeholders until workflow activity handoff is implemented.
func (s *activityParityTestSuite) TestAsyncHandoff() {
	// Presence changes preserve the current task token and phase. Original options remain restorable.
	s.Run("LiveOptions", func(s *activityParityTestSuite) {
		s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
			s.T().Skip("TODO: workflow activity async handoff support")
		})

		s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
			for _, testCase := range []struct {
				name                  string
				configured, handedOff bool
			}{
				{name: "AddBeforeHandoff"},
				{name: "AddAfterHandoff", handedOff: true},
				{name: "EditBeforeHandoff", configured: true},
				{name: "EditAfterHandoff", configured: true, handedOff: true},
			} {
				s.Run(testCase.name, func(s *activityParityTestSuite) {
					t := s.T()
					env := newActivityParityEnv(t)
					cfg := activityConfig{}
					options := &activitypb.AsyncHandoffOptions{
						StartToHandoffTimeout: durationpb.New(time.Minute),
						HandoffToCloseTimeout: durationpb.New(time.Hour),
					}
					if testCase.configured {
						cfg.AsyncHandoffOptions = options
					}
					trace := []model.Event{model.Poll}
					if testCase.handedOff {
						trace = append(trace, model.Handoff)
					}
					a := newSAADriver(t, env, cfg).driveTrace(t, trace)
					before := a.describe(t).GetInfo()
					token := a.taskResponse().GetTaskToken()
					ctx := testcontext.For(t)

					update := func(options *activitypb.ActivityOptions, paths ...string) (*workflowservice.UpdateActivityExecutionOptionsResponse, error) {
						return env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
							Namespace: env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID,
							ActivityOptions: options, UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
						})
					}

					response, err := update(&activitypb.ActivityOptions{AsyncHandoffOptions: options}, "async_handoff_options")
					require.NoError(t, err)
					require.NotNil(t, response.GetActivityOptions().GetAsyncHandoffOptions().GetRetryPolicy())

					// Existing executions remain editable after disablement, including nested policy replacement.
					env.GetTestCluster().OverrideDynamicConfig(t, activity.StandaloneAsyncHandoffEnabled, []dynamicconfig.ConstrainedValue{{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: false}})
					_, err = update(&activitypb.ActivityOptions{AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
						HeartbeatTimeout: durationpb.New(time.Minute),
						RetryPolicy:      &commonpb.RetryPolicy{InitialInterval: durationpb.New(time.Second), MaximumAttempts: 3},
					}}, "asyncHandoffOptions.heartbeatTimeout", "asyncHandoffOptions.retryPolicy")
					require.NoError(t, err)
					require.EqualValues(t, 3, a.describe(t).GetInfo().GetAsyncHandoffOptions().GetRetryPolicy().GetMaximumAttempts())

					_, err = update(&activitypb.ActivityOptions{}, "async_handoff_options")
					require.NoError(t, err)
					removed := a.describe(t).GetInfo()
					require.Nil(t, removed.GetAsyncHandoffOptions())
					require.Zero(t, removed.GetHandoffAttempt())

					_, err = update(&activitypb.ActivityOptions{AsyncHandoffOptions: options}, "async_handoff_options")
					require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
					require.Nil(t, a.describe(t).GetInfo().GetAsyncHandoffOptions())

					env.GetTestCluster().OverrideDynamicConfig(t, activity.StandaloneAsyncHandoffEnabled, []dynamicconfig.ConstrainedValue{{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: true}})
					_, err = update(&activitypb.ActivityOptions{AsyncHandoffOptions: options}, "async_handoff_options")
					require.NoError(t, err)
					_, err = env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
						Namespace: env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID, RestoreOriginal: true,
					})
					require.NoError(t, err)

					restored := a.describe(t).GetInfo()
					protorequire.ProtoEqual(t, before.GetAsyncHandoffOptions(), restored.GetAsyncHandoffOptions())
					require.Equal(t, before.GetAttempt(), restored.GetAttempt())
					require.Equal(t, before.GetAwaitingCompletion(), restored.GetAwaitingCompletion())
					protorequire.ProtoEqual(t, before.GetAsyncHandoffTime(), restored.GetAsyncHandoffTime())

					// Complete using the original worker token after every presence and policy change.
					_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
						Namespace: env.Namespace().String(), TaskToken: token,
					})
					require.NoError(t, err)
					require.Equal(t, activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}, a.terminalOutcome(t))
				})
			}
		})
	})

	// Verify policy selection through frontend validation, merging, persistence and Describe.
	s.Run("RetryPolicyUpdatesWhileWaiting", func(s *activityParityTestSuite) {
		s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
			s.T().Skip("TODO: workflow activity async handoff support")
		})

		s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
			for _, testCase := range []struct {
				name                                            string
				handedOff, updateActivityPolicy, removeAndReadd bool
				expectedInterval                                time.Duration
			}{
				{name: "HandoffPolicy", expectedInterval: time.Minute},
				{name: "HandoffRetryIgnoresActivityPolicy", updateActivityPolicy: true, expectedInterval: time.Hour},
				{name: "ActivityPolicy", handedOff: true, updateActivityPolicy: true, expectedInterval: time.Minute},
				{name: "ActivityRetryIgnoresHandoffPolicy", handedOff: true, expectedInterval: time.Hour},
				{name: "ReaddedHandoffPolicy", removeAndReadd: true, expectedInterval: time.Minute},
			} {
				s.Run(testCase.name, func(s *activityParityTestSuite) {
					t := s.T()
					env := newActivityParityEnv(t)
					cfg := activityConfig{
						RetryInterval: time.Hour,
						AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
							RetryPolicy: &commonpb.RetryPolicy{
								InitialInterval:    durationpb.New(time.Hour),
								MaximumInterval:    durationpb.New(time.Hour),
								BackoffCoefficient: 2,
							},
						},
					}
					trace := []model.Event{model.Poll}
					if testCase.handedOff {
						trace = append(trace, model.Handoff)
					}
					trace = append(trace, model.FailRetryably)
					a := newSAADriver(t, env, cfg).driveTrace(t, trace)
					before := a.describe(t).GetInfo()
					require.Equal(t, time.Hour, before.GetCurrentRetryInterval().AsDuration())
					ctx := testcontext.For(t)
					update := func(options *activitypb.ActivityOptions, path string) {
						t.Helper()
						_, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
							Namespace: env.Namespace().String(), ActivityId: a.activityID, RunId: a.runID,
							ActivityOptions: options, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{path}},
						})
						require.NoError(t, err)
					}

					expectedHandoffAttempt := before.GetHandoffAttempt()
					if testCase.removeAndReadd {
						update(&activitypb.ActivityOptions{}, "async_handoff_options")
						update(&activitypb.ActivityOptions{AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{}}, "async_handoff_options")
						readded := a.describe(t).GetInfo()
						require.EqualValues(t, 1, readded.GetHandoffAttempt())
						require.Equal(t, time.Hour, readded.GetCurrentRetryInterval().AsDuration())
						expectedHandoffAttempt = 1
					}

					policy := &commonpb.RetryPolicy{InitialInterval: durationpb.New(time.Minute)}
					if testCase.updateActivityPolicy {
						update(&activitypb.ActivityOptions{RetryPolicy: policy}, "retry_policy.initial_interval")
					} else {
						update(&activitypb.ActivityOptions{AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{RetryPolicy: policy}}, "async_handoff_options.retry_policy.initial_interval")
					}

					after := a.describe(t).GetInfo()
					require.Equal(t, before.GetAttempt(), after.GetAttempt())
					require.Equal(t, expectedHandoffAttempt, after.GetHandoffAttempt())
					require.Equal(t, testCase.expectedInterval, after.GetCurrentRetryInterval().AsDuration())
					protorequire.ProtoEqual(t, before.GetLastAttemptCompleteTime(), after.GetLastAttemptCompleteTime())
					require.Equal(t, before.GetLastAttemptCompleteTime().AsTime().Add(testCase.expectedInterval), after.GetNextAttemptScheduleTime().AsTime())
					_, err := env.FrontendClient().RespondActivityTaskHandoff(ctx, &workflowservice.RespondActivityTaskHandoffRequest{
						Namespace: env.Namespace().String(), TaskToken: a.taskResponse().GetTaskToken(),
					})
					require.ErrorAs(t, err, new(*serviceerror.NotFound))
				})
			}
		})
	})

	s.Run("RecordHandoffAndCompleteExternally", func(s *activityParityTestSuite) {
		cfg := activityConfig{
			AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
				StartToHandoffTimeout: durationpb.New(time.Minute),
				HandoffToCloseTimeout: durationpb.New(time.Hour),
			},
		}
		trace := []model.Event{model.Poll, model.Handoff}

		check := func(t *testing.T, a parityActivity, completion model.Event) {
			info := a.handoffInfo(t)
			require.NotNil(t, info.handoffTime)
			require.NotNil(t, info.options)
			protorequire.ProtoEqual(t, cfg.AsyncHandoffOptions.GetStartToHandoffTimeout(), info.options.GetStartToHandoffTimeout())
			protorequire.ProtoEqual(t, cfg.AsyncHandoffOptions.GetHandoffToCloseTimeout(), info.options.GetHandoffToCloseTimeout())

			a.driveEvent(t, completion)
			require.Equal(t, activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}, a.terminalOutcome(t))

			closed := a.handoffInfo(t)
			require.False(t, closed.awaitingCompletion)
			protorequire.ProtoEqual(t, info.handoffTime, closed.handoffTime)

			// The model expects handoff after closure to return NotFound.
			a.driveEvent(t, model.Handoff)
		}

		for _, testCase := range []struct {
			name       string
			completion model.Event
		}{
			{name: "TaskToken", completion: model.Complete},
			// By-ID completion uses an unstamped token even when the worker task is stamped.
			{name: "ByID", completion: model.CompleteByID},
		} {
			s.Run(testCase.name, func(s *activityParityTestSuite) {
				s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
					s.T().Skip("TODO: expose retained workflow handoff metadata through the API and run the shared check with newWFADriver")
				})

				s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
					t := s.T()
					env := newActivityParityEnv(t)
					check(t, newSAADriver(t, env, cfg).driveTrace(t, trace), testCase.completion)
				})
			})
		}
	})

	// Eager and Matching delivery use different start paths. Verify that neither
	// adds a start-to-close or schedule-to-close timeout when only phase timeouts
	// are configured.
	s.Run("CompletesWithOnlyPhaseTimeouts", func(s *activityParityTestSuite) {
		cfg := activityConfig{
			DisableStartToClose: true,
			AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
				StartToHandoffTimeout: durationpb.New(time.Minute),
				HandoffToCloseTimeout: durationpb.New(time.Hour),
			},
		}
		trace := []model.Event{model.Poll, model.Handoff}

		check := func(t *testing.T, a parityActivity) {
			task := a.taskResponse()
			require.NotNil(t, task)
			require.EqualValues(t, 1, task.GetAttempt())
			require.EqualValues(t, 1, task.GetHandoffAttempt())
			require.Zero(t, task.GetStartToCloseTimeout().AsDuration())
			require.Zero(t, task.GetScheduleToCloseTimeout().AsDuration())
			require.NotNil(t, task.GetAsyncHandoffOptions().GetRetryPolicy())
			protorequire.ProtoEqual(t, cfg.AsyncHandoffOptions.GetStartToHandoffTimeout(), task.GetAsyncHandoffOptions().GetStartToHandoffTimeout())
			protorequire.ProtoEqual(t, cfg.AsyncHandoffOptions.GetHandoffToCloseTimeout(), task.GetAsyncHandoffOptions().GetHandoffToCloseTimeout())

			info := a.handoffInfo(t)
			require.Zero(t, info.startToCloseTimeout)
			require.Zero(t, info.scheduleToCloseTimeout)
			protorequire.ProtoEqual(t, task.GetAsyncHandoffOptions(), info.options)

			a.driveEvent(t, model.Complete)
			require.Equal(t, activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}, a.terminalOutcome(t))
			require.False(t, a.handoffInfo(t).awaitingCompletion)
		}

		s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
			s.T().Skip("TODO: workflow activity async handoff support")
		})

		s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
			for _, testCase := range []struct {
				name  string
				eager bool
				trace []model.Event
			}{
				{name: "MatchingDispatch", trace: trace},
				{name: "EagerDispatch", eager: true, trace: []model.Event{model.Handoff}},
			} {
				s.Run(testCase.name, func(s *activityParityTestSuite) {
					t := s.T()
					env := newActivityParityEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableActivityEagerDispatchCheck, false))
					caseCfg := cfg
					caseCfg.EagerStart = testCase.eager
					check(t, newSAADriver(t, env, caseCfg).driveTrace(t, testCase.trace))
				})
			}
		})
	})

	s.Run("RepeatedHandoffPreservesTimestamp", func(s *activityParityTestSuite) {
		for _, testCase := range []struct {
			name    string
			options *activitypb.AsyncHandoffOptions
		}{
			{
				name: "WithHandoffOptions",
				options: &activitypb.AsyncHandoffOptions{
					StartToHandoffTimeout: durationpb.New(time.Minute),
					HandoffToCloseTimeout: durationpb.New(time.Hour),
				},
			},
			{name: "WithoutHandoffOptions"},
		} {
			s.Run(testCase.name, func(s *activityParityTestSuite) {
				cfg := activityConfig{AsyncHandoffOptions: testCase.options}
				trace := []model.Event{model.Poll, model.Handoff}

				check := func(t *testing.T, a parityActivity) {
					before := a.handoffInfo(t)
					require.NotNil(t, before.handoffTime)
					if testCase.options == nil {
						require.Nil(t, before.options)
					} else {
						require.NotNil(t, before.options)
					}

					a.driveEvent(t, model.Handoff)
					after := a.handoffInfo(t)
					protorequire.ProtoEqual(t, before.handoffTime, after.handoffTime)
					protorequire.ProtoEqual(t, before.options, after.options)

					a.driveEvent(t, model.Complete)
					require.Equal(t, activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}, a.terminalOutcome(t))

					closed := a.handoffInfo(t)
					require.False(t, closed.awaitingCompletion)
					protorequire.ProtoEqual(t, before.handoffTime, closed.handoffTime)
				}

				s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
					s.T().Skip("TODO: support workflow handoff with and without options and run the shared check with newWFADriver")
				})

				s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
					t := s.T()
					env := newActivityParityEnv(t)
					check(t, newSAADriver(t, env, cfg).driveTrace(t, trace))
				})
			})
		}
	})

	// Without handoff options, external failure still advances the activity attempt and clears the handoff timestamp at pickup.
	s.Run("PreservesActivityRetryBehavior", func(s *activityParityTestSuite) {
		cfg := activityConfig{}
		trace := []model.Event{model.Poll, model.Handoff}

		check := func(t *testing.T, env *testcore.TestEnv, a parityActivity) {
			handoffTime := a.handoffInfo(t).handoffTime
			require.NotNil(t, handoffTime)
			previousToken := a.taskResponse().GetTaskToken()

			a.driveEvent(t, model.FailRetryably)
			protorequire.ProtoEqual(t, handoffTime, a.handoffInfo(t).handoffTime)

			for _, event := range []model.Event{model.BackoffElapses, model.Poll} {
				a.driveEvent(t, event)
			}
			require.Nil(t, a.handoffInfo(t).handoffTime)

			// The previous attempt's task token is now stale.
			_, err := env.FrontendClient().RespondActivityTaskHandoff(testcontext.For(t), &workflowservice.RespondActivityTaskHandoffRequest{
				Namespace: env.Namespace().String(), TaskToken: previousToken,
			})
			require.ErrorAs(t, err, new(*serviceerror.NotFound))

			for _, event := range []model.Event{model.Handoff, model.Complete} {
				a.driveEvent(t, event)
			}
			require.Equal(t, activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}, a.terminalOutcome(t))
		}

		s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
			s.T().Skip("TODO: support reporting-only workflow handoff and run the shared check with newWFADriver")
		})

		s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
			t := s.T()
			env := newActivityParityEnv(t)
			check(t, env, newSAADriver(t, env, cfg).driveTrace(t, trace))
		})
	})

	// Each trace checks server state against the model after every operation.
	s.Run("ModelConformance", func(s *activityParityTestSuite) {
		cfg := activityConfig{
			MaxAttempts: 5,
			AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval:    durationpb.New(activityShortDispatchDelay),
					MaximumInterval:    durationpb.New(activityShortDispatchDelay),
					BackoffCoefficient: 1,
					MaximumAttempts:    4,
				},
			},
		}

		for _, testCase := range []struct {
			name  string
			trace []model.Event
		}{
			{name: "submission retries then external failure", trace: []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.Handoff, model.FailRetryably}},
			{name: "resume after submission failure", trace: []model.Event{model.Poll, model.Pause, model.FailRetryably, model.Unpause, model.BackoffElapses, model.Poll}},
			{name: "reset after handoff", trace: []model.Event{model.Poll, model.Handoff, model.Reset, model.FailRetryably}},
			{name: "reset on nonretryable failure", trace: []model.Event{model.Poll, model.Reset, model.FailNonRetryably}},
			{name: "submission timeout", trace: []model.Event{model.Poll, model.StartToHandoffElapses}},
			{name: "completion timeout", trace: []model.Event{model.Poll, model.Handoff, model.HandoffToCloseElapses}},
			{name: "submission heartbeat", trace: []model.Event{model.Poll, model.HeartbeatElapses}},
			{name: "completion heartbeat", trace: []model.Event{model.Poll, model.Handoff, model.HeartbeatElapses}},
		} {
			s.Run(testCase.name, func(s *activityParityTestSuite) {
				s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
					s.T().Skip("TODO: workflow activity async handoff support")
				})

				s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
					t := s.T()
					env := newActivityParityEnv(t)
					newSAADriver(t, env, cfg).driveTrace(t, testCase.trace)
				})
			})
		}
	})

	// Start-to-close continues through handoff retry backoff and queue wait; expiry replaces the pending handoff retry with an activity retry.
	s.Run("StartToCloseDuringRedispatch", func(s *activityParityTestSuite) {
		for _, testCase := range []struct {
			name          string
			retryInterval time.Duration
			trace         []model.Event
		}{
			{
				name: "backoff", retryInterval: time.Minute,
				trace: []model.Event{model.Poll, model.FailRetryably, model.StartToCloseElapses},
			},
			{
				name: "matching queue wait", retryInterval: time.Second,
				trace: []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.StartToCloseElapses},
			},
		} {
			s.Run(testCase.name, func(s *activityParityTestSuite) {
				cfg := activityConfig{
					StartToClose: 10 * time.Second, RetryInterval: time.Second, MaxAttempts: 2,
					AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
						RetryPolicy: &commonpb.RetryPolicy{InitialInterval: durationpb.New(testCase.retryInterval)},
					},
				}

				check := func(t *testing.T, env *testcore.TestEnv, a parityActivity) {
					// Poll before waiting for backoff so an obsolete Matching task can expose an early pickup.
					next := a.pollForTask(t, activityDriverTimeout)
					require.NotNil(t, next)
					require.EqualValues(t, 2, next.GetAttempt())
					require.EqualValues(t, 1, next.GetHandoffAttempt())
					require.NotNil(t, next.GetStartedTime())
					require.NotNil(t, next.GetCurrentAttemptScheduledTime())
					require.False(t, next.GetStartedTime().AsTime().Before(next.GetCurrentAttemptScheduledTime().AsTime()),
						"replacement attempt started before its dispatch time")

					require.Equal(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, a.timeoutInfo(t).timeout)
					require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, a.activityInfo(t).RunState)

					_, err := env.FrontendClient().RespondActivityTaskCompleted(testcontext.For(t), &workflowservice.RespondActivityTaskCompletedRequest{
						Namespace: env.Namespace().String(), TaskToken: next.GetTaskToken(), Result: payloads.EncodeString("result"),
					})
					require.NoError(t, err)
					require.Equal(t, activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}, a.terminalOutcome(t))
				}

				s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
					s.T().Skip("TODO: workflow activity async handoff support")
				})

				s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
					t := s.T()
					env := newActivityParityEnv(t)
					check(t, env, newSAADriver(t, env, cfg).driveTrace(t, testCase.trace))
				})
			})
		}
	})

	// All token-based activity RPCs must reject the previous task token even though the activity attempt is unchanged.
	s.Run("RedispatchRejectsPreviousTaskToken", func(s *activityParityTestSuite) {
		cfg := activityConfig{
			StartToClose: time.Hour,
			MaxAttempts:  1,
			AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
				StartToHandoffTimeout: durationpb.New(2 * time.Minute),
				HandoffToCloseTimeout: durationpb.New(time.Hour),
				RetryPolicy:           &commonpb.RetryPolicy{InitialInterval: durationpb.New(time.Second)},
			},
		}
		trace := []model.Event{model.Poll, model.FailRetryably}

		check := func(t *testing.T, env *testcore.TestEnv, a parityActivity) {
			first := a.taskResponse()
			staleToken := first.GetTaskToken()
			client := env.FrontendClient()
			namespaceName := env.Namespace().String()

			assertStaleToken := func() {
				t.Helper()
				ctx := testcontext.For(t)

				_, err := client.RespondActivityTaskHandoff(ctx, &workflowservice.RespondActivityTaskHandoffRequest{
					Namespace: namespaceName, TaskToken: staleToken,
				})
				require.ErrorAs(t, err, new(*serviceerror.NotFound))

				_, err = client.RecordActivityTaskHeartbeat(ctx, &workflowservice.RecordActivityTaskHeartbeatRequest{
					Namespace: namespaceName, TaskToken: staleToken,
				})
				require.ErrorAs(t, err, new(*serviceerror.NotFound))

				_, err = client.RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
					Namespace: namespaceName, TaskToken: staleToken,
				})
				require.ErrorAs(t, err, new(*serviceerror.NotFound))

				_, err = client.RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: namespaceName, TaskToken: staleToken,
				})
				require.ErrorAs(t, err, new(*serviceerror.NotFound))

				_, err = client.RespondActivityTaskCanceled(ctx, &workflowservice.RespondActivityTaskCanceledRequest{
					Namespace: namespaceName, TaskToken: staleToken,
				})
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			}

			assertStaleToken()
			for _, event := range []model.Event{model.BackoffElapses, model.Poll} {
				a.driveEvent(t, event)
			}
			next := a.taskResponse()
			require.Equal(t, first.GetAttempt(), next.GetAttempt())
			require.EqualValues(t, 2, next.GetHandoffAttempt())

			assertStaleToken()
			for _, event := range []model.Event{model.Handoff, model.Complete} {
				a.driveEvent(t, event)
			}
			require.Equal(t, activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}, a.terminalOutcome(t))
		}

		s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
			s.T().Skip("TODO: workflow activity async handoff support")
		})

		s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
			t := s.T()
			env := newActivityParityEnv(t)
			check(t, env, newSAADriver(t, env, cfg).driveTrace(t, trace))
		})
	})

	s.Run("PhaseTimeoutClosesActivity", func(s *activityParityTestSuite) {
		expected := activityTerminalOutcome{
			status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, retryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		}

		for _, testCase := range []struct {
			name                  string
			startToHandoffTimeout time.Duration
			handoffToCloseTimeout time.Duration
			trace                 []model.Event
			expectedTimeout       enumspb.TimeoutType
		}{
			{
				name: "submission phase", startToHandoffTimeout: 5 * time.Second, handoffToCloseTimeout: 2 * time.Minute,
				trace:           []model.Event{model.Poll, model.StartToHandoffElapses},
				expectedTimeout: enumspb.TIMEOUT_TYPE_START_TO_HANDOFF,
			},
			{
				name: "completion phase", startToHandoffTimeout: 2 * time.Minute, handoffToCloseTimeout: 5 * time.Second,
				trace:           []model.Event{model.Poll, model.Handoff, model.HandoffToCloseElapses},
				expectedTimeout: enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE,
			},
		} {
			s.Run(testCase.name, func(s *activityParityTestSuite) {
				cfg := activityConfig{
					DisableStartToClose: true,
					MaxAttempts:         1,
					AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
						StartToHandoffTimeout: durationpb.New(testCase.startToHandoffTimeout),
						HandoffToCloseTimeout: durationpb.New(testCase.handoffToCloseTimeout),
						RetryPolicy:           &commonpb.RetryPolicy{MaximumAttempts: 1},
					},
				}

				check := func(t *testing.T, a parityActivity) {
					require.Equal(t, expected, a.terminalOutcome(t))
					require.Equal(t, activityTimeoutInfo{
						timeout:        testCase.expectedTimeout,
						outcomeTimeout: testCase.expectedTimeout,
						attempt:        1,
						terminal:       true,
						handoffAttempt: 1,
					}, a.timeoutInfo(t))
					require.False(t, a.handoffInfo(t).awaitingCompletion)
				}

				s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
					s.T().Skip("TODO: workflow activity async handoff support")
				})

				s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
					t := s.T()
					env := newActivityParityEnv(t)
					check(t, newSAADriver(t, env, cfg).driveTrace(t, testCase.trace))
				})
			})
		}
	})

	// Exhausting handoff retries closes the activity with the final failure even when activity retries remain.
	s.Run("RetryExhaustion", func(s *activityParityTestSuite) {
		const maxHandoffAttempts int32 = 3
		cfg := activityConfig{
			MaxAttempts: 5,
			AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval:    durationpb.New(activityShortDispatchDelay),
					MaximumInterval:    durationpb.New(activityShortDispatchDelay),
					BackoffCoefficient: 1,
					MaximumAttempts:    maxHandoffAttempts,
				},
			},
		}
		// Earlier failures omit the payload so retaining one cannot satisfy the final-failure checks.
		trace := []model.Event{
			model.Poll, {Type: model.RespondFailedType}, model.BackoffElapses,
			model.Poll, {Type: model.RespondFailedType}, model.BackoffElapses,
			model.Poll, model.FailRetryably,
		}
		expected := activityTerminalOutcome{
			status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, retryState: enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		}

		check := func(t *testing.T, a parityActivity) {
			require.Equal(t, expected, a.terminalOutcome(t))

			info := a.terminalFailureInfo(t)
			require.EqualValues(t, 1, info.attempt)
			require.Equal(t, maxHandoffAttempts, info.handoffAttempt)

			finalFailure := &failurepb.Failure{
				Message: "test failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "TestFailure"},
				},
			}
			protorequire.ProtoEqual(t, finalFailure, info.lastFailure)
			protorequire.ProtoEqual(t, finalFailure, info.recordedOutcome.GetFailure())
			protorequire.ProtoEqual(t, info.recordedOutcome, info.resultOutcome)
		}

		s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
			s.T().Skip("TODO: expose workflow handoff terminal failure metadata and run the shared check with newWFADriver")
		})

		s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
			t := s.T()
			env := newActivityParityEnv(t)
			check(t, newSAADriver(t, env, cfg).driveTrace(t, trace))
		})
	})

	s.Run("ResetKeepsPausedAndClearsTimestamp", func(s *activityParityTestSuite) {
		cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
		trace := []model.Event{model.Poll, model.Handoff, model.FailRetryably, model.Pause}

		check := func(t *testing.T, a parityActivity) {
			require.NotNil(t, a.handoffInfo(t).handoffTime)

			a.driveEvent(t, model.ResetKeepPaused)
			require.Nil(t, a.handoffInfo(t).handoffTime)

			for _, event := range []model.Event{model.Unpause, model.Poll, model.Complete} {
				a.driveEvent(t, event)
			}
			require.Equal(t, activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}, a.terminalOutcome(t))
		}

		s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
			s.T().Skip("TODO: workflow activity async handoff support")
		})

		s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
			t := s.T()
			env := newActivityParityEnv(t)
			check(t, newSAADriver(t, env, cfg).driveTrace(t, trace))
		})
	})

	s.Run("PendingOperatorRequests", func(s *activityParityTestSuite) {
		cfg := activityConfig{
			MaxAttempts: 3,
			AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval:    durationpb.New(activityShortDispatchDelay),
					MaximumInterval:    durationpb.New(activityShortDispatchDelay),
					BackoffCoefficient: 1,
					MaximumAttempts:    4,
				},
			},
		}

		for _, testCase := range []struct {
			name                string
			trace               []model.Event
			startToCloseTimeout time.Duration
			heartbeatResponse   *workflowservice.RecordActivityTaskHeartbeatResponse
			expectedRunState    enumspb.PendingActivityState
			finish              []model.Event
			expectedOutcome     activityTerminalOutcome
		}{
			{
				name:              "pause requested before handoff",
				trace:             []model.Event{model.Poll, model.Pause, model.Handoff},
				heartbeatResponse: &workflowservice.RecordActivityTaskHeartbeatResponse{ActivityPaused: true},
				expectedRunState:  enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED,
				finish:            []model.Event{model.FailRetryably, model.Unpause, model.BackoffElapses, model.Poll, model.Complete},
				expectedOutcome:   activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
			},
			{
				name:              "pause requested after handoff",
				trace:             []model.Event{model.Poll, model.Handoff, model.Pause},
				heartbeatResponse: &workflowservice.RecordActivityTaskHeartbeatResponse{ActivityPaused: true},
				expectedRunState:  enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED,
				finish:            []model.Event{model.FailRetryably, model.Unpause, model.BackoffElapses, model.Poll, model.Complete},
				expectedOutcome:   activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
			},
			{
				name:              "cancellation requested before handoff",
				trace:             []model.Event{model.Poll, model.RequestCancel, model.Handoff},
				heartbeatResponse: &workflowservice.RecordActivityTaskHeartbeatResponse{CancelRequested: true},
				expectedRunState:  enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
				finish:            []model.Event{model.RespondCanceled},
				expectedOutcome:   activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED},
			},
			{
				name:              "cancellation requested after handoff",
				trace:             []model.Event{model.Poll, model.Handoff, model.RequestCancel},
				heartbeatResponse: &workflowservice.RecordActivityTaskHeartbeatResponse{CancelRequested: true},
				expectedRunState:  enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
				finish:            []model.Event{model.RespondCanceled},
				expectedOutcome:   activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED},
			},
			// The carried start-to-close timer stays active while an operator request is pending.
			{
				name:                "start-to-close expires after handoff with cancellation pending",
				trace:               []model.Event{model.Poll, model.Handoff, model.RequestCancel},
				startToCloseTimeout: 5 * activityShortTimeout,
				heartbeatResponse:   &workflowservice.RecordActivityTaskHeartbeatResponse{CancelRequested: true},
				expectedRunState:    enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
				finish:              []model.Event{model.StartToCloseElapses},
				expectedOutcome:     activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, retryState: enumspb.RETRY_STATE_CANCEL_REQUESTED},
			},
			{
				name:                "start-to-close expires after handoff with pause pending",
				trace:               []model.Event{model.Poll, model.Handoff, model.Pause},
				startToCloseTimeout: 5 * activityShortTimeout,
				heartbeatResponse:   &workflowservice.RecordActivityTaskHeartbeatResponse{ActivityPaused: true},
				expectedRunState:    enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED,
				finish:              []model.Event{model.StartToCloseElapses, model.Unpause, model.BackoffElapses, model.Poll, model.Complete},
				expectedOutcome:     activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
			},
			{
				name:                "start-to-close expires after handoff with reset pending",
				trace:               []model.Event{model.Poll, model.Handoff, model.Reset},
				startToCloseTimeout: 5 * activityShortTimeout,
				heartbeatResponse:   &workflowservice.RecordActivityTaskHeartbeatResponse{ActivityReset: true},
				expectedRunState:    enumspb.PENDING_ACTIVITY_STATE_AWAITING_COMPLETION,
				finish:              []model.Event{model.StartToCloseElapses, model.Poll, model.Complete},
				expectedOutcome:     activityTerminalOutcome{status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
			},
		} {
			s.Run(testCase.name, func(s *activityParityTestSuite) {
				check := func(t *testing.T, env *testcore.TestEnv, a parityActivity) {
					// The original task token remains usable and exposes the pending operator request.
					response, err := env.FrontendClient().RecordActivityTaskHeartbeat(testcontext.For(t), &workflowservice.RecordActivityTaskHeartbeatRequest{
						Namespace: env.Namespace().String(), TaskToken: a.taskResponse().GetTaskToken(),
					})
					require.NoError(t, err)
					protorequire.ProtoEqual(t, testCase.heartbeatResponse, response)

					require.True(t, a.handoffInfo(t).awaitingCompletion)
					require.Equal(t, testCase.expectedRunState, a.activityInfo(t).RunState)

					for _, event := range testCase.finish {
						a.driveEvent(t, event)
					}
					require.Equal(t, testCase.expectedOutcome, a.terminalOutcome(t))
				}

				s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
					s.T().Skip("TODO: workflow activity async handoff support")
				})

				s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
					t := s.T()
					env := newActivityParityEnv(t)
					testConfig := cfg
					testConfig.StartToClose = testCase.startToCloseTimeout
					check(t, env, newSAADriver(t, env, testConfig).driveTrace(t, testCase.trace))
				})
			})
		}
	})

	// Schedule-to-start timeout closes the redispatched activity even when both retry policies allow more attempts.
	s.Run("RedispatchScheduleToStartTimeout", func(s *activityParityTestSuite) {
		cfg := activityConfig{
			MaxAttempts:     3,
			ScheduleToStart: 5 * time.Second,
			AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval:    durationpb.New(activityShortDispatchDelay),
					MaximumInterval:    durationpb.New(activityShortDispatchDelay),
					BackoffCoefficient: 1,
					MaximumAttempts:    4,
				},
			},
		}
		trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.ScheduleToStartElapses}
		expected := activityTerminalOutcome{
			status:     enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			retryState: enumspb.RETRY_STATE_TIMEOUT,
		}

		check := func(t *testing.T, a parityActivity) {
			require.Equal(t, expected, a.terminalOutcome(t))
			require.Equal(t, activityTimeoutInfo{
				timeout:        enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				outcomeTimeout: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				attempt:        1,
				terminal:       true,
				handoffAttempt: 2,
			}, a.timeoutInfo(t))
			require.False(t, a.handoffInfo(t).awaitingCompletion)
		}

		s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
			s.T().Skip("TODO: workflow activity async handoff support")
		})

		s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
			t := s.T()
			env := newActivityParityEnv(t)
			check(t, newSAADriver(t, env, cfg).driveTrace(t, trace))
		})
	})

	// Whether a handoff retry fits before schedule-to-close must use the handoff policy, independently of the activity policy.
	s.Run("RetryScheduleToClose", func(s *activityParityTestSuite) {
		const (
			scheduleToClose = 30 * time.Second
			shortRetry      = time.Second
			longRetry       = 2 * scheduleToClose
		)

		for _, testCase := range []struct {
			name                        string
			activityRetry, handoffRetry time.Duration
			event                       model.Event
			expectedStatus              enumspb.ActivityExecutionStatus
		}{
			{
				name: "handoff failure cannot retry", activityRetry: shortRetry, handoffRetry: longRetry,
				event: model.FailRetryably, expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
			},
			{
				name: "handoff timeout cannot retry", activityRetry: shortRetry, handoffRetry: longRetry,
				event: model.StartToHandoffElapses, expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			},
			{
				name: "handoff failure can retry", activityRetry: longRetry, handoffRetry: shortRetry,
				event: model.FailRetryably, expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			},
			{
				name: "handoff timeout can retry", activityRetry: longRetry, handoffRetry: shortRetry,
				event: model.StartToHandoffElapses, expectedStatus: enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING,
			},
		} {
			s.Run(testCase.name, func(s *activityParityTestSuite) {
				cfg := activityConfig{
					MaxAttempts: 3, RetryInterval: testCase.activityRetry, ScheduleToClose: scheduleToClose,
					AsyncHandoffOptions: &activitypb.AsyncHandoffOptions{
						RetryPolicy: &commonpb.RetryPolicy{
							InitialInterval:    durationpb.New(testCase.handoffRetry),
							MaximumInterval:    durationpb.New(testCase.handoffRetry),
							BackoffCoefficient: 1,
							MaximumAttempts:    3,
						},
					},
				}
				trace := []model.Event{model.Poll, testCase.event}

				check := func(t *testing.T, a parityActivity) {
					if testCase.expectedStatus == enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING {
						require.False(t, a.timeoutInfo(t).terminal)
						return
					}

					require.Equal(t, activityTerminalOutcome{
						status: testCase.expectedStatus, retryState: enumspb.RETRY_STATE_TIMEOUT,
					}, a.terminalOutcome(t))

					if testCase.expectedStatus == enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT {
						require.Equal(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, a.timeoutInfo(t).outcomeTimeout)
					}
				}

				s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
					s.T().Skip("TODO: workflow activity async handoff support")
				})

				s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
					t := s.T()
					env := newActivityParityEnv(t)
					check(t, newSAADriver(t, env, cfg).driveTrace(t, trace))
				})
			})
		}
	})
}
