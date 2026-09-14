package tests

// Tests ported from tests/activity_standalone_test.go into the SAA <-> WFA parity framework. Each
// keeps the name it had there: the parity suite is a different type, so the two never collide, and
// the shared name says which standalone test the parity claim came from.
//
// A standalone test becomes a parity test when what it asserts is a property of the Activity
// component rather than of the standalone surface, and when both drivers can express it. Assertions
// that read a field only one surface has (state transition counts, TotalHeartbeatCount, task tokens
// reused across attempts, cross-namespace tokens) are dropped, and each drop is noted where it
// matters.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/tests/testcore"
)

// parityActivity is what a ported test reads from a driven activity. Both drivers' handles satisfy
// it, so a ported test states its claim once instead of once per implementation.
type parityActivity interface {
	driverState() *activityDriverState
	testContext() context.Context
	driveEvent(testing.TB, model.Event)
	rpc(testing.TB, model.Event) error
	pollForTask(require.TestingT, time.Duration) *workflowservice.PollActivityTaskQueueResponse
	activityInfo(require.TestingT) activityInfo
	terminal(require.TestingT) activityTerminalProjection
	heartbeatFlags() model.HeartbeatFlags
}

// parityDrive drives trace through both implementations and hands each resulting activity to check.
func parityDrive(
	t *testing.T,
	env *testcore.TestEnv,
	cfg activityConfig,
	trace []model.Event,
	check func(*testing.T, parityActivity),
) {
	t.Run("WorkflowActivity", func(t *testing.T) {
		check(t, newWFADriver(t, env, cfg).driveTrace(t, trace))
	})
	t.Run("StandaloneActivity", func(t *testing.T) {
		check(t, newSAADriver(t, env, cfg).driveTrace(t, trace))
	})
}

// parityDriveOutlivingActivity is parityDrive for a test that acts on the activity after it has
// closed. The WFA wrapper workflow must outlive its activity, or the RPC under test is answered about
// a workflow that no longer exists, which says nothing about how a closed activity behaves.
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

// establishRequestID records the last operator RPC of this type as the one that established the
// current state, so a following SameRequestID event replays its request id. The conformance engine
// does this as it explores; a trace-driven test has to say it.
func establishRequestID(a parityActivity, eventType model.EventType) {
	state := a.driverState()
	state.establishedReqID[eventType] = state.lastReqID
}

func requireFailedPrecondition(t require.TestingT, err error) {
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
}

// scheduleToCloseWithRetryWindow must outlast a poll, a retryable failure, one retry backoff and a
// second poll, and then expire while that second attempt runs.
var scheduleToCloseWithRetryWindow = 6 * activityShortDispatchDelay

// TestRetryWithoutScheduleToCloseTimeout ports the standalone test of the same name: an activity with
// no schedule-to-close deadline still retries a retryable attempt failure, so the second attempt is
// dispatched and pollable.
func (s *activityParityTestSuite) TestRetryWithoutScheduleToCloseTimeout() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 2}
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll}

	parityDrive(s.T(), env, cfg, trace, func(t *testing.T, a parityActivity) {
		require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2},
			a.activityInfo(t), "an activity with no schedule-to-close deadline must still retry")
	})
}

// Test_ScheduleToCloseTimeout_WithRetry ports the standalone test of the same name: the
// schedule-to-close deadline closes the activity while a retry attempt is running, so the terminal
// timeout is ScheduleToClose and not the per-attempt timeout that would otherwise have ended it.
func (s *activityParityTestSuite) Test_ScheduleToCloseTimeout_WithRetry() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{
		MaxAttempts:     2,
		RetryInterval:   activityShortDispatchDelay,
		ScheduleToClose: scheduleToCloseWithRetryWindow,
	}
	trace := []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.ScheduleToCloseElapses}
	expected := activityTerminalProjection{
		Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
		RetryState:  enumspb.RETRY_STATE_TIMEOUT,
	}

	parityDrive(s.T(), env, cfg, trace, func(t *testing.T, a parityActivity) {
		require.Equal(t, expected, a.terminal(t))
	})
}

// TestStartToCloseTimeout_WhileCancelRequested ports the standalone test of the same name: a worker
// that ignores a cancellation request still has its attempt ended by the start-to-close timeout, and
// the activity closes TIMED_OUT reporting cancellation as the reason it stopped retrying.
func (s *activityParityTestSuite) TestStartToCloseTimeout_WhileCancelRequested() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 1}
	trace := []model.Event{model.Poll, model.RequestCancel, model.StartToCloseElapses}
	expected := activityTerminalProjection{
		Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		FailureType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String(),
		RetryState:  enumspb.RETRY_STATE_CANCEL_REQUESTED,
	}

	parityDrive(s.T(), env, cfg, trace, func(t *testing.T, a parityActivity) {
		require.Equal(t, expected, a.terminal(t),
			"an activity in CANCEL_REQUESTED must still time out via START_TO_CLOSE")
	})
}

// TestScheduleToStartTimeout ports the standalone test of the same name: an activity no worker ever
// polls closes TIMED_OUT with the ScheduleToStart TimeoutType once its schedule-to-start window ends.
func (s *activityParityTestSuite) TestScheduleToStartTimeout() {
	env := newActivityParityEnv(s.T())
	trace := []model.Event{model.ScheduleToStartElapses}
	expected := activityTerminalProjection{
		Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START.String(),
		RetryState:  enumspb.RETRY_STATE_TIMEOUT,
	}

	parityDrive(s.T(), env, activityConfig{}, trace, func(t *testing.T, a parityActivity) {
		require.Equal(t, expected, a.terminal(t))
	})
}

// TestComplete ports the standalone test of the same name. Its stale-token, absent-run-id and
// cross-namespace-token subtests are about the standalone token and request shape rather than about
// the component, so only the completion paths are ported.
func (s *activityParityTestSuite) TestComplete() {
	t := s.T()
	env := newActivityParityEnv(t)
	completed := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}

	assertCompleted := func(t *testing.T, a parityActivity) {
		require.Equal(t, completed, a.terminal(t))
	}

	t.Run("ByToken", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Complete}, assertCompleted)
	})

	t.Run("ByIDWithRunID", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.CompleteByID}, assertCompleted)
	})

	// By-ID completion must succeed on attempt 2+, not only on the first attempt: the server
	// synthesizes the token from the activity's current attempt rather than assuming attempt 1.
	t.Run("ByIDAfterRetry", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 3},
			[]model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, model.CompleteByID},
			assertCompleted)
	})
}

// TestFail ports the standalone test of the same name, for the same reasons TestComplete ports only
// its completion paths. Its WithHeartbeatDetails subtest is already covered, for both surfaces, by
// TestLastHeartbeatDetailsPersistedOnAttemptFailure.
func (s *activityParityTestSuite) TestFail() {
	t := s.T()
	env := newActivityParityEnv(t)
	failNonRetryablyByID := model.Event{Type: model.RespondFailedByIDType, Failure: &model.Failure{NonRetryable: true}}
	failed := activityTerminalProjection{
		Status:      enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
		FailureType: "TestFailure",
		RetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
	}

	assertFailed := func(t *testing.T, a parityActivity) {
		require.Equal(t, failed, a.terminal(t))
	}

	t.Run("ByToken", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.FailNonRetryably}, assertFailed)
	})

	t.Run("ByIDWithRunID", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, failNonRetryablyByID}, assertFailed)
	})

	// As TestComplete/ByIDAfterRetry: the by-ID failure path must work on a later attempt too.
	t.Run("ByIDAfterRetry", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 3},
			[]model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll, failNonRetryablyByID},
			assertFailed)
	})
}

// TestHeartbeat ports the standalone test of the same name. Its stale-token and cross-namespace-token
// subtests are token-shape tests; HeartbeatKeepsActivityAlive and
// HeartbeatWithNoTimeoutDoesNotKillActivity turn on wall-clock sleeps inside a heartbeat window, which
// the trace vocabulary has no event for; HeartbeatDetailsAvailableOnRetry asserts on a poll response
// field the drivers do not surface; and ActivityTimesOutWithoutHeartbeat and
// HeartbeatTimeoutReportsScheduleToCloseWhenRetryCannotFit are already ported as
// TestParityHeartbeatTimeout and TestParityTimeoutTypeOnInsufficientTimeForRetry.
func (s *activityParityTestSuite) TestHeartbeat() {
	t := s.T()
	env := newActivityParityEnv(t)

	// A heartbeat tells the worker whether cancellation has been requested, so a worker that polls
	// only by heartbeating still learns of it.
	t.Run("ResponseIncludesCancelRequested", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Heartbeat},
			func(t *testing.T, a parityActivity) {
				require.False(t, a.heartbeatFlags().CancelRequested,
					"no cancellation has been requested yet")
				a.driveEvent(t, model.RequestCancel)
				a.driveEvent(t, model.Heartbeat)
				require.True(t, a.heartbeatFlags().CancelRequested,
					"a heartbeat after a cancellation request must report it")
			})
	})

	// A heartbeat timeout is a retryable attempt failure, so with attempts remaining the activity
	// comes back and is pollable rather than closing.
	t.Run("ActivityRetriesOnHeartbeatTimeout", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 2},
			[]model.Event{model.Poll, model.HeartbeatElapses, model.BackoffElapses, model.Poll},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2},
					a.activityInfo(t))
			})
	})

	// The terminal heartbeat timeout carries the last checkpoint the worker reported, so a client
	// consuming the failure can resume from it. The two surfaces expose it through unrelated shapes —
	// SAA's TimeoutFailureInfo, the SDK's TimeoutError — so each arm reads its own.
	t.Run("HeartbeatDetailsSurfacedOnTimeout", func(t *testing.T) {
		cfg := activityConfig{MaxAttempts: 1}
		trace := []model.Event{model.Poll, model.Heartbeat, model.HeartbeatElapses}
		expected := activityTerminalProjection{
			Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
			FailureType: enumspb.TIMEOUT_TYPE_HEARTBEAT.String(),
			RetryState:  enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
		}
		const surfaced = "the terminal heartbeat timeout must carry the last reported checkpoint"

		t.Run("WorkflowActivity", func(t *testing.T) {
			a := newWFADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, expected, a.terminal(t))
			var timeoutErr *temporal.TimeoutError
			require.ErrorAs(t, a.run.Get(a.testContext(), nil), &timeoutErr)
			var got string
			require.NoError(t, timeoutErr.LastHeartbeatDetails(&got), surfaced)
			require.Equal(t, activityRecordedHeartbeatDetailsString, got, surfaced)
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			a := newSAADriver(t, env, cfg).driveTrace(t, trace)
			require.Equal(t, expected, a.terminal(t))
			timeoutFailure := a.describe(t).GetOutcome().GetFailure().GetTimeoutFailureInfo()
			require.Equal(t, heartbeatWant, firstPayloadData(timeoutFailure.GetLastHeartbeatDetails()), surfaced)
		})
	})
}

// TestPauseActivityExecution ports the standalone test of the same name. Its request-validation
// subtests are frontend argument checks rather than component behavior, and its
// UpdateOptionsPreservesTimeoutsWhilePauseRequested subtest turns on the exact timeout the update
// installs, which the trace vocabulary's UpdateOptions event does not choose.
func (s *activityParityTestSuite) TestPauseActivityExecution() {
	t := s.T()
	env := newActivityParityEnv(t)

	// Pausing a started activity cannot take the attempt away from the worker that owns it, so the
	// pause is pending: Describe reports PAUSE_REQUESTED, and the worker learns of it by heartbeating.
	t.Run("PauseWhileStarted", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Pause},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, a.activityInfo(t).RunState)
				a.driveEvent(t, model.Heartbeat)
				require.True(t, a.heartbeatFlags().ActivityPaused,
					"a heartbeat during a pending pause must report it")
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, a.activityInfo(t).RunState,
					"heartbeating does not resolve the pending pause")
			})
	})

	// Pausing a scheduled activity takes effect at once, and the pending dispatch it invalidated must
	// not reach a poller.
	t.Run("PauseWhileScheduled", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration},
			[]model.Event{model.Pause},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 1},
					a.activityInfo(t))
				require.Nil(t, a.pollForTask(t, activityDriverTimeout),
					"no task may be dispatched while the activity is paused")
			})
	})

	// A second pause carrying a fresh request id is a new request to pause an already-paused
	// activity, so it is refused rather than silently accepted.
	t.Run("PauseWhilePaused", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Pause},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Pause))
			})
	})

	// The same pause arriving twice is one pause: the request id makes the repeat a no-op success.
	t.Run("PauseWhilePausedIdempotent", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Pause},
			func(t *testing.T, a parityActivity) {
				establishRequestID(a, model.PauseType)
				a.driveEvent(t, model.Event{Type: model.PauseType, SameRequestID: true})
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 1},
					a.activityInfo(t))
			})
	})

	// A pause request that is redelivered after the activity has been unpaused must be recognized as
	// the one already applied, not re-pause the activity. The transition model does not capture this
	// dedup — it reads a pause of a scheduled activity as a fresh pause — so the claim is asserted
	// here rather than left to the driver's model check.
	t.Run("PauseReplayAfterUnpause_IsDeduplicated", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration},
			[]model.Event{model.Pause},
			func(t *testing.T, a parityActivity) {
				establishRequestID(a, model.PauseType)
				a.driveEvent(t, model.Unpause)
				a.driveEvent(t, model.Event{Type: model.PauseType, SameRequestID: true})
				require.NotEqual(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, a.activityInfo(t).RunState,
					"a replayed pause whose request id was already consumed must not re-pause the activity")
			})
	})

	// A pause requested while a worker owns the attempt takes effect when that attempt yields: the
	// retry lands in PAUSED rather than being dispatched, and unpausing releases it.
	//
	// The standalone test also asserts LastFailure is recorded on the paused retry. There is no
	// PendingActivityInfo counterpart to compare it against, so it is dropped here; the retry landing
	// on attempt 2 is what both surfaces report.
	t.Run("PauseWhileRunning", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 10, RetryInterval: activityShortDispatchDelay},
			[]model.Event{model.Poll, model.Pause, model.Heartbeat, model.FailRetryably},
			func(t *testing.T, a parityActivity) {
				require.True(t, a.heartbeatFlags().ActivityPaused)
				// The trace heartbeated, so the retry carries that checkpoint forward.
				paused := activityInfo{
					RunState:             enumspb.PENDING_ACTIVITY_STATE_PAUSED,
					Attempt:              2,
					LastHeartbeatDetails: activityMarshalPayloads(activityHeartbeatDetails),
				}
				require.Equal(t, paused, a.activityInfo(t),
					"the pause takes effect on the retry rather than being dropped")
				a.driveEvent(t, model.Unpause)
				a.driveEvent(t, model.BackoffElapses)
				a.driveEvent(t, model.Poll)
				started := paused
				started.RunState = enumspb.PENDING_ACTIVITY_STATE_STARTED
				require.Equal(t, started, a.activityInfo(t),
					"unpausing must release the retry to a poller")
			})
	})

	// PauseIncreaseAttemptsOnFailure is PauseWhileRunning seen from the attempt counter: an attempt
	// that fails while a pause is pending is still an attempt, so the count advances as it would have
	// without the pause.
	t.Run("PauseIncreaseAttemptsOnFailure", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 10, RetryInterval: activityLongDuration},
			[]model.Event{model.Poll, model.Pause},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, Attempt: 1},
					a.activityInfo(t))
				a.driveEvent(t, model.FailRetryably)
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2},
					a.activityInfo(t), "failing under a pending pause still consumes an attempt")
			})
	})

	// Pausing an activity that is already waiting out a retry backoff holds the retry back, and
	// unpausing releases it once the backoff it was already serving has elapsed.
	t.Run("PauseWhileWaiting", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 10, RetryInterval: activityShortTimeout},
			[]model.Event{model.Poll, model.FailRetryably, model.Pause},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2},
					a.activityInfo(t))
				a.driveEvent(t, model.Unpause)
				a.driveEvent(t, model.BackoffElapses)
				a.driveEvent(t, model.Poll)
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2},
					a.activityInfo(t))
			})
	})

	// Cancellation outranks pausing: once a cancellation is pending there is nothing to pause for, so
	// the pause is refused.
	t.Run("PauseWhileCancelRequested", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Pause))
			})
	})

	// A paused activity has no attempt in flight, so there is no worker to wait for: the cancellation
	// is applied rather than left pending.
	t.Run("CancelWhilePaused", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration},
			[]model.Event{model.Pause, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED},
					a.terminal(t))
			})
	})

	// A pending reset is likewise not a state to pause from.
	t.Run("PauseWhileResetRequested", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 2},
			[]model.Event{model.Poll, model.Reset},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Pause))
			})
	})

	// Updating options is refused once a cancellation is pending, and the refusal must change nothing.
	t.Run("UpdateWhileCancelRequestedFails", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.UpdateOptions))
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED, a.activityInfo(t).RunState,
					"a refused update must not disturb the pending cancellation")
			})
	})

	// A pending pause is advisory: the worker's token stays valid, so a worker that finishes anyway
	// completes the activity.
	t.Run("CompleteWhileStartedAndPaused", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Pause},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED, a.activityInfo(t).RunState)
				a.driveEvent(t, model.Complete)
				require.Equal(t, activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
					a.terminal(t))
			})
	})

	// A non-retryable failure closes the activity even under a pending pause: there is no retry for the
	// pause to take effect on.
	//
	// The standalone test also asserts the attempt count did not advance. Neither surface reports an
	// attempt count once the activity has closed, so that is dropped.
	t.Run("NonRetryableFailWhilePaused", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 10},
			[]model.Event{model.Poll, model.Pause, model.FailNonRetryably},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityTerminalProjection{
					Status:      enumspb.ACTIVITY_EXECUTION_STATUS_FAILED,
					FailureType: "TestFailure",
					RetryState:  enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
				}, a.terminal(t))
			})
	})

	// Pausing suspends dispatch, not the schedule-to-close deadline: the deadline still closes the
	// activity while it is paused.
	t.Run("ScheduleToCloseTimeoutWhilePaused", func(t *testing.T) {
		parityDrive(t, env, activityConfig{RetryInterval: activityLongDuration},
			[]model.Event{model.Pause, model.ScheduleToCloseElapses},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityTerminalProjection{
					Status:      enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
					FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String(),
					RetryState:  enumspb.RETRY_STATE_TIMEOUT,
				}, a.terminal(t), "a paused activity is still subject to its schedule-to-close deadline")
			})
	})

	// A pending pause must not cost the running attempt its server-side timeout enforcement: the
	// start-to-close timeout still fires, and the retry it schedules is where the pause takes effect.
	t.Run("StartToCloseTimeoutWhilePauseRequested", func(t *testing.T) {
		parityDrive(t, env,
			activityConfig{MaxAttempts: 10, RetryInterval: activityShortDispatchDelay, StartToClose: activityShortTimeout},
			[]model.Event{model.Poll, model.Pause, model.StartToCloseElapses},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2},
					a.activityInfo(t))
			})
	})

	// As StartToCloseTimeoutWhilePauseRequested, for the heartbeat timer.
	t.Run("HeartbeatTimeoutWhilePauseRequested", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 10, RetryInterval: activityShortDispatchDelay},
			[]model.Event{model.Poll, model.Pause, model.HeartbeatElapses},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2},
					a.activityInfo(t))
			})
	})

	// A keepPaused reset requested while a pause is pending is deferred with the pause, and the two are
	// consumed together when the attempt ends: the activity lands PAUSED on a fresh attempt 1.
	t.Run("ResetKeepPausedWhilePauseRequested", func(t *testing.T) {
		parityDrive(t, env,
			activityConfig{
				MaxAttempts:   10,
				RetryInterval: activityShortDispatchDelay,
				StartToClose:  2 * activityShortTimeout,
			},
			[]model.Event{
				model.Poll, model.FailRetryably, model.BackoffElapses, model.Poll,
				model.Pause, model.ResetKeepPaused, model.StartToCloseElapses,
			},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 1},
					a.activityInfo(t), "the deferred keepPaused reset must land the activity paused on attempt 1")
			})
	})

	// Pause is refused on a closed activity: there is no longer anything to suspend.
	t.Run("PauseTerminalState", func(t *testing.T) {
		parityDriveOutlivingActivity(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Complete},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Pause))
			})
	})
}

// TestUnpauseActivityExecution ports the standalone test of the same name. Its request-validation
// subtests are frontend argument checks; UnpauseNonPausedActivityFails is already ported as
// TestUnpauseWithoutPause.
func (s *activityParityTestSuite) TestUnpauseActivityExecution() {
	t := s.T()
	env := newActivityParityEnv(t)

	// Unpausing a paused activity that never started returns it to Scheduled and releases its
	// dispatch, so a poller can pick it up.
	t.Run("UnpauseWhileScheduled", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1, RetryInterval: activityLongDuration},
			[]model.Event{model.Pause, model.Unpause},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1},
					a.activityInfo(t))
				a.driveEvent(t, model.Poll)
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1},
					a.activityInfo(t))
			})
	})

	// A pending pause is the one pause state that can be withdrawn: unpausing a started activity
	// leaves the attempt exactly where it was.
	t.Run("UnpauseWhileStarted", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Pause, model.Unpause},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1},
					a.activityInfo(t))
			})
	})

	// A pending cancellation is not a pause, so there is nothing for unpause to undo.
	t.Run("UnpauseWhileCancelRequestedFails", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Unpause))
			})
	})
}

// TestResetActivityExecution ports the standalone test of the same name. Its restore-original-options
// subtests turn on which option was changed, which the trace vocabulary's UpdateOptions event does not
// choose; its token subtests are about the standalone token shape.
func (s *activityParityTestSuite) TestResetActivityExecution() {
	t := s.T()
	env := newActivityParityEnv(t)

	// Resetting a started activity cannot take the attempt away from its worker, so the reset is
	// deferred: the attempt runs on, and the reset is applied when it ends, landing a fresh attempt 1
	// that dispatches at once rather than serving the failed attempt's backoff.
	t.Run("WhileRunning", func(t *testing.T) {
		parityDrive(t, env, activityConfig{RetryInterval: activityLongDuration},
			[]model.Event{model.Poll, model.Reset},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_STARTED, a.activityInfo(t).RunState,
					"the reset is deferred, so the running attempt is untouched")
				a.driveEvent(t, model.FailRetryably)
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1},
					a.activityInfo(t))
				a.driveEvent(t, model.Poll)
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1},
					a.activityInfo(t), "the reset attempt must dispatch without waiting out the retry backoff")
			})
	})

	// Cancellation outranks reset, and the refusal must leave the attempt intact: its token still
	// completes the activity.
	t.Run("WhileCancelRequestedReturnsFailedPrecondition", func(t *testing.T) {
		parityDrive(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.RequestCancel},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Reset))
				a.driveEvent(t, model.Complete)
				require.Equal(t, activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED},
					a.terminal(t))
			})
	})

	// Resetting an activity that is waiting out a long retry backoff discards that backoff: the reset
	// attempt is dispatchable immediately.
	t.Run("InRetryWithLongInterval", func(t *testing.T) {
		parityDrive(t, env, activityConfig{RetryInterval: activityLongDuration},
			[]model.Event{model.Poll, model.FailRetryably, model.Reset},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 1},
					a.activityInfo(t))
				a.driveEvent(t, model.Poll)
				require.Equal(t, activityInfo{RunState: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1},
					a.activityInfo(t), "the reset attempt must not wait out the discarded backoff")
			})
	})

	// A reset starts the activity over, so the checkpoint the previous attempts reported is no longer
	// theirs to resume from and must be cleared.
	t.Run("ResetClearsHeartbeatDetails", func(t *testing.T) {
		parityDrive(t, env, activityConfig{RetryInterval: activityLongDuration},
			[]model.Event{model.Poll, model.Heartbeat, model.FailRetryably},
			func(t *testing.T, a parityActivity) {
				require.Equal(t, activityMarshalPayloads(activityRecordedHeartbeatDetails),
					a.activityInfo(t).LastHeartbeatDetails)
				a.driveEvent(t, model.Reset)
				require.Nil(t, a.activityInfo(t).LastHeartbeatDetails,
					"a reset must clear the heartbeat checkpoint it is starting over from")
			})
	})

	// Reset is refused on a closed activity: there is nothing left to start over.
	t.Run("TerminalStateReturnsFailedPrecondition", func(t *testing.T) {
		parityDriveOutlivingActivity(t, env, activityConfig{MaxAttempts: 1},
			[]model.Event{model.Poll, model.Complete},
			func(t *testing.T, a parityActivity) {
				requireFailedPrecondition(t, a.rpc(t, model.Reset))
			})
	})
}
