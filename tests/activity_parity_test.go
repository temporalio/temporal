package tests

// SAA↔WFA parity repros. For each behavior at the intersection of the standalone activity (SAA) and
// the workflow activity (WFA), a test drives the same trace through both surfaces as "WorkflowActivity"
// and "StandaloneActivity" subtests and asserts the same public activity info. WFA is the oracle: the
// WorkflowActivity subtest blesses the expectation and the StandaloneActivity subtest proves the CHASM
// activity matches it.
//
// These use the full model-based drivers — the standalone driver (activity_standalone_driver.go), the
// workflow driver (activity_workflow_driver.go), driveTrace, and the archetype model
// (chasm/lib/activity/model). The bug-fix branches carry the same repros in a halfway-house form
// (ad-hoc per-surface drivers, no driveTrace/model) in a file of the same name; the two files are kept
// reasonably parallel so migrating from one to the other is a small step.
//
// They live on standaloneActivityTestSuite because its env enables the standalone activity (WFA needs
// nothing special); one SAA-enabled env drives both.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
)

// TestWFASAAStartToCloseTimeout ports a slice of Test_ActivityTimeouts: a started attempt
// exceeds its StartToClose timeout and, with no retries left, the activity ends TIMED_OUT. Both
// subtests must reach the same terminal status AND the same TimeoutType. WorkflowActivity is the
// oracle.
//
// Fidelity vs the original: covered — terminal status and the StartToClose TimeoutType (the semantic
// contract). Not covered: the other three timeout types (each is an additional scenario, not more
// fidelity here) and the failure *message* (SAA carries a proto message; WFA's SDK TimeoutError
// formats its own, so the strings differ by construction, not by behavior — the TimeoutType is the
// stable cross-surface discriminant).
func (s *standaloneActivityTestSuite) TestWFASAAStartToCloseTimeout() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.StartToCloseElapses}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 1, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 1}, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAAScheduleToCloseTimeout ports the schedule-to-close slice of Test_ActivityTimeouts
// (fired-during-run): the activity is started, then its ScheduleToClose deadline elapses while it
// runs, so it ends TIMED_OUT with the ScheduleToClose TimeoutType. Both subtests must reach the same
// status and type. (A never-started activity that hits the deadline times out as ScheduleToStart
// instead — on both surfaces — which is why the port polls first.)
func (s *standaloneActivityTestSuite) TestWFASAAScheduleToCloseTimeout() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.ScheduleToCloseElapses}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 1, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 1, HasScheduleToClose: true}, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAATimeoutPreservesUnderlyingFailureCause ports TestTimeoutPreservesUnderlyingFailureCause:
// when retries are exhausted by a StartToClose timeout, or a schedule-to-close deadline closes a
// backing-off activity, the terminal TimedOut failure chains the application failure that drove the
// retries as its Cause (mutable_state_impl.go AddActivityTaskTimedOutEvent, temporalio/temporal#3667).
// Both surfaces must agree; WorkflowActivity is the oracle.
func (s *standaloneActivityTestSuite) TestWFASAATimeoutPreservesUnderlyingFailureCause() {
	env := s.newTestEnv()

	// The underlying application failure driven on attempt 1 (see saaFailure); the terminal timeout must
	// chain it verbatim — both its Type and Message — as its Cause.
	wantCause := failureCause{Type: "drive", Message: "drive"}

	// assertCausePreserved drives the trace on both surfaces and asserts each ends TIMED_OUT with the
	// given timeout type, chaining wantCause.
	assertCausePreserved := func(t *testing.T, maxAttempts int32, trace []model.Event, timeoutType enumspb.TimeoutType) {
		want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: timeoutType.String()}
		t.Run("WorkflowActivity", func(t *testing.T) {
			h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: maxAttempts, retryInterval: 200 * time.Millisecond, shortTimeout: saaTimeoutIn(trace)}
			a := h.driveTrace(t, trace)
			require.Equal(t, want, a.terminal(t))
			require.Equal(t, wantCause, a.terminalCause(t), "the terminal timeout must chain the underlying application failure as its Cause")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			cfg := model.Config{MaxAttempts: maxAttempts}
			switch timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				cfg.HasScheduleToClose = true
			case enumspb.TIMEOUT_TYPE_HEARTBEAT:
				cfg.HasHeartbeat = true
			}
			h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: cfg, shortTimeout: saaTimeoutIn(trace)}
			a := h.driveTrace(t, trace)
			require.Equal(t, want, a.terminal(t))
			require.Equal(t, wantCause, a.terminalCause(t), "the terminal timeout must chain the underlying application failure as its Cause")
		})
	}

	// Retries exhausted by a StartToClose timeout on the final attempt (attempt 1 failed retryably).
	s.T().Run("StartToClose", func(t *testing.T) {
		assertCausePreserved(t, 2, []model.Event{saaPoll, saaFailRetryably, saaPoll, {Kind: model.StartToCloseElapses}}, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	})
	// Retries exhausted by a Heartbeat timeout on the final attempt: the attempt is started but never
	// heartbeats, so it times out — chaining the same cause via the heartbeat-timeout code path.
	s.T().Run("Heartbeat", func(t *testing.T) {
		assertCausePreserved(t, 2, []model.Event{saaPoll, saaFailRetryably, saaPoll, {Kind: model.HeartbeatElapses}}, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	})
	// Schedule-to-close deadline closes the activity while it backs off to retry — a distinct server
	// code path that must also chain the cause.
	s.T().Run("ScheduleToClose", func(t *testing.T) {
		assertCausePreserved(t, 0, []model.Event{saaPoll, saaFailRetryably, {Kind: model.ScheduleToCloseElapses}}, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
	})
}

// TestWFASAATimeoutTypeOnRetryDeadline ports the HeartbeatWithScheduleToClose slice of
// Test_ActivityTimeouts: a heartbeat timeout fires on a started attempt, but the retry interval
// cannot fit before the schedule-to-close deadline, so retries are given up and the terminal timeout
// is reported as ScheduleToClose — not Heartbeat. WorkflowActivity is the oracle (which has always
// done this via timer_queue_active_task_executor.go); the StandaloneActivity subtest is red until the
// SAA fix (fredtzeng/saa-timeout-on-retry) lands, since SAA currently reports the raw Heartbeat type.
func (s *standaloneActivityTestSuite) TestWFASAATimeoutTypeOnRetryDeadline() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.HeartbeatElapses}}
	// Heartbeat fires at ~2s; the 30s retry cannot fit before the 10s schedule-to-close deadline.
	const retryInterval, scheduleToClose = 30 * time.Second, 10 * time.Second
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 2, retryInterval: retryInterval, scheduleToClose: scheduleToClose, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 2, HasHeartbeat: true, HasScheduleToClose: true}, retryInterval: retryInterval, scheduleToClose: scheduleToClose, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAAQueuedRetryInterval exercises the divergence raised in review of the C5 fix: SCHEDULED
// spans both "backing off" and "dispatched to matching, not yet started". With a non-constant backoff
// (InitialInterval 5s, coefficient 2), once the first 5s backoff elapses and attempt 2 is queued, WFA
// recomputes CurrentRetryInterval prospectively via the backoff algorithm (10s) whereas SAA reports
// the served 5s. Our other tests use a constant interval and so cannot see this. WorkflowActivity is
// the oracle; the StandaloneActivity subtest is red until SAA recomputes (or nulls) the interval in
// the queued window — CurrentRetryInterval is the sole diverging field (state, attempt, and
// next-attempt-schedule all agree).

func (s *standaloneActivityTestSuite) TestWFASAAQueuedRetryInterval() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse}
	const initialInterval, maxInterval = 5 * time.Second, 30 * time.Second
	want := activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{
			env:                env,
			ctx:                testcontext.For(t),
			maxAttempts:        3,
			retryInterval:      initialInterval,
			backoffCoefficient: 2.0,
			maxRetryInterval:   maxInterval,
		}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{
			env:                env,
			ctx:                testcontext.For(t),
			idBase:             testcore.RandomizeStr(t.Name()),
			cfg:                model.Config{MaxAttempts: 3},
			retryInterval:      initialInterval,
			backoffCoefficient: 2.0,
			maxRetryInterval:   maxInterval,
		}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAAHeartBeat ports the core of TestActivityHeartBeatWorkflow_Success: a worker polls
// the activity and heartbeats a checkpoint payload; the checkpoint round-trips (readable while
// running); then the worker completes it and the activity ends COMPLETED. WorkflowActivity is the
// oracle; both subtests must observe the same heartbeat detail and the same terminal status. (The
// original also asserts the exact workflow history-event shape, which is not part of the shared
// contract.)
var heartbeatWant = []byte(`"hb"`) // == saaHeartbeatDetails

func (s *standaloneActivityTestSuite) TestWFASAAHeartBeat() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.Heartbeat}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: 2 * time.Second}
		a := h.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Event{Kind: model.RespondCompleted})
		require.Equal(t, want, a.terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: 2 * time.Second}
		a := h.driveTrace(t, trace)
		require.Equal(t, heartbeatWant, a.heartbeatDetails(t))
		a.driveEvent(t, model.Event{Kind: model.RespondCompleted})
		require.Equal(t, want, a.terminal(t))
	})
}

// TestWFASAARetry ports the core of TestWFASAARetry (functional test) to the equivalence
// framework: an attempt fails retryably, the backoff elapses, the next attempt fails non-retryably, and
// the activity ends FAILED with the application failure type. Both subtests must reach the same
// terminal status AND the same failure type. WorkflowActivity is the oracle.
//
// Fidelity vs the original: covered — the retryable-then-non-retryable -> FAILED path and the terminal
// application failure type. Not covered: the original's second activity (a schedule-to-start timeout on
// a no-worker queue — a separate scenario) and its assertions on workflow history-event shape, which is
// not part of the shared cross-surface contract.
func (s *standaloneActivityTestSuite) TestWFASAARetry() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse, saaPoll, saaFailNonRetryably}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, FailureType: "drive"}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAAHeartbeatTimeout ports the core of TestActivityHeartBeatWorkflow_Timeout: a started attempt
// heartbeats nothing within its HeartbeatTimeout and, with no retries left, the activity ends TIMED_OUT
// with the Heartbeat TimeoutType. Both subtests must reach the same status and type.
func (s *standaloneActivityTestSuite) TestWFASAAHeartbeatTimeout() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.HeartbeatElapses}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, FailureType: enumspb.TIMEOUT_TYPE_HEARTBEAT.String()}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 1, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 1, HasHeartbeat: true}, shortTimeout: saaTimeoutIn(trace)}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

// TestWFASAACancel ports the core of TestTryActivityCancellationFromWorkflow: a running
// activity is cancel-requested, the worker acknowledges (RespondActivityTaskCanceled), and the activity
// ends CANCELED. WorkflowActivity is the oracle; both subtests must reach CANCELED. The RequestCancel
// event realizes differently per surface — SAA's direct RequestCancelActivityExecution RPC vs WFA's
// workflow-driven cancel (signal -> RequestCancelActivity) — which is exactly the driver's job to hide.
//
// Fidelity vs the original: covered — the cancel-then-acknowledge -> CANCELED path. Not covered: the
// original also asserts the workflow observed the cancellation (workflow-level, not the activity's
// cross-surface contract).
func (s *standaloneActivityTestSuite) TestWFASAACancel() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, {Kind: model.RequestCancel}, {Kind: model.RespondCanceled}}
	want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 1}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 1}}
		require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
	})
}

// retryAfterFail: attempt 1 fails retryably, the backoff elapses, attempt 2 starts. The activity is
// then running its second attempt — no pending retry — so there is no current retry interval and no
// next-attempt schedule time.

func (s *standaloneActivityTestSuite) TestWFASAARetryAfterFail() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse, saaPoll}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_STARTED,
		Attempt:                2,
		CurrentRetryInterval:   0,
		NextAttemptScheduleSet: false,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
}

// backingOff: attempt 1 fails retryably, and we observe during the backoff window (before it elapses,
// so the next dispatch is still in the future). The retry is genuinely pending, so both the current
// retry interval and the next-attempt schedule time are populated — the case where C5 says the two
// products agree. The long interval keeps the window open across the describe. Expected fully green.

func (s *standaloneActivityTestSuite) TestWFASAABackingOff() {
	env := s.newTestEnv()
	backingOffInterval := 30 * time.Second
	trace := []model.Event{saaPoll, saaFailRetryably}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                2,
		CurrentRetryInterval:   backingOffInterval,
		NextAttemptScheduleSet: true,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: backingOffInterval}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: backingOffInterval}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
}

// nextRetryDelayOverride: the worker fails with a next_retry_delay that overrides the policy backoff
// (policy 5s, override 30s); observed during the override-length window. Both products must honor the
// override identically — the resulting current retry interval is 30s, not the policy's 5s. Expected
// fully green.
func (s *standaloneActivityTestSuite) TestWFASAANextRetryDelayOverride() {
	env := s.newTestEnv()
	nextRetryDelayOverride := 30 * time.Second
	trace := []model.Event{saaPoll, saaFailRetryably}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		Attempt:                2,
		CurrentRetryInterval:   nextRetryDelayOverride,
		NextAttemptScheduleSet: true,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: 5 * time.Second, nextRetryDelay: nextRetryDelayOverride}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: 5 * time.Second, nextRetryDelay: nextRetryDelayOverride}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
}

// firstAttemptStarted: a worker polls the first attempt, which is now running. No attempt has failed,
// so there is no current retry interval and no next-attempt schedule time. The baseline running-state
// equivalence. Expected fully green.
func (s *standaloneActivityTestSuite) TestWFASAAFirstAttemptStarted() {
	env := s.newTestEnv()
	trace := []model.Event{saaPoll}
	want := activityInfoProjection{
		State:                  enumspb.PENDING_ACTIVITY_STATE_STARTED,
		Attempt:                1,
		CurrentRetryInterval:   0,
		NextAttemptScheduleSet: false,
	}

	s.T().Run("WorkflowActivity", func(t *testing.T) {
		h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
	s.T().Run("StandaloneActivity", func(t *testing.T) {
		h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: 2 * time.Second}
		require.Equal(t, want, h.driveTrace(t, trace).projection(t))
	})
}

// TestWFASAANextAttemptScheduleTimeAndCurrentRetryInterval sweeps NextAttemptScheduleTime and
// CurrentRetryInterval across the activity lifecycle, comparing SAA against WFA (the oracle) at each
// point. Each scenario drives the same trace through both surfaces and asserts the same public info.
// The running-state scenarios are the C5 divergence: WFA reports no pending retry while an attempt
// runs, whereas SAA leaks the preceding backoff's retry-scheduling metadata — so those SAA subtests
// are expected red until C5 is fixed. StartDelayPending and PausedDuringBackoff are standalone-only
// (WFA has no per-activity start delay, and the WFA driver has no operator pause).
func (s *standaloneActivityTestSuite) TestWFASAANextAttemptScheduleTimeAndCurrentRetryInterval() {
	env := s.newTestEnv()
	t := s.T()

	// both drives a trace through the WFA oracle and the SAA surface, asserting each reports want.
	both := func(t *testing.T, maxAttempts int32, retryInterval time.Duration, trace []model.Event, want activityInfoProjection) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: maxAttempts, retryInterval: retryInterval}
			require.Equal(t, want, h.driveTrace(t, trace).projection(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: maxAttempts}, retryInterval: retryInterval}
			require.Equal(t, want, h.driveTrace(t, trace).projection(t))
		})
	}

	// First attempt within its start delay: the dispatch is pending in the future and is not a retry.
	t.Run("StartDelayPending", func(t *testing.T) {
		info := s.driveTrace(t, env, saaTrace{trace: []model.Event{}, startDelayed: true}).describe(t).GetInfo()
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, info.GetRunState())
		require.Equal(t, info.GetExecutionTime().AsTime(), info.GetNextAttemptScheduleTime().AsTime(),
			"during a start delay, NextAttemptScheduleTime is the pending dispatch time (schedule+delay)")
		require.Nil(t, info.GetCurrentRetryInterval(), "the first attempt is not a retry")
	})

	// First attempt running: no pending next dispatch, and no preceding backoff, so no retry interval.
	t.Run("FirstAttemptRunning", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{saaPoll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 1})
	})

	// Backing off before the retry dispatches: the retry is genuinely pending, so both the interval and
	// the next-attempt schedule time are populated. The case where the two products agree.
	t.Run("BackingOffBeforeRetry", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{saaPoll, saaFailRetryably},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2, CurrentRetryInterval: saaDelayWindow, NextAttemptScheduleSet: true})
	})

	// Retry dispatched to matching but not yet polled: schedulable now, so no future dispatch time.
	t.Run("RetryQueuedNotStarted", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, Attempt: 2, CurrentRetryInterval: saaDelayWindow})
	})

	// Retry attempt running with a further retry permitted: nothing pending (C5 — SAA leaks the backoff's
	// metadata here).
	t.Run("RetryAttemptRunning", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse, saaPoll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2})
	})

	// Final attempt running with no retry remaining: nothing pending (C5 — SAA leaks metadata here too).
	t.Run("FinalAttemptRunning", func(t *testing.T) {
		both(t, 2, saaDelayWindow, []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse, saaPoll},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_STARTED, Attempt: 2})
	})

	// Completed after a retry: terminal, nothing pending.
	t.Run("Completed", func(t *testing.T) {
		trace := []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse, saaPoll, saaComplete}
		want := activityTerminalProjection{Status: enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED}
		t.Run("WorkflowActivity", func(t *testing.T) {
			h := &wfaHarness{env: env, ctx: testcontext.For(t), maxAttempts: 3, retryInterval: saaDelayWindow}
			require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			h := &saaHarness{env: env, ctx: testcontext.For(t), idBase: testcore.RandomizeStr(t.Name()), cfg: model.Config{MaxAttempts: 3}, retryInterval: saaDelayWindow}
			require.Equal(t, want, h.driveTrace(t, trace).terminal(t))
		})
	})

	// Paused while backing off. There is no next attempt set to occur, and it's not appropriate to
	// report the current retry interval since dispatch will not occur while paused.
	t.Run("PausedDuringBackoff", func(t *testing.T) {
		both(t, 3, saaDelayWindow, []model.Event{saaPoll, saaFailRetryably, saaPause},
			activityInfoProjection{State: enumspb.PENDING_ACTIVITY_STATE_PAUSED, Attempt: 2})
	})
}
