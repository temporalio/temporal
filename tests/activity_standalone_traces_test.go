// Standalone-activity traces: behavior with no workflow-activity counterpart, so nothing to compare
// against. Each declarative trace is checked against chasm/lib/activity/model at every step.
package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
)

// saaTraceBudget is the context budget for a group of declarative traces. They pay real wall-clock
// waits, so a group can run a few minutes past the default per-test timeout.
func saaTraceBudget() time.Duration {
	const floor = 8 * time.Minute
	if d := testcontext.DefaultTimeout(); d > floor {
		return d
	}
	return floor
}

// driveTrace drives one declared trace on its own driver and returns a handle at the reached state. It
// checks conformance to the model at every step, unless customizeStart is set.
func (s *activityParityTestSuite) driveTrace(t *testing.T, env *testcore.TestEnv, tr saaTrace) *saaHandle {
	d := newSAADriver(t, env, tr.config())
	d.customizeStart = tr.customizeStart
	// Bound the positive poll below the delay window, so that "Dispatchable" means "dispatches promptly".
	d.positivePollTimeout = saaPollTimeout
	// customizeStart injects config the model cannot see, so drive model-free and leave the assertions to
	// the caller.
	if tr.customizeStart != nil {
		return d.driveTrace(t, tr.trace)
	}
	return d.driveTraceWithModelConformanceChecking(t, tr.trace)
}

// TestSAAWorkerMustSendApplicationFailure: a worker failing an attempt must send an
// ApplicationFailureInfo failure, and a server failure is rejected. SAA-only worker-side RPC validation.
func (s *activityParityTestSuite) TestSAAWorkerMustSendApplicationFailure() {
	env := newActivityParityEnv(s.T())
	a := s.driveTrace(s.T(), env, saaTrace{trace: []model.Event{model.Poll}, cfg: activityConfig{MaxAttempts: 3}})
	_, err := env.FrontendClient().RespondActivityTaskFailed(testcontext.For(s.T()), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: a.token,
		Identity:  "worker",
		Failure: &failurepb.Failure{
			Message:     "server failure",
			FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{NonRetryable: false}},
		},
	})
	require.ErrorContains(s.T(), err, "Failure must have ApplicationFailureInfo")
}

// TestStartDelay_Declarative drives the start-delay scenarios, each a named subtest with its trace
// declared inline. Each step is model-checked by driveTrace; there are no further assertions. SAA-only:
// WFA has no per-activity start delay.
func (s *activityParityTestSuite) TestStartDelay_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := newActivityParityEnv(s.T())
	t := s.T()

	t.Run("start-delay/first-dispatch", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{model.Poll, model.StartDelayElapses, model.Poll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/pause-then-unpause", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{model.Pause, {Type: model.UnpauseType}, model.Poll, model.StartDelayElapses, model.Poll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/reset", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{{Type: model.ResetType}, model.Poll, model.StartDelayElapses, model.Poll},
			startDelayed: true,
		})
	})
	t.Run("start-delay/update-while-paused", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{model.Pause, {Type: model.UpdateOptionsType, SetsStartDelay: true}},
			startDelayed: true,
		})
	})
	t.Run("start-delay/update-then-restore-original", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{
				{Type: model.UpdateOptionsType, SetsStartDelay: true},
				{Type: model.UpdateOptionsType, RestoreOriginal: true},
				model.Poll, model.StartDelayElapses, model.Poll,
			},
			startDelayed: true,
		})
	})
}

// TestBackoff_Declarative drives the retry-backoff scenarios, including the operator commands during a
// backoff. Model-checked by driveTrace.
func (s *activityParityTestSuite) TestBackoff_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := newActivityParityEnv(s.T())
	t := s.T()

	t.Run("backoff/retry-dispatch", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, model.Poll, model.BackoffElapses, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow},
		})
	})
	t.Run("backoff/next-retry-delay-override", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, model.Poll, model.BackoffElapses, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, NextRetryDelay: activityDelayWindow},
		})
	})
	// Paused mid-backoff: the unpause resumes waiting, so the next poll must find nothing until the
	// remaining window elapses.
	t.Run("backoff/pause-before-dispatch-then-unpause", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, model.Pause, {Type: model.UnpauseType}, model.Poll, model.BackoffElapses, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow},
		})
	})
	// The counterpart: paused after the backoff already elapsed, so the unpause must dispatch at once
	// rather than impose a fresh window.
	t.Run("backoff/pause-after-dispatch-then-unpause", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses, model.Pause, {Type: model.UnpauseType}, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow},
		})
	})
	t.Run("backoff/pause-unpause-then-update", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, model.Pause, {Type: model.UnpauseType}, {Type: model.UpdateOptionsType}, model.Poll, model.BackoffElapses, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow},
		})
	})
	t.Run("backoff/next-retry-delay-override-then-update", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, {Type: model.UpdateOptionsType}, model.Poll, model.BackoffElapses, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, NextRetryDelay: activityDelayWindow},
		})
	})
	t.Run("backoff/reset", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.FailRetryably, {Type: model.ResetType}, model.Poll},
			cfg:   activityConfig{MaxAttempts: 3, RetryInterval: activityDelayWindow},
		})
	})
}

// TestTimeout_Declarative drives the activity-timeout scenarios, including the start-delay and paused
// variants that have no WFA counterpart. Model-checked by driveTrace.
func (s *activityParityTestSuite) TestTimeout_Declarative() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaTraceBudget()))
	env := newActivityParityEnv(s.T())
	t := s.T()

	t.Run("schedule-to-close/elapses-while-paused", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Pause, {Type: model.ScheduleToCloseElapsesType}},
		})
	})
	t.Run("schedule-to-start/elapses-while-scheduled", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{{Type: model.ScheduleToStartElapsesType}},
		})
	})
	t.Run("schedule-to-start/elapses-while-paused", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Pause, {Type: model.ScheduleToStartElapsesType}},
		})
	})
	t.Run("start-to-close/elapses-while-started/retries-remain", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.StartToCloseElapses},
		})
	})
	t.Run("start-to-close/elapses-while-started/last-attempt", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.StartToCloseElapses},
			cfg:   activityConfig{MaxAttempts: 1},
		})
	})
	t.Run("start-to-close/elapses-while-cancel-requested", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, model.RequestCancel, model.StartToCloseElapses},
		})
	})
	t.Run("heartbeat/elapses-while-started/retries-remain", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, {Type: model.HeartbeatElapsesType}},
		})
	})
	t.Run("heartbeat/elapses-while-started/last-attempt", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace: []model.Event{model.Poll, {Type: model.HeartbeatElapsesType}},
			cfg:   activityConfig{MaxAttempts: 1},
		})
	})
	t.Run("schedule-to-start/elapses-within-start-delay", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{{Type: model.ScheduleToStartElapsesType}},
			startDelayed: true,
		})
	})
	t.Run("schedule-to-close/elapses-within-start-delay", func(t *testing.T) {
		s.driveTrace(t, env, saaTrace{
			trace:        []model.Event{{Type: model.ScheduleToCloseElapsesType}},
			startDelayed: true,
		})
	})
}
