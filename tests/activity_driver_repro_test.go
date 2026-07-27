package tests

// Repros for defects claimed against the drivers themselves, as distinct from the parity tests, which
// assert what the product should do. Each one failed when it was written.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/tests/testcore"
)

type activityDriverErrorRecorder struct {
	failed bool
}

func (r *activityDriverErrorRecorder) Errorf(string, ...any) {
	r.failed = true
}

func (r *activityDriverErrorRecorder) FailNow() {
	r.failed = true
}

func (s *activityParityTestSuite) TestDriversRejectBackoffElapseWhenScheduleToCloseWins() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{
		MaxAttempts:     3,
		RetryInterval:   activityLongDuration,
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
		MaxAttempts:      1,
		StartToClose:     activityShortTimeout,
		HeartbeatTimeout: 2 * activityShortTimeout,
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
		RetryInterval: activityShortDispatchDelay,
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
		task := a.pollForTask(t, activityDriverTimeout)
		require.NotNil(t, task)
		require.Equal(t, "Input", testcore.DecodeString(t, task.GetInput()))
	})
	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		a := newSAADriver(t, env, activityConfig{}).driveTrace(t, nil)
		task := a.pollForTask(t, activityDriverTimeout)
		require.NotNil(t, task)
		require.Equal(t, "Input", testcore.DecodeString(t, task.GetInput()))
	})
}

// A dispatch that is polled before the driver looks again is still a dispatch. The drivers sample the
// activity every activityDriverPollInterval, so a worker taking the task inside that window leaves
// them reading an attempt already running.
func (s *activityParityTestSuite) TestDriversAcceptDispatchPolledDuringTheWait() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityShortDispatchDelay}
	trace := []model.Event{model.Poll, model.FailRetryably}

	// pollInBackground stands in for a worker waiting on the queue when the retry is dispatched.
	pollInBackground := func(ctx context.Context, tq string) func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: &taskqueuepb.TaskQueue{Name: tq},
				Identity:  "repro-worker",
			})
		}()
		time.Sleep(activityDriverPollInterval) //nolint:forbidigo // let the poll reach matching first
		return func() { <-done }
	}

	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		d := newSAADriver(t, env, cfg)
		a := d.driveTrace(t, trace)
		wait := pollInBackground(d.ctx, a.taskQueue)
		rec := &activityDriverErrorRecorder{}
		a.driveEvent(rec, model.BackoffElapses)
		wait()
		require.False(t, rec.failed, "a dispatch taken by a worker during the wait is still a dispatch")
	})
	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		d := newWFADriver(t, env, cfg)
		a := d.driveTrace(t, trace)
		wait := pollInBackground(d.ctx, a.taskQueue)
		rec := &activityDriverErrorRecorder{}
		a.driveEvent(rec, model.BackoffElapses)
		wait()
		require.False(t, rec.failed, "a dispatch taken by a worker during the wait is still a dispatch")
	})
}

// A paused activity reports no pending dispatch, whatever its backoff is doing, so the drivers cannot
// see a dispatch delay elapse while it is paused and must not claim they have.
func (s *activityParityTestSuite) TestDriversRejectDispatchDelayWhilePaused() {
	env := newActivityParityEnv(s.T())
	cfg := activityConfig{MaxAttempts: 3, RetryInterval: activityLongDuration}
	trace := []model.Event{model.Poll, model.FailRetryably, model.Pause}

	s.Run("StandaloneActivity", func(s *activityParityTestSuite) {
		t := s.T()
		a := newSAADriver(t, env, cfg).driveTrace(t, trace)
		rec := &activityDriverErrorRecorder{}
		a.driveEvent(rec, model.BackoffElapses)
		require.True(t, rec.failed, "a paused activity shows no pending dispatch, so none can be seen to elapse")
	})
	s.Run("WorkflowActivity", func(s *activityParityTestSuite) {
		t := s.T()
		a := newWFADriver(t, env, cfg).driveTrace(t, trace)
		rec := &activityDriverErrorRecorder{}
		a.driveEvent(rec, model.BackoffElapses)
		require.True(t, rec.failed, "a paused activity shows no pending dispatch, so none can be seen to elapse")
	})
}
