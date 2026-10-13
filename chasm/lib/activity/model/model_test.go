package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Smoke tests over a few worked examples

func TestInitial(t *testing.T) {
	require.Equal(t, AbstractState{Status: Scheduled, AttemptCount: 1}, Initial(Config{HasScheduleToClose: true}))
}

func TestPollFromScheduledStarts(t *testing.T) {
	out := Transition(Config{}, Initial(Config{}), Event{Type: PollType})
	require.Equal(t, NoError, out.Reject)
	require.Equal(t, Started, out.Next.Status)
}

func TestPauseWhileScheduledIsPaused(t *testing.T) {
	cfg := Config{HasScheduleToClose: true}
	out := Transition(cfg, Initial(cfg), Event{Type: PauseType})
	require.Equal(t, NoError, out.Reject)
	require.Equal(t, Paused, out.Next.Status)
}

func TestPauseWhileStartedIsPauseRequested(t *testing.T) {
	cfg := Config{}
	started := Transition(cfg, Initial(cfg), Event{Type: PollType}).Next
	out := Transition(cfg, started, Event{Type: PauseType})
	require.Equal(t, NoError, out.Reject)
	require.Equal(t, PauseRequested, out.Next.Status)
}

// The tests below cover a pending dispatch delay — a start_delay before the first attempt, or a
// retry backoff between attempts — and what the model says about timeouts and operator commands
// that arrive while one is still pending.

// backedOffRetry returns a Scheduled state with a pending retry backoff (attempt 2), reached the way
// a worker would: poll the first attempt, then fail it retryably.
func backedOffRetry(t require.TestingT, cfg Config) AbstractState {
	started := Transition(cfg, Initial(cfg), Event{Type: PollType}).Next
	s := Transition(cfg, started, Event{Type: RespondFailedType, Failure: &Failure{}}).Next
	require.Equal(t, Scheduled, s.Status, "a retryable failure must schedule a retry")
	require.Equal(t, BackoffPending, s.Dispatchability, "a retry must wait for its backoff")
	return s
}

func pollable(cfg Config, s AbstractState) bool {
	return Transition(cfg, s, Event{Type: PollType}).Next.Status == Started
}

// Pause during a start delay is possible; unpause does not dispatch immediately — it keeps waiting
// for the delay, and only a StartDelayElapses makes it dispatchable.
func TestPauseUnpauseDuringStartDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true}
	paused := Transition(cfg, Initial(cfg), Event{Type: PauseType})
	require.Equal(t, NoError, paused.Reject, "pause during a start delay must be accepted")
	require.Equal(t, Paused, paused.Next.Status)
	unpaused := Transition(cfg, paused.Next, Event{Type: UnpauseType}).Next
	require.Equal(t, Scheduled, unpaused.Status)
	require.Equal(t, StartDelayPending, unpaused.Dispatchability, "unpause must resume waiting for the start delay")
	require.False(t, pollable(cfg, unpaused), "a poll must find no task while the start delay is still pending")
	require.True(t, pollable(cfg, Transition(cfg, unpaused, Event{Type: StartDelayElapsesType}).Next),
		"once the start delay elapses the activity must dispatch")
}

// Same as TestPauseUnpauseDuringStartDelay but for a retry backoff.
func TestPauseUnpauseDuringBackoff(t *testing.T) {
	cfg := Config{}
	paused := Transition(cfg, backedOffRetry(t, cfg), Event{Type: PauseType})
	require.Equal(t, NoError, paused.Reject, "pause during a backoff must be accepted")
	require.Equal(t, Paused, paused.Next.Status)
	unpaused := Transition(cfg, paused.Next, Event{Type: UnpauseType}).Next
	require.Equal(t, BackoffPending, unpaused.Dispatchability, "unpause must resume waiting for the backoff")
	require.False(t, pollable(cfg, unpaused), "a poll must find no task while the backoff is still pending")
	require.True(t, pollable(cfg, Transition(cfg, unpaused, Event{Type: BackoffElapsesType}).Next),
		"once the backoff elapses the activity must dispatch")
}

// Schedule-to-close is pushed back by a start delay (and a retry backoff)
func TestScheduleToClosePushedBackByStartDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true, HasScheduleToClose: true}
	s := Initial(cfg)
	require.Equal(t, StartDelayPending, s.Dispatchability, "an activity with a start delay must wait for it")
	require.Equal(t, Scheduled, Transition(cfg, s, Event{Type: ScheduleToCloseElapsesType}).Next.Status,
		"schedule-to-close must not fire during the start delay")
}

// Schedule-to-start is pushed back by a start delay (and a retry backoff)
func TestScheduleToStartPushedBackByDispatchDelay(t *testing.T) {
	startDelayCfg := Config{HasStartDelay: true, HasScheduleToStart: true}
	s := Initial(startDelayCfg)
	require.Equal(t, Scheduled, Transition(startDelayCfg, s, Event{Type: ScheduleToStartElapsesType}).Next.Status,
		"schedule-to-start must not fire during the start delay")
	dispatched := Transition(startDelayCfg, s, Event{Type: StartDelayElapsesType}).Next
	require.Equal(t, TimedOut, Transition(startDelayCfg, dispatched, Event{Type: ScheduleToStartElapsesType}).Next.Status,
		"schedule-to-start must fire once the start delay elapses")

	backoffCfg := Config{HasScheduleToStart: true}
	retry := backedOffRetry(t, backoffCfg)
	require.Equal(t, Scheduled, Transition(backoffCfg, retry, Event{Type: ScheduleToStartElapsesType}).Next.Status,
		"schedule-to-start must not fire during the retry backoff")
}

// Reset during a start delay is possible and behaves like unpause: it keeps waiting for the delay
// rather than dispatching now.
func TestResetDuringStartDelayPreservesDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true}
	out := Transition(cfg, Initial(cfg), Event{Type: ResetType})
	require.Equal(t, NoError, out.Reject, "reset during a start delay must be accepted")
	require.Equal(t, StartDelayPending, out.Next.Dispatchability, "reset must keep waiting for the start delay")
	require.False(t, pollable(cfg, out.Next), "a poll must find no task after a reset during the start delay")
}

// Reset during a retry backoff discards the backoff: the reset attempt dispatches immediately.
func TestResetDuringBackoffDispatchesImmediately(t *testing.T) {
	cfg := Config{}
	out := Transition(cfg, backedOffRetry(t, cfg), Event{Type: ResetType})
	require.Equal(t, NoError, out.Reject, "reset during a backoff must be accepted")
	require.Equal(t, Dispatchable, out.Next.Dispatchability, "reset must discard the backoff")
	require.True(t, pollable(cfg, out.Next), "a poll after a reset during a backoff must dispatch immediately")
}

func TestPossible(t *testing.T) {
	t.Parallel()

	// The configs a case names its state under; the state is reached by driving events from Initial.
	var (
		plain  = Config{MaxAttempts: 3}
		full   = Config{MaxAttempts: 3, HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true}
		delay  = Config{MaxAttempts: 3, HasStartDelay: true}
		polled = func(cfg Config) AbstractState { return Transition(cfg, Initial(cfg), Poll).Next }
	)

	cases := []struct {
		name  string
		cfg   Config
		state AbstractState
		event EventType
		want  bool
	}{
		{"schedule-to-start awaiting first dispatch", full, Initial(full), ScheduleToStartElapsesType, true},
		{"schedule-to-start not configured", plain, Initial(plain), ScheduleToStartElapsesType, false},
		{"schedule-to-start once started", full, polled(full), ScheduleToStartElapsesType, false},
		{"schedule-to-start on a retry", full, backedOffRetry(t, full), ScheduleToStartElapsesType, false},

		{"schedule-to-close while running", full, polled(full), ScheduleToCloseElapsesType, true},
		{"schedule-to-close not configured", plain, polled(plain), ScheduleToCloseElapsesType, false},
		{"schedule-to-close once closed", full, Transition(full, polled(full), Complete).Next, ScheduleToCloseElapsesType, false},

		{"start-to-close while started", plain, polled(plain), StartToCloseElapsesType, true},
		{"start-to-close while cancel requested", plain, Transition(plain, polled(plain), RequestCancel).Next, StartToCloseElapsesType, true},
		{"start-to-close while scheduled", plain, Initial(plain), StartToCloseElapsesType, false},

		{"heartbeat while started", full, polled(full), HeartbeatElapsesType, true},
		{"heartbeat not configured", plain, polled(plain), HeartbeatElapsesType, false},
		{"heartbeat while scheduled", full, Initial(full), HeartbeatElapsesType, false},

		{"start delay within the window", delay, Initial(delay), StartDelayElapsesType, true},
		{"start delay not configured", plain, Initial(plain), StartDelayElapsesType, false},
		{"start delay already elapsed", delay, Transition(delay, Initial(delay), StartDelayElapses).Next, StartDelayElapsesType, false},

		{"backoff between attempts", plain, backedOffRetry(t, plain), BackoffElapsesType, true},
		{"backoff while started", plain, polled(plain), BackoffElapsesType, false},
		{"backoff on the first attempt", plain, Initial(plain), BackoffElapsesType, false},

		{"an RPC is always possible", plain, Transition(plain, polled(plain), Complete).Next, PollType, true},
		{"an RPC it will reject is possible", plain, Initial(plain), RespondCompletedType, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			require.Equalf(t, c.want, Possible(c.cfg, c.state, c.event),
				"Possible(%s in %v/%v)", c.event, c.state.Status, c.state.Dispatchability)
		})
	}
}
