package model

import "testing"

// Smoke tests over a few worked examples

func TestInitial(t *testing.T) {
	got := Initial(Config{HasScheduleToClose: true})
	expected := AbstractState{Status: Scheduled, AttemptCount: 1}
	if got != expected {
		t.Fatalf("Initial: got %+v want %+v", got, expected)
	}
}

func TestPollFromScheduledStarts(t *testing.T) {
	s := Initial(Config{})
	out := Transition(Config{}, s, Event{Type: PollType})
	if out.Reject != NoError {
		t.Fatalf("unexpected reject %v", out.Reject)
	}
	if out.Next.Status != Started {
		t.Fatalf("Poll: got %+v", out.Next)
	}
}

func TestPauseWhileScheduledIsPaused(t *testing.T) {
	cfg := Config{HasScheduleToClose: true}
	out := Transition(cfg, Initial(cfg), Event{Type: PauseType})
	if out.Reject != NoError {
		t.Fatalf("unexpected reject %v", out.Reject)
	}
	if out.Next.Status != Paused {
		t.Fatalf("want Paused got %v", out.Next.Status)
	}
}

func TestPauseWhileStartedIsPauseRequested(t *testing.T) {
	cfg := Config{}
	s := Transition(cfg, Initial(cfg), Event{Type: PollType}).Next // Scheduled -> Started
	out := Transition(cfg, s, Event{Type: PauseType})
	if out.Reject != NoError {
		t.Fatalf("unexpected reject %v", out.Reject)
	}
	if out.Next.Status != PauseRequested {
		t.Fatalf("want PauseRequested got %v", out.Next.Status)
	}
}

// The tests below cover a pending dispatch delay — a start_delay before the first attempt, or a
// retry backoff between attempts — and what the model says about timeouts and operator commands
// that arrive while one is still pending.

// backedOffRetry returns a Scheduled state with a pending retry backoff (attempt 2), reached the way
// a worker would: poll the first attempt, then fail it retryably.
func backedOffRetry(t *testing.T, cfg Config) AbstractState {
	t.Helper()
	started := Transition(cfg, Initial(cfg), Event{Type: PollType}).Next
	s := Transition(cfg, started, Event{Type: RespondFailedType, Failure: &Failure{Retryable: true}}).Next
	if s.Status != Scheduled || s.Dispatchability != BackoffPending {
		t.Fatalf("expected a Scheduled/BackoffPending retry, got %v/%v", s.Status, s.Dispatchability)
	}
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
	if paused.Reject != NoError || paused.Next.Status != Paused {
		t.Fatalf("pause during start delay must succeed -> Paused, got %v/%v", paused.Reject, paused.Next.Status)
	}
	unpaused := Transition(cfg, paused.Next, Event{Type: UnpauseType}).Next
	if unpaused.Status != Scheduled || unpaused.Dispatchability != StartDelayPending {
		t.Fatalf("unpause during start delay must resume waiting (Scheduled/StartDelayPending), got %v/%v", unpaused.Status, unpaused.Dispatchability)
	}
	if pollable(cfg, unpaused) {
		t.Fatal("a poll must find no task while the start delay is still pending")
	}
	elapsed := Transition(cfg, unpaused, Event{Type: StartDelayElapsesType}).Next
	if !pollable(cfg, elapsed) {
		t.Fatal("once the start delay elapses the activity must dispatch")
	}
}

// Same as TestPauseUnpauseDuringStartDelay but for a retry backoff.
func TestPauseUnpauseDuringBackoff(t *testing.T) {
	cfg := Config{}
	retry := backedOffRetry(t, cfg)
	paused := Transition(cfg, retry, Event{Type: PauseType})
	if paused.Reject != NoError || paused.Next.Status != Paused {
		t.Fatalf("pause during backoff must succeed -> Paused, got %v/%v", paused.Reject, paused.Next.Status)
	}
	unpaused := Transition(cfg, paused.Next, Event{Type: UnpauseType}).Next
	if unpaused.Dispatchability != BackoffPending {
		t.Fatalf("unpause during backoff must resume waiting (BackoffPending), got %v", unpaused.Dispatchability)
	}
	if pollable(cfg, unpaused) {
		t.Fatal("a poll must find no task while the backoff is still pending")
	}
	if !pollable(cfg, Transition(cfg, unpaused, Event{Type: BackoffElapsesType}).Next) {
		t.Fatal("once the backoff elapses the activity must dispatch")
	}
}

// Schedule-to-close is pushed back by a start delay (and a retry backoff)
func TestScheduleToClosePushedBackByStartDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true, HasScheduleToClose: true}
	s := Initial(cfg)
	if s.Dispatchability != StartDelayPending {
		t.Fatalf("Initial with start delay should be StartDelayPending, got %v", s.Dispatchability)
	}
	if out := Transition(cfg, s, Event{Type: ScheduleToCloseElapsesType}); out.Next.Status != Scheduled {
		t.Fatalf("schedule-to-close must not fire during the start delay, got %v", out.Next.Status)
	}
}

// Schedule-to-start is pushed back by a start delay (and a retry backoff)
func TestScheduleToStartPushedBackByDispatchDelay(t *testing.T) {
	startDelayCfg := Config{HasStartDelay: true, HasScheduleToStart: true}
	s := Initial(startDelayCfg)
	if out := Transition(startDelayCfg, s, Event{Type: ScheduleToStartElapsesType}); out.Next.Status != Scheduled {
		t.Fatalf("schedule-to-start must not fire during the start delay (pushed back), got %v", out.Next.Status)
	}
	dispatched := Transition(startDelayCfg, s, Event{Type: StartDelayElapsesType}).Next
	if out := Transition(startDelayCfg, dispatched, Event{Type: ScheduleToStartElapsesType}); out.Next.Status != TimedOut {
		t.Fatalf("schedule-to-start should fire once the delay elapses, got %v", out.Next.Status)
	}

	backoffCfg := Config{HasScheduleToStart: true}
	retry := backedOffRetry(t, backoffCfg)
	if out := Transition(backoffCfg, retry, Event{Type: ScheduleToStartElapsesType}); out.Next.Status != Scheduled {
		t.Fatalf("schedule-to-start must not fire during the retry backoff (pushed back), got %v", out.Next.Status)
	}
}

// Reset during a start delay is possible and behaves like unpause: it keeps waiting for the delay
// rather than dispatching now.
func TestResetDuringStartDelayPreservesDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true}
	out := Transition(cfg, Initial(cfg), Event{Type: ResetType})
	if out.Reject != NoError {
		t.Fatalf("reset during start delay must be accepted, got reject %v", out.Reject)
	}
	if out.Next.Dispatchability != StartDelayPending {
		t.Fatalf("reset during start delay must keep waiting (StartDelayPending), got %v", out.Next.Dispatchability)
	}
	if pollable(cfg, out.Next) {
		t.Fatal("a poll must find no task after a reset during the start delay")
	}
}

// Reset during a retry backoff discards the backoff: the reset attempt dispatches immediately.
func TestResetDuringBackoffDispatchesImmediately(t *testing.T) {
	cfg := Config{}
	out := Transition(cfg, backedOffRetry(t, cfg), Event{Type: ResetType})
	if out.Reject != NoError {
		t.Fatalf("reset during backoff must be accepted, got reject %v", out.Reject)
	}
	if out.Next.Dispatchability != Dispatchable {
		t.Fatalf("reset during backoff must discard the backoff (Dispatchable), got %v", out.Next.Dispatchability)
	}
	if !pollable(cfg, out.Next) {
		t.Fatal("a poll after reset-during-backoff must dispatch immediately")
	}
}

func TestPossible(t *testing.T) {
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
			if got := Possible(c.cfg, c.state, c.event); got != c.want {
				t.Fatalf("Possible(%s in %v/%v) = %v, want %v", c.event, c.state.Status, c.state.Dispatchability, got, c.want)
			}
		})
	}
}
