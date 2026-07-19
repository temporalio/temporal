package model

import "testing"

// Smoke tests over a few worked examples; the graph traversal checks the whole graph.

func TestInitial(t *testing.T) {
	got := Initial(Config{HasScheduleToClose: true})
	want := AbstractState{Status: Scheduled, Count: 1, DispatchTimeSet: true}
	if got != want {
		t.Fatalf("Initial: got %+v want %+v", got, want)
	}
}

func TestPollFromScheduledStarts(t *testing.T) {
	s := Initial(Config{})
	out := Transition(Config{}, s, Event{Kind: Poll})
	if out.Reject != NoError {
		t.Fatalf("unexpected reject %v", out.Reject)
	}
	if out.Next.Status != Started || !out.Next.FirstAttemptStarted {
		t.Fatalf("Poll: got %+v", out.Next)
	}
	if out.AttemptTasksInvalidated {
		t.Fatalf("Poll must not invalidate attempt tasks")
	}
}

func TestPauseFromScheduledInvalidatesAttemptTasks(t *testing.T) {
	cfg := Config{HasScheduleToClose: true}
	s := Initial(cfg)
	out := Transition(cfg, s, Event{Kind: Pause})
	if out.Reject != NoError {
		t.Fatalf("unexpected reject %v", out.Reject)
	}
	if out.Next.Status != Paused {
		t.Fatalf("want Paused got %v", out.Next.Status)
	}
	if !out.AttemptTasksInvalidated {
		t.Fatalf("pause from scheduled must invalidate attempt tasks")
	}
	if out.ScheduleToCloseTaskInvalidated {
		t.Fatalf("pause must not invalidate the schedule-to-close task")
	}
}

func TestPauseWhileStartedIsPauseRequested(t *testing.T) {
	cfg := Config{}
	s := Transition(cfg, Initial(cfg), Event{Kind: Poll}).Next // Scheduled -> Started
	out := Transition(cfg, s, Event{Kind: Pause})
	if out.Reject != NoError {
		t.Fatalf("unexpected reject %v", out.Reject)
	}
	if out.Next.Status != PauseRequested {
		t.Fatalf("want PauseRequested got %v", out.Next.Status)
	}
	if out.AttemptTasksInvalidated {
		t.Fatalf("pause while started must NOT invalidate attempt tasks")
	}
}

// The tests below pin the dispatch-delay requirements (start_delay and retry backoff interacting
// with timeouts and operator commands) at the model level.

// backedOffRetry returns a Scheduled state with a pending retry backoff (attempt 2), reached the way
// a worker would: poll the first attempt, then fail it retryably.
func backedOffRetry(t *testing.T, cfg Config) AbstractState {
	t.Helper()
	started := Transition(cfg, Initial(cfg), Event{Kind: Poll}).Next
	s := Transition(cfg, started, Event{Kind: RespondFailed, Retryable: true}).Next
	if s.Status != Scheduled || s.Dispatchability != BackoffPending {
		t.Fatalf("expected a Scheduled/BackoffPending retry, got %v/%v", s.Status, s.Dispatchability)
	}
	return s
}

func pollable(cfg Config, s AbstractState) bool {
	return Transition(cfg, s, Event{Kind: Poll}).Next.Status == Started
}

// Pause during a start delay is possible; unpause does not dispatch immediately — it keeps waiting
// for the delay, and only a StartDelayElapses makes it dispatchable.
func TestPauseUnpauseDuringStartDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true}
	paused := Transition(cfg, Initial(cfg), Event{Kind: Pause})
	if paused.Reject != NoError || paused.Next.Status != Paused {
		t.Fatalf("pause during start delay must succeed -> Paused, got %v/%v", paused.Reject, paused.Next.Status)
	}
	unpaused := Transition(cfg, paused.Next, Event{Kind: Unpause}).Next
	if unpaused.Status != Scheduled || unpaused.Dispatchability != StartDelayPending {
		t.Fatalf("unpause during start delay must resume waiting (Scheduled/StartDelayPending), got %v/%v", unpaused.Status, unpaused.Dispatchability)
	}
	if pollable(cfg, unpaused) {
		t.Fatalf("a poll must find no task while the start delay is still pending")
	}
	elapsed := Transition(cfg, unpaused, Event{Kind: StartDelayElapses}).Next
	if !pollable(cfg, elapsed) {
		t.Fatalf("once the start delay elapses the activity must dispatch")
	}
}

// Same as TestPauseUnpauseDuringStartDelay but for a retry backoff.
func TestPauseUnpauseDuringBackoff(t *testing.T) {
	cfg := Config{}
	retry := backedOffRetry(t, cfg)
	paused := Transition(cfg, retry, Event{Kind: Pause})
	if paused.Reject != NoError || paused.Next.Status != Paused {
		t.Fatalf("pause during backoff must succeed -> Paused, got %v/%v", paused.Reject, paused.Next.Status)
	}
	unpaused := Transition(cfg, paused.Next, Event{Kind: Unpause}).Next
	if unpaused.Dispatchability != BackoffPending {
		t.Fatalf("unpause during backoff must resume waiting (BackoffPending), got %v", unpaused.Dispatchability)
	}
	if pollable(cfg, unpaused) {
		t.Fatalf("a poll must find no task while the backoff is still pending")
	}
	if !pollable(cfg, Transition(cfg, unpaused, Event{Kind: BackoffElapses}).Next) {
		t.Fatalf("once the backoff elapses the activity must dispatch")
	}
}

// Schedule-to-close is pushed back by a start delay (and a retry backoff)
func TestScheduleToClosePushedBackByStartDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true, HasScheduleToClose: true}
	s := Initial(cfg)
	if s.Dispatchability != StartDelayPending {
		t.Fatalf("Initial with start delay should be StartDelayPending, got %v", s.Dispatchability)
	}
	if out := Transition(cfg, s, Event{Kind: ScheduleToCloseElapses}); out.Next.Status != Scheduled {
		t.Fatalf("schedule-to-close must not fire during the start delay, got %v", out.Next.Status)
	}
}

// Schedule-to-start is pushed back by a start delay (and a retry backoff)
func TestScheduleToStartPushedBackByDispatchDelay(t *testing.T) {
	startDelayCfg := Config{HasStartDelay: true, HasScheduleToStart: true}
	s := Initial(startDelayCfg)
	if out := Transition(startDelayCfg, s, Event{Kind: ScheduleToStartElapses}); out.Next.Status != Scheduled {
		t.Fatalf("schedule-to-start must not fire during the start delay (pushed back), got %v", out.Next.Status)
	}
	dispatched := Transition(startDelayCfg, s, Event{Kind: StartDelayElapses}).Next
	if out := Transition(startDelayCfg, dispatched, Event{Kind: ScheduleToStartElapses}); out.Next.Status != TimedOut {
		t.Fatalf("schedule-to-start should fire once the delay elapses, got %v", out.Next.Status)
	}

	backoffCfg := Config{HasScheduleToStart: true}
	retry := backedOffRetry(t, backoffCfg)
	if out := Transition(backoffCfg, retry, Event{Kind: ScheduleToStartElapses}); out.Next.Status != Scheduled {
		t.Fatalf("schedule-to-start must not fire during the retry backoff (pushed back), got %v", out.Next.Status)
	}
}

// Reset during a start delay is possible and behaves like unpause: it keeps waiting for the delay
// rather than dispatching now.
func TestResetDuringStartDelayPreservesDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true}
	out := Transition(cfg, Initial(cfg), Event{Kind: Reset})
	if out.Reject != NoError {
		t.Fatalf("reset during start delay must be accepted, got reject %v", out.Reject)
	}
	if out.Next.Dispatchability != StartDelayPending {
		t.Fatalf("reset during start delay must keep waiting (StartDelayPending), got %v", out.Next.Dispatchability)
	}
	if pollable(cfg, out.Next) {
		t.Fatalf("a poll must find no task after a reset during the start delay")
	}
}

// Reset during a retry backoff discards the backoff: the reset attempt dispatches immediately.
func TestResetDuringBackoffDispatchesImmediately(t *testing.T) {
	cfg := Config{}
	out := Transition(cfg, backedOffRetry(t, cfg), Event{Kind: Reset})
	if out.Reject != NoError {
		t.Fatalf("reset during backoff must be accepted, got reject %v", out.Reject)
	}
	if out.Next.Dispatchability != Dispatchable {
		t.Fatalf("reset during backoff must discard the backoff (Dispatchable), got %v", out.Next.Dispatchability)
	}
	if !pollable(cfg, out.Next) {
		t.Fatalf("a poll after reset-during-backoff must dispatch immediately")
	}
}
