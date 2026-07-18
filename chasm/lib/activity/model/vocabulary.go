// Package model is a test-only behavioral model of the CHASM activity archetype: an abstract,
// implementation-independent description of how an activity should behave, against which a driver
// checks a real activity. It is not runtime code and must never be imported by the server binary.
//
// This file defines the model's vocabulary: the event alphabet (Event/EventKind), the start-time
// Config, and Dispatchability. The observable-state projection and transition rules the model checks
// against are added with the model-transitions work.
//
// The model is archetype-level, not tied to one product surface: the current driver realizes each
// event via frontend RPCs, but a mid-level driver, or a workflow-activity driver, could realize the
// same events differently and be checked against this one shared model.
package model

// Dispatchability says whether a SCHEDULED attempt's next dispatch is available now, or still delayed
// by a start_delay or retry backoff. Not readable via ReadComponent (dispatch_time looks identical
// before and after it elapses), so it is excluded from SameObserved and verified by polling. Its zero
// value Dispatchable is also the value for any non-SCHEDULED status. Distinct from a "deferred"
// pause/reset (an operator command applied when the attempt ends; see ResetKeepPaused).
type Dispatchability int

const (
	Dispatchable      Dispatchability = iota // pollable now: a worker poll returns a task
	StartDelayPending                        // first dispatch delayed until schedule_time + start_delay
	BackoffPending                           // retry dispatch delayed until complete_time + retry interval
)

func (d Dispatchability) String() string {
	switch d {
	case Dispatchable:
		return "Dispatchable"
	case StartDelayPending:
		return "StartDelayPending"
	case BackoffPending:
		return "BackoffPending"
	default:
		return "Dispatchability(?)"
	}
}

// Config captures the start-time options that change transition behavior.
type Config struct {
	HasScheduleToClose bool
	HasScheduleToStart bool
	HasHeartbeat       bool
	HasStartDelay      bool
	MaxAttempts        int32 // 0 = unlimited
}

// EventKind enumerates the events the model covers.
type EventKind int

const (
	Poll EventKind = iota // worker poll that transitions Scheduled -> Started
	Heartbeat
	RespondCompleted
	RespondFailed
	RespondCanceled
	RequestCancel
	Terminate
	Pause
	Unpause
	Reset
	UpdateOptions

	// Timeout deadlines elapsing. The event means the configured deadline window has passed in
	// wall-clock, not that the timer fired; the effect per status is decided by the transition rules.
	// The harness triggers one by configuring the timeout short and waiting.
	ScheduleToStartElapses
	ScheduleToCloseElapses
	StartToCloseElapses
	HeartbeatElapses

	// Dispatch-delay clocks elapsing. When elapsed the delayed dispatch becomes available
	// (Dispatchability -> Dispatchable); status is unchanged, so the only observable is a subsequent
	// Poll returning a task. The harness triggers one by configuring the delay/backoff short and waiting.
	StartDelayElapses
	BackoffElapses
)

// Event carries the variant flags that affect the outcome. Leave irrelevant flags zero.
type Event struct {
	Kind EventKind

	Retryable       bool // RespondFailed: the failure is retryable. Whether it actually retries also depends on cfg.MaxAttempts and s.Count.
	KeepPaused      bool // Reset
	RestoreOriginal bool // Reset / UpdateOptions
	ResetHeartbeat  bool // Unpause
	ResetAttempts   bool // Unpause
	SameRequestID   bool // Pause / Terminate / RequestCancel: repeat of the previous op's request id
	SetsStartDelay  bool // UpdateOptions: the update changes start_delay
}
