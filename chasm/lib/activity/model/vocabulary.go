// Package model is an implementation-independent model of how a CHASM activity should behave.
//
// Drivers can be written that realize these events for Standalone Activity or for Workflow
// Activity.
package model

import (
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type Status int

const (
	Unspecified Status = iota
	Scheduled
	Started
	Completed
	Failed
	CancelRequested
	Canceled
	Terminated
	TimedOut
	PauseRequested
	Paused
	ResetRequested
)

// Dispatchability says whether a SCHEDULED attempt's next dispatch is available now, or still delayed
// by a start_delay or retry backoff.
type Dispatchability int

const (
	Dispatchable Dispatchability = iota // pollable now
	StartDelayPending
	BackoffPending
)

// AbstractState is the projection of observable internal state that the model predicts.
type AbstractState struct {
	Status              Status
	AttemptCount        int32
	FirstAttemptStarted bool
	Dispatchability     Dispatchability
	DispatchTimeSet     bool

	// Flags supporting deferred reset/update
	ResetKeepPaused     bool
	ResetHeartbeats     bool
	ResetRestoreOptions bool
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
	Poll EventKind = iota
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

	// Timeout deadlines elapsing: the configured deadline window has passed in wall-clock (a timer
	// may or may not have actually fired)
	ScheduleToStartElapses
	ScheduleToCloseElapses
	StartToCloseElapses
	HeartbeatElapses

	// Dispatch-delay clocks elapsing. On elapse the delayed dispatch becomes available
	// (Dispatchability -> Dispatchable).
	StartDelayElapses
	BackoffElapses
)

// Event carries the variant flags that affect the outcome.
type Event struct {
	Kind EventKind

	Retryable       bool // RespondFailed: the failure is retryable. Whether it actually retries also depends on cfg.MaxAttempts and s.AttemptCount.
	KeepPaused      bool // Reset
	RestoreOriginal bool // Reset / UpdateOptions
	ResetHeartbeat  bool // Unpause
	ResetAttempts   bool // Unpause
	SameRequestID   bool // Pause / Terminate / RequestCancel: repeat of the previous op's request id
	SetsStartDelay  bool // UpdateOptions
}

// ErrorKind is the API-level outcome the model expects for a rejected or no-op call.
type ErrorKind int

const (
	NoError ErrorKind = iota
	FailedPrecondition
	NotFound
	InvalidArgument
)

// Observed is the internal state the harness reads back via ReadComponent. The reader
// returns Status as the internal proto enum; Abstract maps it onto the model's Status.
type Observed struct {
	Status               activitypb.ActivityExecutionStatus
	Count                int32
	Stamp                int32
	ScheduleToCloseStamp int32
	ResetKeepPaused      bool
	ResetHeartbeats      bool
	ResetRestoreOptions  bool
	FirstAttemptStarted  bool
	DispatchTimeSet      bool
}

func (s Status) String() string {
	switch s {
	case Unspecified:
		return "Unspecified"
	case Scheduled:
		return "Scheduled"
	case Started:
		return "Started"
	case CancelRequested:
		return "CancelRequested"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	case Canceled:
		return "Canceled"
	case Terminated:
		return "Terminated"
	case TimedOut:
		return "TimedOut"
	case Paused:
		return "Paused"
	case PauseRequested:
		return "PauseRequested"
	case ResetRequested:
		return "ResetRequested"
	default:
		return "Status(?)"
	}
}

// Terminal reports whether the activity has reached a terminal state.
func (s Status) Terminal() bool {
	switch s {
	case Completed, Failed, Canceled, Terminated, TimedOut:
		return true
	default:
		return false
	}
}

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

// SameObserved reports whether two states agree on every ReadComponent-readable field that is live in
// the expected status. It excludes the latent Dispatchability, and additionally drops any field that
// is not observable-meaningful in the status (see mask), so the oracle never asserts mechanism state
// where nobody observes it.
func (s AbstractState) SameObserved(o AbstractState) bool {
	return s.mask() == o.mask()
}

// mask zeroes fields that are not observable-meaningful in status s.Status, so the oracle only
// compares each field where it is live. Dispatchability is always latent (verified by polling).
func (s AbstractState) mask() AbstractState {
	s.Dispatchability = Dispatchable
	if s.Status != ResetRequested {
		// The pending-reset intent is only meaningful while a reset is deferred.
		s.ResetKeepPaused, s.ResetHeartbeats, s.ResetRestoreOptions = false, false, false
	}
	return s
}

// Abstract maps an observed internal snapshot onto the model's AbstractState.
func Abstract(o Observed) AbstractState {
	return AbstractState{
		Status:              mapStatus(o.Status),
		AttemptCount:        o.Count,
		ResetKeepPaused:     o.ResetKeepPaused,
		ResetHeartbeats:     o.ResetHeartbeats,
		ResetRestoreOptions: o.ResetRestoreOptions,
		FirstAttemptStarted: o.FirstAttemptStarted,
		DispatchTimeSet:     o.DispatchTimeSet,
	}
}

func mapStatus(s activitypb.ActivityExecutionStatus) Status {
	switch s {
	case activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		return Unspecified
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		return Scheduled
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED:
		return Started
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		return CancelRequested
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		return Completed
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED:
		return Failed
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return Canceled
	case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		return Terminated
	case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return TimedOut
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED:
		return Paused
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		return PauseRequested
	case activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		return ResetRequested
	default:
		return Unspecified
	}
}
