// Activity archetype behavior model: transition rules.
//
// The model is defined by a collection of transition functions, each representing one of two sorts
// of events:
//
// 1. An RPC
// 2. A time window elapsing (configured activity timeouts or start delay / retry backoff)
//
// The transition functions accept the current activity state and information about the event, and
// return an Outcome specifying the event's consequences:
//
// func someEvent(Config, AbstractState, Event) Outcome
//
// Note to authors: aim to update this file independently of the implementation, as opposed to
// "filling it out" to match the implementation.
// Note to AIs: ask your human operator to update this file when you are making implementation
// changes that cause model conformance tests to fail.

package model

import (
	"fmt"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
)

// Outcome is the return type of a transition function. It specifies the consequences of an event.
//
// AttemptTasksInvalidated and ScheduleToCloseTaskInvalidated are not part of AbstractState: a task
// invalidation is observable only as a change in the underlying stamp, not as a distinct state
// value. The model states, per transition, whether each is invalidated, and the driver detects it as
// a stamp delta across the edge.
type Outcome struct {
	Next                           AbstractState
	Reject                         ErrorKind
	ImpossibleTimeWindowElapse     bool
	AttemptTasksInvalidated        bool // this transition invalidates the pending attempt dispatch/timer tasks
	ScheduleToCloseTaskInvalidated bool // this transition restarts/invalidates the schedule-to-close timer
}

// Initial is the state of a newly created Activity.
func Initial(cfg Config) AbstractState {
	s := AbstractState{Status: Scheduled, AttemptCount: 1, DispatchTimeSet: true}
	if cfg.HasStartDelay {
		s.Dispatchability = StartDelayPending
	}
	return s
}

// Transition is the model's total transition function: given the config, the current state, and an
// event, it returns an Outcome.
func Transition(cfg Config, s AbstractState, e Event) Outcome {
	if s.Status == Unspecified {
		panic("unreachable")
	}
	if s.Status.Terminal() {
		return handleEventInTerminalState(s, e)
	}
	switch e.Type {
	case PollType:
		return poll(cfg, s, e)
	case HeartbeatType:
		return heartbeat(cfg, s, e)
	case RespondCompletedType, RespondCompletedByIDType:
		return respondCompleted(cfg, s, e)
	case RespondFailedType, RespondFailedByIDType:
		return respondFailed(cfg, s, e)
	case RespondCanceledType:
		return respondCanceled(cfg, s, e)
	case RespondCanceledByIDType:
		return respondCanceledByID(cfg, s, e)
	case RequestCancelType:
		return requestCancel(cfg, s, e)
	case TerminateType:
		return terminate(cfg, s, e)
	case PauseType:
		return pause(cfg, s, e)
	case UnpauseType:
		return unpause(cfg, s, e)
	case ResetType:
		return reset(cfg, s, e)
	case UpdateOptionsType:
		return updateOptions(cfg, s, e)
	case ScheduleToStartElapsesType:
		return scheduleToStartElapses(cfg, s, e)
	case ScheduleToCloseElapsesType:
		return scheduleToCloseElapses(cfg, s, e)
	case StartToCloseElapsesType:
		return startToCloseElapses(cfg, s, e)
	case HeartbeatElapsesType:
		return heartbeatElapses(cfg, s, e)
	case StartDelayElapsesType:
		return startDelayElapses(cfg, s, e)
	case BackoffElapsesType:
		return backoffElapses(cfg, s, e)
	default:
		panic("model: unhandled event type")
	}
}

// PollActivityTaskQueue: advances a dispatchable Scheduled attempt to Started.
func poll(_ Config, s AbstractState, _ Event) Outcome {
	if !FindsTask(s) {
		return noop(s)
	}
	n := s
	n.Status = Started
	n.FirstAttemptStarted = true
	return Outcome{Next: n}
}

// RespondActivityTaskCompleted (by task token, from a worker) or
// RespondActivityTaskCompletedById (from a service completing the activity out of band). Completes
// an in-progress attempt. Only the by-id form can complete an activity whose attempt has not
// started.
func respondCompleted(_ Config, s AbstractState, e Event) Outcome {
	completed := func() Outcome {
		n := s
		n.Status = Completed
		return Outcome{Next: n}
	}
	switch s.Status {
	case Started, PauseRequested, CancelRequested, ResetRequested:
		return completed()
	case Scheduled, Paused:
		if e.Type == RespondCompletedByIDType {
			return completed()
		}
		return reject(s, NotFound)
	default:
		panic("model does not handle RespondCompleted while in status " + s.Status.String())
	}
}

// RespondActivityTaskFailed (by task token, from worker) or RespondActivityTaskFailedById.
// (from a service completing the activity out of band). Fails an in-progress attempt.
func respondFailed(cfg Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case ResetRequested:
		return applyDeferredReset(cfg, s)
	case Started, PauseRequested:
		n := s
		if isRetryableFailure(cfg, e) && s.retriesRemaining(cfg) {
			n.Status = Scheduled
			n.Dispatchability = BackoffPending // the retry waits for the backoff interval
			if s.Status == PauseRequested {
				n.Status = Paused // pause takes effect on the retry
			}
			n.AttemptCount++
			return Outcome{Next: n, AttemptTasksInvalidated: true} // new attempt invalidates last attempt's tasks
		}
		// no retry: terminal failure
		n.Status = Failed
		return Outcome{Next: n}
	case CancelRequested:
		n := s
		n.Status = Failed
		return Outcome{Next: n}
	case Scheduled, Paused:
		// No attempt is in progress to fail.
		return reject(s, NotFound)
	default:
		panic("model does not handle RespondFailed while in status " + s.Status.String())
	}
}

// Worker RespondActivityTaskCanceledById acknowledges a cancellation without a task token, so
// staleness cannot reject it; only the absence of a cancellation request can.
func respondCanceledByID(_ Config, s AbstractState, _ Event) Outcome {
	switch s.Status {
	case CancelRequested:
		n := s
		n.Status = Canceled
		return Outcome{Next: n}
	case Scheduled, Paused, Started, PauseRequested, ResetRequested:
		return reject(s, InvalidArgument) // nobody asked for a cancellation
	default:
		panic("model does not handle RespondCanceledByID while in status " + s.Status.String())
	}
}

// Note: The model implies an "order of precedence": Cancel > Reset > Pause. I.e. you can Cancel in
// {Reset,Pause}Requested, and you can Reset in PauseRequested, but neither a Pause nor a Reset will
// undo a Cancel request.

// Worker RespondActivityTaskCanceled with task token: cancels an in-progress attempt for which
// cancellation has been requested.
func respondCanceled(_ Config, s AbstractState, _ Event) Outcome {
	switch s.Status {
	case CancelRequested:
		n := s
		n.Status = Canceled
		return Outcome{Next: n}
	case Scheduled, Paused:
		return reject(s, NotFound) // task token invalid
	case Started, PauseRequested, ResetRequested:
		return reject(s, InvalidArgument) // token valid, but cancel has not been requested
	default:
		panic("model does not handle RespondCanceled while in status " + s.Status.String())
	}
}

// Worker RecordActivityTaskHeartbeat
func heartbeat(_ Config, s AbstractState, _ Event) Outcome {
	switch s.Status {
	case Started, PauseRequested, CancelRequested, ResetRequested:
		return noop(s)
	case Scheduled, Paused:
		return reject(s, NotFound)
	default:
		panic("model does not handle Heartbeat while in status " + s.Status.String())
	}
}

// Cancellation requested: For SAA this is RequestCancelActivityExecution; for WFA this is a
// workflow cancelling the activity.
func requestCancel(_ Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case Scheduled, Paused:
		n := s
		n.Status = Canceled
		return Outcome{Next: n}
	case Started, PauseRequested, ResetRequested:
		n := s
		n.Status = CancelRequested
		return Outcome{Next: n}
	case CancelRequested:
		if e.SameRequestID {
			return noop(s) // requestID-based idempotency
		}
		return reject(s, FailedPrecondition) // TODO(dan): should we consider making this idempotent success when the requestID differs?
	default:
		panic("model does not handle RequestCancel while in status " + s.Status.String())
	}
}

// TerminateActivityExecution
func terminate(_ Config, s AbstractState, _ Event) Outcome {
	switch s.Status {
	case Scheduled, Paused, Started, PauseRequested, CancelRequested, ResetRequested:
		n := s
		n.Status = Terminated
		return Outcome{Next: n}
	default:
		panic("model does not handle Terminate while in status " + s.Status.String())
	}
}

// PauseActivityExecution
func pause(_ Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case Scheduled:
		n := s
		n.Status = Paused
		return Outcome{Next: n, AttemptTasksInvalidated: true} // invalidate any pending dispatch task
	case Started:
		n := s
		n.Status = PauseRequested // the running attempt is left alone; the pause takes effect on the next one
		return Outcome{Next: n}
	case Paused, PauseRequested:
		if e.SameRequestID {
			return noop(s) // requestID-based idempotency
		}
		return reject(s, FailedPrecondition) // "already paused"
	case CancelRequested:
		return reject(s, FailedPrecondition) // a pause cannot undo a cancel
	case ResetRequested:
		if s.ResetKeepPaused && e.SameRequestID {
			// Pause -> Reset(keepPaused) -> Pause(same requestID)
			return noop(s)
		}
		return reject(s, FailedPrecondition)
	default:
		panic("model does not handle Pause while in status " + s.Status.String())
	}
}

// UnpauseActivityExecution
func unpause(_ Config, s AbstractState, _ Event) Outcome {
	switch s.Status {
	case Paused:
		n := s
		n.Status = Scheduled
		n.DispatchTimeSet = true
		// TODO(dan) double-check this is as it should be: we invalidate attempt tasks on Unpause, not on entry to Paused?
		return Outcome{Next: n, AttemptTasksInvalidated: true}
	case PauseRequested:
		// Unlike CancelRequested and ResetRequested, PauseRequested can be "undone" (by Unpause).
		n := s
		n.Status = Started
		return Outcome{Next: n}
	case Scheduled, Started, CancelRequested, ResetRequested:
		return reject(s, FailedPrecondition)
	default:
		panic("model does not handle Unpause while in status " + s.Status.String())
	}
}

// ResetActivityExecution: makes the activity behave as if starting its first attempt, except the
// schedule-to-close timer keeps running. Applied only once any current attempt has ended.
func reset(cfg Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case Scheduled, Paused:
		n := s
		n.AttemptCount = 1
		// Reset discards a pending retry backoff (the reset attempt dispatches immediately) but keeps
		// a pending start_delay.
		if s.Dispatchability == BackoffPending {
			n.Dispatchability = Dispatchable
		}
		if s.Status == Paused && e.KeepPaused {
			n.DispatchTimeSet = false
		} else {
			n.Status = Scheduled
			n.DispatchTimeSet = true
		}
		return Outcome{Next: n, AttemptTasksInvalidated: true, ScheduleToCloseTaskInvalidated: e.RestoreOriginal && cfg.HasScheduleToClose}
	case Started, PauseRequested:
		n := s
		n.Status = ResetRequested
		n.ResetKeepPaused = s.Status == PauseRequested && e.KeepPaused // Reset during PauseRequested honors KeepPaused
		n.ResetHeartbeats = e.ResetHeartbeat
		n.ResetRestoreOptions = e.RestoreOriginal
		// The current attempt stays live; the reset is applied once it ends.
		return Outcome{Next: n}
	case CancelRequested, ResetRequested:
		// TODO(dan): should we support repeat reset requests?
		return reject(s, FailedPrecondition)
	default:
		panic("model does not handle Reset while in status " + s.Status.String())
	}
}

// UpdateActivityExecutionOptions
func updateOptions(cfg Config, s AbstractState, e Event) Outcome {
	// TODO(dan): RestoreOriginal, field-mask merge. Does it re-dispatch when SCHEDULED?

	// start_delay is mutable while the first dispatch hasn't happened yet: either still within the
	// start-delay window, or paused before the first attempt started.
	startDelayMutable := s.Dispatchability == StartDelayPending ||
		(s.Status == Paused && !s.FirstAttemptStarted)
	if e.SetsStartDelay && !startDelayMutable {
		return reject(s, FailedPrecondition)
	}

	switch s.Status {
	case Scheduled, Paused, Started, PauseRequested:
		n := s
		if e.SetsStartDelay {
			// TODO: shouldn't this only be possible in Scheduled?
			n.Dispatchability = StartDelayPending
		}
		return Outcome{Next: n, AttemptTasksInvalidated: true, ScheduleToCloseTaskInvalidated: cfg.HasScheduleToClose}
	case CancelRequested, ResetRequested:
		return reject(s, FailedPrecondition)
	default:
		panic("model does not handle UpdateOptions while in status " + s.Status.String())
	}
}

// The events below represent timeouts (scheduleToStartElapses, scheduleToCloseElapses,
// startToCloseElapses, heartbeatElapses) or dispatch delays (startDelayElapses, backoffElapses).
// More precisely, they represent the end of a time window configured by the driver: whether or not
// a timer task actually fires around that time depends on whether this nominal timeout or delay
// should correspond to a real one in the product behavior, and on the correctness of the
// implementation. These events never reject; they either cause a transition, or no-op.
//
// An example of a nominal timeout that does not correspond to a real timeout in the product behavior
// is a schedule-to-close time configured shorter than the start delay. Interpreted naively (counting
// from schedule time), its window would end during the start delay. But in fact, under the model,
// the schedule-to-close timer starts to count down at the end of the start delay.

// scheduleToStartElapses represents the end of the nominal schedule-to-start timeout configured by
// the driver. The product behavior is that the timeout starts counting from dispatch time, so a
// start_delay or retry backoff effectively pushes it back.
func scheduleToStartElapses(cfg Config, s AbstractState, _ Event) Outcome {
	if !cfg.HasScheduleToStart || s.Status != Scheduled || s.Dispatchability != Dispatchable {
		return impossibleElapse(s)
	}
	n := s
	n.Status = TimedOut
	return Outcome{Next: n}
}

// scheduleToCloseElapses represents the end of the nominal schedule-to-close timeout configured by
// the driver. The product behavior is that the deadline is anchored at first-dispatch time
// (schedule_time + start_delay), so it does not run during a start_delay, and is not suspended
// while paused.
func scheduleToCloseElapses(cfg Config, s AbstractState, _ Event) Outcome {
	if !cfg.HasScheduleToClose || s.Dispatchability == StartDelayPending {
		return impossibleElapse(s)
	}
	n := s
	n.Status = TimedOut
	return Outcome{Next: n}
}

// startToCloseElapses and heartbeatElapses represent the end of the nominal per-attempt timeouts
// configured by the driver. The product behavior is that either one ends the running attempt.
func startToCloseElapses(cfg Config, s AbstractState, e Event) Outcome {
	return attemptTimedOut(cfg, s, e)
}

func heartbeatElapses(cfg Config, s AbstractState, e Event) Outcome {
	if !cfg.HasHeartbeat {
		return impossibleElapse(s)
	}
	return attemptTimedOut(cfg, s, e)
}

// startDelayElapses represents the end of the nominal start_delay window configured by the driver.
// The product behavior is that the delayed first dispatch becomes available; the status is unchanged
// (a Paused activity stays Paused but now dispatches on unpause).
func startDelayElapses(cfg Config, s AbstractState, _ Event) Outcome {
	if !cfg.HasStartDelay || s.Dispatchability != StartDelayPending {
		return impossibleElapse(s)
	}
	n := s
	n.Dispatchability = Dispatchable
	return Outcome{Next: n}
}

// backoffElapses represents the end of the nominal retry-backoff window configured by the driver.
// The product behavior is that the delayed retry dispatch becomes available; symmetric to
// startDelayElapses.
func backoffElapses(_ Config, s AbstractState, _ Event) Outcome {
	if s.Dispatchability != BackoffPending {
		return impossibleElapse(s)
	}
	n := s
	n.Dispatchability = Dispatchable
	return Outcome{Next: n}
}

// helpers

// Possible returns whether event type t can occur in state s. This is not related to whether an
// error would be returned by the server. An event is always Possible, unless it is an elapse of a
// time window that was not in effect.
func Possible(cfg Config, s AbstractState, t EventType) bool {
	return !Transition(cfg, s, Event{Type: t}).ImpossibleTimeWindowElapse
}

// FindsTask reports whether a poll would find a task to start.
func FindsTask(s AbstractState) bool {
	return s.Status == Scheduled && s.Dispatchability == Dispatchable
}

func noop(s AbstractState) Outcome { return Outcome{Next: s, Reject: NoError} }

func impossibleElapse(s AbstractState) Outcome {
	o := noop(s)
	o.ImpossibleTimeWindowElapse = true
	return o
}

func reject(s AbstractState, k ErrorKind) Outcome { return Outcome{Next: s, Reject: k} }

// handleEventInTerminalState handles any event once the activity has reached a terminal status.
func handleEventInTerminalState(s AbstractState, e Event) Outcome {
	switch e.Type {
	case PollType:
		return noop(s) // A worker can always poll
	case HeartbeatType, RespondCompletedType, RespondFailedType, RespondCanceledType,
		RespondCompletedByIDType, RespondFailedByIDType, RespondCanceledByIDType:
		return reject(s, NotFound) // Activity-specific RPCs from a worker
	case TerminateType:
		if s.Status == Terminated && e.SameRequestID {
			return noop(s) // idempotent only from Terminated
		}
		return reject(s, FailedPrecondition)
	case RequestCancelType, PauseType, UnpauseType, ResetType, UpdateOptionsType:
		return reject(s, FailedPrecondition) // Operator commands from client
	case ScheduleToStartElapsesType, ScheduleToCloseElapsesType, StartToCloseElapsesType,
		HeartbeatElapsesType, StartDelayElapsesType, BackoffElapsesType:
		return impossibleElapse(s) // the activity has closed and no time windows are in effect
	default:
		panic("model: unhandled event type in eventInTerminalState")
	}
}

func attemptTimedOut(cfg Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case ResetRequested:
		return applyDeferredReset(cfg, s)
	case Started, PauseRequested:
		n := s
		if isRetryableTimeout(cfg, e.Type) && s.retriesRemaining(cfg) {
			// retry
			n.Status = Scheduled
			if s.Status == PauseRequested {
				n.Status = Paused
			}
			n.Dispatchability = BackoffPending
			n.AttemptCount++
			return Outcome{Next: n, AttemptTasksInvalidated: true} // new attempt invalidates last attempt's tasks
		}
		n.Status = TimedOut
		return Outcome{Next: n}
	case CancelRequested:
		// Timeout -> TimedOut, not Canceled.
		n := s
		n.Status = TimedOut
		return Outcome{Next: n}
	case Scheduled, Paused:
		return impossibleElapse(s) // there was no running attempt
	default:
		panic("model does not handle a per-attempt timeout while in status " + s.Status.String())
	}
}

// retriesRemaining is whether the retry policy allows an attempt after the current one.
func (s AbstractState) retriesRemaining(cfg Config) bool {
	return cfg.MaxAttempts == 0 || s.AttemptCount < cfg.MaxAttempts
}

// isRetryableFailure is whether a reported failure should result in a retry.
func isRetryableFailure(cfg Config, e Event) bool {
	if cfg.RetryOutlivesScheduleToClose {
		return false
	}
	f := e.Failure
	if f == nil {
		return true
	}
	switch f.Type {
	case ApplicationFailureType, ServerFailureType:
		return !f.NonRetryable
	case StartToCloseTimeoutFailureType:
		return isRetryableTimeout(cfg, StartToCloseElapsesType)
	case HeartbeatTimeoutFailureType:
		return isRetryableTimeout(cfg, HeartbeatElapsesType)
	case UnknownFailureType:
		return true
	case ScheduleToStartTimeoutFailureType, ScheduleToCloseTimeoutFailureType:
		return false
	default:
		panic(fmt.Sprintf("model does not classify failure type %d as retryable or not", f.Type))
	}
}

// isRetryableTimeout is whether the timeout a timeout-elapse event type names should result in a
// retry.
func isRetryableTimeout(cfg Config, t EventType) bool {
	if cfg.RetryOutlivesScheduleToClose {
		return false
	}
	return !slices.Contains(cfg.NonRetryableTimeouts, t)
}

// applyDeferredReset is triggered by failure or timeout. It consumes the pause intent stored while
// the activity was RESET_REQUESTED and applies the reset.
func applyDeferredReset(cfg Config, s AbstractState) Outcome {
	n := s
	n.AttemptCount = 1
	n.Dispatchability = Dispatchable // reset discards remaining backoff
	n.ResetKeepPaused, n.ResetHeartbeats, n.ResetRestoreOptions = false, false, false
	if s.ResetKeepPaused {
		n.Status = Paused
		n.DispatchTimeSet = false // no dispatch task while paused
	} else {
		n.Status = Scheduled
		n.DispatchTimeSet = true
	}
	return Outcome{Next: n, AttemptTasksInvalidated: true, ScheduleToCloseTaskInvalidated: s.ResetRestoreOptions && cfg.HasScheduleToClose}
}

// --- returned values ---
//
// The values the server returns to a caller (not persisted state).

// HeartbeatFlags are the flags a heartbeat response carries back to the worker.
type HeartbeatFlags struct {
	CancelRequested bool
	ActivityPaused  bool
	ActivityReset   bool
}

func ExpectedHeartbeatFlags(s AbstractState) HeartbeatFlags {
	switch s.Status {
	case Started:
		return HeartbeatFlags{}
	case CancelRequested:
		return HeartbeatFlags{CancelRequested: true}
	case ResetRequested:
		return HeartbeatFlags{ActivityReset: true}
	case PauseRequested:
		return HeartbeatFlags{ActivityPaused: true}
	default:
		panic("ExpectedHeartbeatFlags: not a token-valid status: " + s.Status.String())
	}
}

// ExpectedDescribe is the pair of statuses a describe reports for a state.
func ExpectedDescribe(s AbstractState) (enumspb.ActivityExecutionStatus, enumspb.PendingActivityState) {
	switch s.Status {
	case Scheduled:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
	case Started:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_STARTED
	case Completed:
		return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case Failed:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case CancelRequested:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	case Canceled:
		return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case Terminated:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case TimedOut:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case PauseRequested:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
	case Paused:
		return enumspb.ACTIVITY_EXECUTION_STATUS_PAUSED, enumspb.PENDING_ACTIVITY_STATE_PAUSED
	case ResetRequested:
		if s.ResetKeepPaused {
			return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
		}
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_STARTED
	default:
		panic("Unexpected status: " + s.Status.String())
	}
}
