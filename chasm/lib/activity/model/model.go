// Activity archetype behavior model: the transition rules.
//
// The model is defined by a collection of functions in this file that each represent one of 3 sorts
// of events:
//
// 1. An RPC
// 2. A timeout (schedule-to-close, schedule-to-start, start-to-close, heartbeat)
// 3. A dispatch delay (start delay, retry backoff)
//
// The functions accept the current activity state, and information about the event, and return the
// activity state after the event's consequences. The bottom of the file specifies the other half of
// the contract: the values the server returns to a caller in a given state (projections of
// AbstractState) rather than the resulting state.
package model

import enumspb "go.temporal.io/api/enums/v1"

// Outcome is what the model says the API + resulting state should be. For a rejected or
// no-op call, Next == the input state (the call must not mutate).
//
// These two booleans are not part of AbstractState: a task invalidation is observable only as a
// change in the underlying stamp, not as a distinct state value. The model states, per transition,
// whether each is invalidated, and the harness detects it as a stamp delta across the edge.
type Outcome struct {
	Next                           AbstractState
	Reject                         ErrorKind
	AttemptTasksInvalidated        bool // this transition invalidates the pending attempt dispatch/timer tasks
	ScheduleToCloseTaskInvalidated bool // this transition restarts/invalidates the schedule-to-close timer
}

// Initial is the state immediately after a successful StartActivityExecution.
func Initial(cfg Config) AbstractState {
	s := AbstractState{Status: Scheduled, Count: 1, DispatchTimeSet: true}
	if cfg.HasStartDelay {
		s.Dispatchability = StartDelayPending // first dispatch waits until schedule_time + start_delay
	}
	return s
}

func noop(s AbstractState) Outcome                { return Outcome{Next: s, Reject: NoError} }
func reject(s AbstractState, k ErrorKind) Outcome { return Outcome{Next: s, Reject: k} }

// Transition is the model's total transition function: given the config, the current abstract state,
// and an event, it returns the Outcome (resulting state, reject kind, task-invalidation flags) the
// model requires. It is named for its kinship with the code's activity.Transition* descriptors.
func Transition(cfg Config, s AbstractState, e Event) Outcome {
	if s.Status == Unspecified {
		panic("unreachable: no event is driven from the pre-creation zero value")
	}
	if s.Status.Terminal() {
		return terminalOutcome(s, e)
	}
	switch e.Kind {
	case Poll:
		return poll(cfg, s, e)
	case Heartbeat:
		return heartbeat(cfg, s, e)
	case RespondCompleted:
		return respondCompleted(cfg, s, e)
	case RespondFailed:
		return respondFailed(cfg, s, e)
	case RespondCanceled:
		return respondCanceled(cfg, s, e)
	case RequestCancel:
		return requestCancel(cfg, s, e)
	case Terminate:
		return terminate(cfg, s, e)
	case Pause:
		return pause(cfg, s, e)
	case Unpause:
		return unpause(cfg, s, e)
	case Reset:
		return reset(cfg, s, e)
	case UpdateOptions:
		return updateOptions(cfg, s, e)
	case ScheduleToStartElapses:
		return scheduleToStartElapses(cfg, s, e)
	case ScheduleToCloseElapses:
		return scheduleToCloseElapses(cfg, s, e)
	case StartToCloseElapses:
		return startToCloseElapses(cfg, s, e)
	case HeartbeatElapses:
		return heartbeatElapses(cfg, s, e)
	case StartDelayElapses:
		return startDelayElapses(cfg, s, e)
	case BackoffElapses:
		return backoffElapses(cfg, s, e)
	default:
		panic("model: unhandled event kind")
	}
}

// Each function below returns Outcome{Next: n}, noop(s), or reject(s, kind).
//
// Note: The model implies an "order of precedence": Cancel > Reset > Pause. I.e. you can Cancel in
// {Reset,Pause}Requested, and you can Reset in PauseRequested, but neither a Pause nor a Reset will
// undo a Cancel request.

// PollActivityTaskQueue advances a dispatchable Scheduled attempt to Started.
func poll(_ Config, s AbstractState, _ Event) Outcome {
	if s.Status != Scheduled || s.Dispatchability != Dispatchable {
		return noop(s)
	}
	n := s
	n.Status = Started
	n.FirstAttemptStarted = true
	return Outcome{Next: n}
}

// Worker RespondActivityTaskCompleted with task token completes an in-progress attempt.
func respondCompleted(_ Config, s AbstractState, _ Event) Outcome {
	switch s.Status {
	case Started, PauseRequested, CancelRequested, ResetRequested:
		n := s
		n.Status = Completed
		return Outcome{Next: n}
	case Scheduled, Paused:
		return reject(s, NotFound)
	default:
		panic("model does not handle RespondCompleted while in status " + s.Status.String())
	}
}

// Worker RespondActivityTaskFailed with task token fails an in-progress attempt
func respondFailed(cfg Config, s AbstractState, e Event) Outcome {
	retriesRemaining := cfg.MaxAttempts == 0 || s.Count < cfg.MaxAttempts
	switch s.Status {
	case ResetRequested:
		return applyDeferredReset(cfg, s)
	case Started, PauseRequested:
		n := s
		if e.Retryable && retriesRemaining {
			n.Status = Scheduled
			n.Dispatchability = BackoffPending // the retry waits for the backoff interval
			if s.Status == PauseRequested {
				n.Status = Paused // pause takes effect on the retry
			}
			n.Count++
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
		return reject(s, NotFound) // task token invalid
	default:
		panic("model does not handle RespondFailed while in status " + s.Status.String())
	}
}

// Worker RespondActivityTaskCanceled with task token cancels an in-progress attempt for which
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
		return reject(s, FailedPrecondition) // task token valid
	default:
		panic("model does not handle RespondCanceled while in status " + s.Status.String())
	}
}

// Worker RecordActivityTaskHeartbeat
func heartbeat(_ Config, s AbstractState, _ Event) Outcome {
	// See ExpectedHeartbeatFlags below for the model of heartbeat response flags
	// (CancelRequested / ActivityPaused / ActivityReset).
	switch s.Status {
	case Started, PauseRequested, CancelRequested, ResetRequested:
		return noop(s)
	case Scheduled, Paused:
		return reject(s, NotFound)
	default:
		panic("model does not handle Heartbeat while in status " + s.Status.String())
	}
}

// RequestCancelActivityExecution requests cancellation of an activity.
func requestCancel(_ Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case Scheduled, Paused:
		n := s
		n.Status = Canceled
		// TODO(dan) invalidate attempt tasks?
		return Outcome{Next: n}
	case Started, PauseRequested, ResetRequested:
		n := s
		n.Status = CancelRequested
		return Outcome{Next: n}
	case CancelRequested:
		if e.SameRequestID {
			return noop(s) // requestID-based idempotency
		}
		return reject(s, FailedPrecondition) // TODO(dan): should we consider making is idempotent success even when requestID differs?
	default:
		panic("model does not handle RequestCancel while in status " + s.Status.String())
	}
}

// TerminateActivityExecution from any non-terminal status -> Terminated. Terminal-status handling,
// including idempotent repeat by request id, lives in terminalOutcome.
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
		n.Status = PauseRequested
		// do not invalidate attempt timer tasks
		return Outcome{Next: n}
	case Paused, PauseRequested:
		if e.SameRequestID {
			return noop(s) // requestID-based idempotency
		}
		return reject(s, FailedPrecondition) // "already paused"
	case CancelRequested:
		return reject(s, FailedPrecondition)
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
func unpause(_ Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case Paused:
		n := s
		n.Status = Scheduled
		n.DispatchTimeSet = true
		if e.ResetAttempts {
			n.Count = 1
		}
		// TODO(dan) double-check this is as it should be: we invalidate attempt tasks on Unpause, not on entry to Paused?
		return Outcome{Next: n, AttemptTasksInvalidated: true}
	case PauseRequested:
		// TODO(dan): Unlike CancelRequested and ResetRequested, PauseRequested can be "undone" (by Unpause).
		n := s
		n.Status = Started
		return Outcome{Next: n}
	case ResetRequested:
		n := s
		// TODO(dan): our implementation has Unpause strip the ResetKeepPaused flag from a pending
		// Reset. But is that really what we want? Does it not seem like rather complex/ambitious
		// behavior?
		n.ResetKeepPaused = false // TODO(dan) see comment above; not sure this line should be in the model
		return Outcome{Next: n}
	case Scheduled, Started, CancelRequested:
		// TODO(dan): is it desirable that repeat Unpause are accepted idempotently but other things
		// such as repeat cancel requests are FailedPrecondition? If the repeat Unpauses carry
		// e.ResetAttempts / e.ResetHeartbeat, should they be no-op or reject?
		return noop(s)
	default:
		panic("model does not handle Unpause while in status " + s.Status.String())
	}
}

// ResetActivityExecution makes the activity behave as if starting its first attempt, except the
// schedule-to-close timer keeps running. Applied only once any current attempt has ended.
func reset(cfg Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case Scheduled, Paused:
		n := s
		n.Count = 1
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
		n.ResetRestoreOptions = e.RestoreOriginal
		// Current attempt stays live; do not invalidate its tasks.
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
	case Scheduled, Paused, Started, PauseRequested, CancelRequested, ResetRequested:
		n := s
		if e.SetsStartDelay {
			n.Dispatchability = StartDelayPending
		}
		return Outcome{Next: n, AttemptTasksInvalidated: true, ScheduleToCloseTaskInvalidated: cfg.HasScheduleToClose}
	default:
		panic("model does not handle UpdateOptions while in status " + s.Status.String())
	}
}

// The events below represent timeouts (scheduleToStartElapses, scheduleToCloseElapses,
// startToCloseElapses, heartbeatElapses) or dispatch delays (startDelayElapses, backoffElapses).
// More precisely, they represent the end of a time window configured by the harness: whether or not
// a timer task actually fires around that time depends on whether this nominal timeout or delay
// should correspond to a real one in the product behavior, and on the correctness of the
// implementation. These events never reject; they either cause a transition, or no-op.
//
// An example of a nominal timeout that does not correspond to a real timeout in the product behavior
// is a schedule-to-close time configured shorter than the start delay. Interpreted naively (counting
// from schedule time), its window would end during the start delay. But in fact, under the model,
// the schedule-to-close timer starts to count down at the end of the start delay.

// scheduleToStartElapses represents the end of the nominal schedule-to-start timeout configured by
// the harness. The product behavior is that the timeout starts counting from dispatch time, so a
// start_delay or retry backoff effectively pushes it back.
func scheduleToStartElapses(_ Config, s AbstractState, _ Event) Outcome {
	if s.Status != Scheduled || s.Dispatchability != Dispatchable {
		return noop(s)
	}
	n := s
	n.Status = TimedOut
	return Outcome{Next: n}
}

// scheduleToCloseElapses represents the end of the nominal schedule-to-close timeout configured by
// the harness. The product behavior is that the deadline is anchored at first-dispatch time
// (schedule_time + start_delay), so it does not run during a start_delay, and — unlike a workflow
// activity — is not suspended while paused.
func scheduleToCloseElapses(_ Config, s AbstractState, _ Event) Outcome {
	if s.Dispatchability == StartDelayPending {
		return noop(s)
	}
	n := s
	n.Status = TimedOut
	return Outcome{Next: n}
}

// startToCloseElapses and heartbeatElapses represent the end of the nominal per-attempt timeouts
// configured by the harness. The product behavior is that either one ends the running attempt.
func startToCloseElapses(cfg Config, s AbstractState, _ Event) Outcome {
	return attemptTimedOut(cfg, s)
}
func heartbeatElapses(cfg Config, s AbstractState, _ Event) Outcome {
	return attemptTimedOut(cfg, s)
}

// startDelayElapses represents the end of the nominal start_delay window configured by the harness.
// The product behavior is that the delayed first dispatch becomes available; the status is unchanged
// (a Paused activity stays Paused but now dispatches on unpause).
func startDelayElapses(_ Config, s AbstractState, _ Event) Outcome {
	if s.Dispatchability != StartDelayPending {
		return noop(s)
	}
	n := s
	n.Dispatchability = Dispatchable
	return Outcome{Next: n}
}

// backoffElapses represents the end of the nominal retry-backoff window configured by the harness.
// The product behavior is that the delayed retry dispatch becomes available; symmetric to startDelayElapses.
func backoffElapses(_ Config, s AbstractState, _ Event) Outcome {
	if s.Dispatchability != BackoffPending {
		return noop(s)
	}
	n := s
	n.Dispatchability = Dispatchable
	return Outcome{Next: n}
}

// helpers

// terminalOutcome is the response to any event once the activity has reached a terminal status.
// Worker RPCs see a stale task token (NotFound); operator commands fail the precondition; timeout
// and dispatch-delay events are stale no-ops; Terminate is idempotent success only from Terminated
// with the same request id, else FailedPrecondition.
func terminalOutcome(s AbstractState, e Event) Outcome {
	switch e.Kind {
	case Heartbeat, RespondCompleted, RespondFailed, RespondCanceled:
		return reject(s, NotFound) // stale task token
	case RequestCancel, Pause, Unpause, Reset, UpdateOptions:
		return reject(s, FailedPrecondition)
	case Terminate:
		if s.Status == Terminated && e.SameRequestID {
			return noop(s) // idempotent only from Terminated
		}
		return reject(s, FailedPrecondition)
	case Poll, ScheduleToStartElapses, ScheduleToCloseElapses, StartToCloseElapses,
		HeartbeatElapses, StartDelayElapses, BackoffElapses:
		return noop(s) // no running attempt or dispatch; the event is stale
	default:
		panic("model: unhandled event kind in terminalOutcome")
	}
}

// attemptTimedOut is a shared per-attempt timeout: retry if attempts remain, else TimedOut
func attemptTimedOut(cfg Config, s AbstractState) Outcome {
	switch s.Status {
	case ResetRequested:
		return applyDeferredReset(cfg, s)
	case Started, PauseRequested:
		n := s
		if cfg.MaxAttempts == 0 || s.Count < cfg.MaxAttempts {
			// retry
			n.Status = Scheduled
			if s.Status == PauseRequested {
				n.Status = Paused
			}
			n.Dispatchability = BackoffPending
			n.Count++
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
		// TODO(dan): should this be impossible?
		return noop(s) // no running attempt: timer is stale
	default:
		panic("model does not handle a per-attempt timeout while in status " + s.Status.String())
	}
}

// applyDeferredReset is triggered by failure or timeout. It consumes the reset flags stored while
// the activity was RESET_REQUESTED and applies the reset.
func applyDeferredReset(cfg Config, s AbstractState) Outcome {
	n := s
	n.Count = 1
	n.Dispatchability = Dispatchable // reset discards remaining backoff
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

type HeartbeatFlags struct {
	CancelRequested bool
	ActivityPaused  bool
	ActivityReset   bool
}

func ExpectedHeartbeatFlags(s AbstractState) HeartbeatFlags {
	switch s.Status {
	case Started:
		return HeartbeatFlags{
			ActivityPaused:  false,
			ActivityReset:   false,
			CancelRequested: false,
		}
	case CancelRequested:
		return HeartbeatFlags{
			ActivityPaused:  false,
			ActivityReset:   false,
			CancelRequested: true,
		}
	case ResetRequested:
		return HeartbeatFlags{
			ActivityPaused:  false,
			ActivityReset:   true,
			CancelRequested: false,
		}
	case PauseRequested:
		return HeartbeatFlags{
			ActivityPaused:  true,
			ActivityReset:   false,
			CancelRequested: false,
		}
	default:
		panic("ExpectedHeartbeatFlags: not a token-valid status: " + s.Status.String())
	}
}

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
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_STARTED
	default:
		panic("Unexpected status: " + s.Status.String())
	}
}
