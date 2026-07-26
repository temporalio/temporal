package model

import (
	"fmt"
	"strings"
)

// Fingerprint identifies a state for graph exploration, bucketing the attempt count so retry loops
// converge to a finite reachable set.
func Fingerprint(s AbstractState) string {
	return fmt.Sprintf("%v|%d|%v|%v|%v|%v|%v|%v",
		s.Status, min(s.AttemptCount, 3), s.ResetKeepPaused, s.ResetHeartbeats,
		s.ResetRestoreOptions, s.FirstAttemptStarted, s.DispatchTimeSet, s.Dispatchability)
}

// CellKey identifies a (state, event type) cell at fingerprint granularity.
func CellKey(s AbstractState, k EventType) string {
	return Fingerprint(s) + " / " + k.String()
}

// NeedsToken reports whether an event is a worker RPC that requires a dispatched task token.
func NeedsToken(k EventType) bool {
	switch k {
	case HeartbeatType, RespondCompletedType, RespondFailedType, RespondCanceledType:
		return true
	default:
		return false
	}
}

// CarriesReqID reports whether an operator command's server-side idempotency is keyed on its request id.
func CarriesReqID(k EventType) bool {
	switch k {
	case RequestCancelType, TerminateType, PauseType:
		return true
	default:
		return false
	}
}

// Possible reports whether event type t can occur in state s. This is not whether the state machine
// would accept it: a client can always send an RPC, and a rejection is an occurrence a trace may
// assert. A clock event cannot occur unless its clock is running, which its transition function
// already decides, so this asks that function.
func Possible(cfg Config, s AbstractState, t EventType) bool {
	return !Transition(cfg, s, Event{Type: t}).Impossible
}

// ValidateTrace walks trace from Initial(cfg) and reports the first event that is not Possible in the
// state it would be driven from.
func ValidateTrace(cfg Config, trace []Event) error {
	s := Initial(cfg)
	for i, e := range trace {
		if !Possible(cfg, s, e.Type) {
			return fmt.Errorf("trace[%d] %s cannot occur in %v/%v: its clock is not running there. Remove "+
				"it, or drive the events that start its clock first", i, e, s.Status, s.Dispatchability)
		}
		s = Transition(cfg, s, e).Next
	}
	return nil
}

// Reachable computes, purely from Transition (no driver), every (state, event) cell reachable from
// Initial(cfg) by following non-reject edges to fixpoint (states deduped by Fingerprint).
func Reachable(cfg Config, events []Event) map[string]bool {
	cells := map[string]bool{}
	start := Initial(cfg)
	visited := map[string]bool{Fingerprint(start): true}
	frontier := []AbstractState{start}
	for len(frontier) > 0 {
		var next []AbstractState
		for _, s := range frontier {
			for _, e := range events {
				out := Transition(cfg, s, e)
				cells[CellKey(s, e.Type)] = true
				if out.Reject != NoError {
					continue
				}
				fp := Fingerprint(out.Next)
				if !visited[fp] {
					visited[fp] = true
					next = append(next, out.Next)
				}
			}
		}
		frontier = next
	}
	return cells
}

// String is a stable label for an event type, for logs and failure reports.
func (t EventType) String() string {
	switch t {
	case PollType:
		return "Poll"
	case HeartbeatType:
		return "Heartbeat"
	case RespondCompletedType:
		return "RespondCompleted"
	case RespondFailedType:
		return "RespondFailed"
	case RespondCanceledType:
		return "RespondCanceled"
	case RequestCancelType:
		return "RequestCancel"
	case TerminateType:
		return "Terminate"
	case PauseType:
		return "Pause"
	case UnpauseType:
		return "Unpause"
	case ResetType:
		return "Reset"
	case UpdateOptionsType:
		return "UpdateOptions"
	case ScheduleToStartElapsesType:
		return "ScheduleToStartElapses"
	case ScheduleToCloseElapsesType:
		return "ScheduleToCloseElapses"
	case StartToCloseElapsesType:
		return "StartToCloseElapses"
	case HeartbeatElapsesType:
		return "HeartbeatElapses"
	case StartDelayElapsesType:
		return "StartDelayElapses"
	case BackoffElapsesType:
		return "BackoffElapses"
	default:
		return fmt.Sprintf("EventType(%d)", int(t))
	}
}

// String names an event and appends the flags that affect its outcome.
func (e Event) String() string {
	var flags []string
	add := func(cond bool, name string) {
		if cond {
			flags = append(flags, name)
		}
	}
	switch e.Type {
	case RespondFailedType:
		flags = append(flags, fmt.Sprintf("retryable=%v", e.Retryable))
	case ResetType:
		add(e.KeepPaused, "keepPaused")
		add(e.RestoreOriginal, "restoreOriginal")
	case UnpauseType:
		add(e.ResetAttempts, "resetAttempts")
		add(e.ResetHeartbeat, "resetHeartbeat")
	case PauseType, TerminateType, RequestCancelType:
		add(e.SameRequestID, "sameRequestID")
	case UpdateOptionsType:
		add(e.SetsStartDelay, "setsStartDelay")
		add(e.RestoreOriginal, "restoreOriginal")
	}
	if len(flags) == 0 {
		return e.Type.String()
	}
	return fmt.Sprintf("%s[%s]", e.Type, strings.Join(flags, ","))
}
