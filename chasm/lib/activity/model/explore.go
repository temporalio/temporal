package model

import (
	"fmt"
	"strings"
)

// Fingerprint identifies a state for graph exploration, bucketing the attempt count so retry loops
// converge to a finite reachable set.
func Fingerprint(s AbstractState) string {
	return fmt.Sprintf("%v|%d|%v|%v|%v|%v|%v|%v",
		s.Status, min(s.Count, 3), s.ResetKeepPaused, s.ResetHeartbeats,
		s.ResetRestoreOptions, s.FirstAttemptStarted, s.DispatchTimeSet, s.Dispatchability)
}

// CellKey identifies a (state, event kind) cell at fingerprint granularity.
func CellKey(s AbstractState, k EventKind) string {
	return Fingerprint(s) + " / " + KindName(k)
}

// NeedsToken reports whether an event is a worker RPC that requires a dispatched task token.
func NeedsToken(k EventKind) bool {
	switch k {
	case Heartbeat, RespondCompleted, RespondFailed, RespondCanceled:
		return true
	default:
		return false
	}
}

// CarriesReqID reports whether an operator command's server-side idempotency is keyed on its request id.
func CarriesReqID(k EventKind) bool {
	switch k {
	case RequestCancel, Terminate, Pause:
		return true
	default:
		return false
	}
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
				cells[CellKey(s, e.Kind)] = true
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

// KindName is a stable label for an event kind, for logs and failure reports.
func KindName(k EventKind) string {
	switch k {
	case Poll:
		return "Poll"
	case Heartbeat:
		return "Heartbeat"
	case RespondCompleted:
		return "RespondCompleted"
	case RespondFailed:
		return "RespondFailed"
	case RespondCanceled:
		return "RespondCanceled"
	case RequestCancel:
		return "RequestCancel"
	case Terminate:
		return "Terminate"
	case Pause:
		return "Pause"
	case Unpause:
		return "Unpause"
	case Reset:
		return "Reset"
	case UpdateOptions:
		return "UpdateOptions"
	case ScheduleToStartElapses:
		return "ScheduleToStartElapses"
	case ScheduleToCloseElapses:
		return "ScheduleToCloseElapses"
	case StartToCloseElapses:
		return "StartToCloseElapses"
	case HeartbeatElapses:
		return "HeartbeatElapses"
	case StartDelayElapses:
		return "StartDelayElapses"
	case BackoffElapses:
		return "BackoffElapses"
	default:
		return fmt.Sprintf("EventKind(%d)", k)
	}
}

// EventLabel names an event and appends the flags that affect its outcome.
func EventLabel(e Event) string {
	var flags []string
	add := func(cond bool, name string) {
		if cond {
			flags = append(flags, name)
		}
	}
	switch e.Kind {
	case RespondFailed:
		flags = append(flags, fmt.Sprintf("retryable=%v", e.Retryable))
	case Reset:
		add(e.KeepPaused, "keepPaused")
		add(e.RestoreOriginal, "restoreOriginal")
	case Unpause:
		add(e.ResetAttempts, "resetAttempts")
		add(e.ResetHeartbeat, "resetHeartbeat")
	case Pause, Terminate, RequestCancel:
		add(e.SameRequestID, "sameRequestID")
	case UpdateOptions:
		add(e.SetsStartDelay, "setsStartDelay")
		add(e.RestoreOriginal, "restoreOriginal")
	}
	if len(flags) == 0 {
		return KindName(e.Kind)
	}
	return fmt.Sprintf("%s[%s]", KindName(e.Kind), strings.Join(flags, ","))
}
