package model

import "fmt"

// Fingerprint identifies a state for graph exploration, bucketing the attempt count so retry loops
// converge to a finite reachable set.
func Fingerprint(s AbstractState) string {
	return fmt.Sprintf("%v|%d|%v|%v",
		s.Status, min(s.AttemptCount, 3), s.ResetKeepPaused, s.Dispatchability)
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
