package conformance

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"

	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/model"
)

var allSpecStatuses = []model.Status{
	model.Unspecified, model.Scheduled, model.Started, model.CancelRequested,
	model.Completed, model.Failed, model.Canceled, model.Terminated, model.TimedOut,
	model.Paused, model.PauseRequested, model.ResetRequested,
}

var allEventKinds = []model.EventKind{
	model.Poll, model.Heartbeat, model.RespondCompleted, model.RespondFailed,
	model.RespondCanceled, model.RequestCancel, model.Terminate, model.Pause,
	model.Unpause, model.Reset, model.UpdateOptions,
}

var cfgs = []model.Config{
	{}, // minimal: no schedule-to-close, unlimited attempts
	{HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, MaxAttempts: 3},
}

var countValues = []int32{1, 2}

// eventsFor returns a representative set of events for a kind, covering the flags that affect
// the outcome.
func eventsFor(k model.EventKind) []model.Event {
	bools := []bool{false, true}
	var out []model.Event
	switch k {
	case model.RespondFailed:
		for _, r := range bools {
			out = append(out, model.Event{Kind: k, Retryable: r})
		}
	case model.Reset:
		for _, kp := range bools {
			for _, ro := range bools {
				for _, rh := range bools {
					out = append(out, model.Event{Kind: k, KeepPaused: kp, RestoreOriginal: ro, ResetHeartbeat: rh})
				}
			}
		}
	case model.Unpause:
		for _, ra := range bools {
			for _, rh := range bools {
				out = append(out, model.Event{Kind: k, ResetAttempts: ra, ResetHeartbeat: rh})
			}
		}
	case model.Pause, model.Terminate, model.RequestCancel:
		for _, sr := range bools {
			out = append(out, model.Event{Kind: k, SameRequestID: sr})
		}
	default:
		out = append(out, model.Event{Kind: k})
	}
	return out
}

func srcState(cfg model.Config, st model.Status, keepPaused bool, count int32) model.AbstractState {
	s := model.AbstractState{Status: st, AttemptCount: count, ResetKeepPaused: keepPaused}
	switch st {
	case model.Unspecified, model.Scheduled:
	default:
		s.FirstAttemptStarted = true
	}
	return s
}

// evalModel calls Model, classifying an "unreachable" assertion panic separately from any other
// (unexpected) panic instead of crashing the enumeration.
type verdict int

const (
	decided     verdict = iota // Model returned an Outcome
	unreachable                // Model panicked with "unreachable": author asserts this can't happen
	unexpected                 // Model panicked with something else: a bug
)

func evalModel(cfg model.Config, s model.AbstractState, e model.Event) (out model.Outcome, v verdict, panicMsg string) {
	defer func() {
		if r := recover(); r != nil {
			panicMsg = fmt.Sprint(r)
			if strings.Contains(panicMsg, "unreachable") {
				v = unreachable
			} else {
				v = unexpected
			}
		}
	}()
	out = model.Transition(cfg, s, e)
	v = decided
	return
}

// --- checks --------------------------------------------------------------------------------

// TestModelDecisionCoverage asserts Model is total over the RPC domain: every (status, event)
// cell either returns an Outcome or is an explicit unreachable assertion (the pre-creation
// Unspecified status). Any other panic is a bug in Model or here and fails the test.
type cell struct {
	status model.Status
	kind   model.EventKind
}

func TestModelDecisionCoverage(t *testing.T) {
	decidedCells := map[cell]bool{}
	var counts [3]int

	for _, cfg := range cfgs {
		for _, st := range allSpecStatuses {
			for _, kp := range []bool{false, true} {
				for _, ct := range countValues {
					for _, k := range allEventKinds {
						for _, e := range eventsFor(k) {
							_, v, msg := evalModel(cfg, srcState(cfg, st, kp, ct), e)
							counts[v]++
							switch v {
							case decided, unreachable:
								decidedCells[cell{st, k}] = true
							case unexpected:
								t.Errorf("unexpected panic: status=%s kind=%s event=%+v: %s",
									st, kindName(k), e, msg)
							}
						}
					}
				}
			}
		}
	}

	t.Logf("cells evaluated: decided=%d unreachable=%d unexpected=%d",
		counts[decided], counts[unreachable], counts[unexpected])
	t.Logf("distinct (status,event) covered: %d", len(decidedCells))
}

// TestModelEdgesReachableInCode asserts that every status change the spec accepts can actually
// be produced by the code: if Model moves the activity from A to a different status B, then B
// must be reachable from A by following one or more declared transitions.
func TestModelEdgesReachableInCode(t *testing.T) {
	reach := codeReachability()

	// Deduplicate reported failures so one missing edge is not printed hundreds of times.
	reported := map[[2]model.Status]bool{}

	for _, cfg := range cfgs {
		for _, st := range allSpecStatuses {
			for _, kp := range []bool{false, true} {
				for _, ct := range countValues {
					for _, k := range allEventKinds {
						for _, e := range eventsFor(k) {
							s := srcState(cfg, st, kp, ct)
							out, v, _ := evalModel(cfg, s, e)
							if v != decided || out.Reject != model.NoError {
								continue
							}
							if out.Next.Status == st {
								continue // no status change, no structural claim
							}
							key := [2]model.Status{st, out.Next.Status}
							if reported[key] {
								continue
							}
							src := specToProto(st)
							dst := specToProto(out.Next.Status)
							if !reach[src][dst] {
								reported[key] = true
								t.Errorf("spec accepts %s --%s--> %s, but the code cannot reach %s from %s via any transition path",
									st, kindName(k), out.Next.Status, out.Next.Status, st)
							}
						}
					}
				}
			}
		}
	}
}

// --- reading the code's transition graph ---------------------------------------------------

// codeTransition names an exported activity.Transition* for reporting.
type codeTransition struct {
	name string
	tr   any
}

var codeTransitions = []codeTransition{
	{"Scheduled", activity.TransitionScheduled},
	{"Rescheduled", activity.TransitionRescheduled},
	{"Started", activity.TransitionStarted},
	{"Completed", activity.TransitionCompleted},
	{"Failed", activity.TransitionFailed},
	{"Terminated", activity.TransitionTerminated},
	{"CancelRequested", activity.TransitionCancelRequested},
	{"Canceled", activity.TransitionCanceled},
	{"TimedOut", activity.TransitionTimedOut},
	{"Paused", activity.TransitionPaused},
	{"PauseRequested", activity.TransitionPauseRequested},
	{"Unpaused", activity.TransitionUnpaused},
	{"UnpausedWhilePauseRequested", activity.TransitionUnpausedWhilePauseRequested},
	{"AttemptFailedWhilePauseRequested", activity.TransitionAttemptFailedWhilePauseRequested},
	{"Reset", activity.TransitionReset},
	{"ResetRequested", activity.TransitionResetRequested},
	{"ResetAttemptFailedToPaused", activity.TransitionResetAttemptFailedToPaused},
	{"ResetAttemptFailedToScheduled", activity.TransitionResetAttemptFailedToScheduled},
}

// codeAdjacency reads Sources and Destination off each transition by reflection (the fields are
// exported; the transitions' event type parameters differ and many are unexported, so reflection
// is the uniform way to read them).
func codeAdjacency() map[activitypb.ActivityExecutionStatus][]activitypb.ActivityExecutionStatus {
	adj := map[activitypb.ActivityExecutionStatus][]activitypb.ActivityExecutionStatus{}
	for _, ct := range codeTransitions {
		v := reflect.ValueOf(ct.tr)
		dst := v.FieldByName("Destination").Interface().(activitypb.ActivityExecutionStatus)
		srcs := v.FieldByName("Sources")
		for i := 0; i < srcs.Len(); i++ {
			src := srcs.Index(i).Interface().(activitypb.ActivityExecutionStatus)
			adj[src] = append(adj[src], dst)
		}
	}
	return adj
}

// codeReachability returns, for each status, the set of statuses reachable via one or more
// declared transitions.
func codeReachability() map[activitypb.ActivityExecutionStatus]map[activitypb.ActivityExecutionStatus]bool {
	adj := codeAdjacency()
	reach := map[activitypb.ActivityExecutionStatus]map[activitypb.ActivityExecutionStatus]bool{}
	for _, start := range allProtoStatuses() {
		seen := map[activitypb.ActivityExecutionStatus]bool{}
		queue := append([]activitypb.ActivityExecutionStatus{}, adj[start]...)
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			if seen[cur] {
				continue
			}
			seen[cur] = true
			queue = append(queue, adj[cur]...)
		}
		reach[start] = seen
	}
	return reach
}

func allProtoStatuses() []activitypb.ActivityExecutionStatus {
	out := make([]activitypb.ActivityExecutionStatus, 0, len(allSpecStatuses))
	for _, s := range allSpecStatuses {
		out = append(out, specToProto(s))
	}
	return out
}

func specToProto(s model.Status) activitypb.ActivityExecutionStatus {
	switch s {
	case model.Unspecified:
		return activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	case model.Scheduled:
		return activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED
	case model.Started:
		return activitypb.ACTIVITY_EXECUTION_STATUS_STARTED
	case model.CancelRequested:
		return activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED
	case model.Completed:
		return activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED
	case model.Failed:
		return activitypb.ACTIVITY_EXECUTION_STATUS_FAILED
	case model.Canceled:
		return activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED
	case model.Terminated:
		return activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED
	case model.TimedOut:
		return activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	case model.Paused:
		return activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED
	case model.PauseRequested:
		return activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED
	case model.ResetRequested:
		return activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED
	default:
		panic(fmt.Sprintf("specToProto: unknown status %v", s))
	}
}

func kindName(k model.EventKind) string {
	switch k {
	case model.Poll:
		return "Poll"
	case model.Heartbeat:
		return "Heartbeat"
	case model.RespondCompleted:
		return "RespondCompleted"
	case model.RespondFailed:
		return "RespondFailed"
	case model.RespondCanceled:
		return "RespondCanceled"
	case model.RequestCancel:
		return "RequestCancel"
	case model.Terminate:
		return "Terminate"
	case model.Pause:
		return "Pause"
	case model.Unpause:
		return "Unpause"
	case model.Reset:
		return "Reset"
	case model.UpdateOptions:
		return "UpdateOptions"
	default:
		return fmt.Sprintf("EventKind(%d)", k)
	}
}
