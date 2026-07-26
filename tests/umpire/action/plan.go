package action

import (
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common/testing/testhooks"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/model"
	"go.temporal.io/server/tests/umpire/planner"
)

// actionFor resolves the atomic action that causes `event` from state `from` under `hosting`.
// It is the registry: the event picks the action, and where an event has several producers the
// from-state and hosting disambiguate (schedule → standalone RPC vs embedded command; succeed
// from scheduled is a handler result vs from started is a completion callback). See
// UMPIRE_ACTIONS.md.
func actionFor(from, event string, hosting umpire.Hosting) (umpire.Action, bool) {
	switch event {
	case model.NexusSchedule:
		if hosting == umpire.Standalone {
			return StartStandalone, true
		}
		return ScheduleEmbedded, true
	case model.NexusStart:
		return HandlerAsyncAck, true
	case model.NexusAttemptFailed:
		return HandlerRetryable, true
	case model.NexusSucceed:
		if from == model.NexusStarted {
			return CompleteWith(nil, model.NexusSucceed), true
		}
		return HandlerSyncOk, true
	case model.NexusFail:
		if from == model.NexusStarted {
			return CompleteWith(nexus.NewOperationFailedError("umpire action: injected async failure"), model.NexusFail), true
		}
		return HandlerOpFailed, true
	case model.NexusCancel:
		if from == model.NexusStarted {
			return CompleteWith(nexus.NewOperationCanceledError("umpire action: injected async cancellation"), model.NexusCancel), true
		}
		return HandlerOpCanceled, true
	case model.NexusTimeout:
		switch from {
		case model.NexusScheduled:
			return TimerForceTimeout(testhooks.NexusForceTimeoutFromScheduled), true
		case model.NexusBackingOff:
			return TimerForceTimeout(testhooks.NexusForceTimeoutFromBackingOff), true
		default: // started: the force-timeout hook fires on the attempt, not once started —
			// this edge needs a real schedule-to-close timer, so there is no atomic action.
			return umpire.Action{}, false
		}
	case model.NexusTerminate:
		return TerminateFrom(from), true
	}
	return umpire.Action{}, false
}

// PlanEdge assembles the action sequence that traverses the edge (from --event--> …) on the
// NexusOperation model under `hosting`: it routes to `from` over the FSM (the existing entity
// planner) and maps each route event — plus the target event — to the action that causes it.
// This is the actions-model planner: the hand-written plans are exactly what it computes.
func PlanEdge(from, event string, hosting umpire.Hosting) ([]umpire.Action, error) {
	lc, ok := planner.DefaultModels().Lifecycle(string(model.NexusOperationType))
	if !ok {
		return nil, fmt.Errorf("no NexusOperation lifecycle")
	}
	// The target edge must itself be reachable under this hosting (the route below already
	// respects it). e.g. terminate is Standalone-only, so it is unplannable under Embedded.
	if h := lc.EdgeHosting(from, event); h != umpire.AnyHosting && hosting != umpire.AnyHosting && h != hosting {
		return nil, fmt.Errorf("edge %s --%s--> is %s-only, not reachable under %s", from, event, h, hosting)
	}
	var route []string
	if from != lc.Initial() {
		plan, err := umpire.PlanTo(lc, from, umpire.Shortest, umpire.Constraints{Hosting: hosting})
		if err != nil {
			return nil, fmt.Errorf("route to %s: %w", from, err)
		}
		route = plan.Routes[0]
	}
	events := append(append([]string{}, route...), event)

	seq := make([]umpire.Action, 0, len(events))
	state := lc.Initial()
	for _, ev := range events {
		a, ok := actionFor(state, ev, hosting)
		if !ok {
			return nil, fmt.Errorf("no action realizes %s from %s under %s", ev, state, hosting)
		}
		seq = append(seq, a)
		if dst, ok := lc.Destination(ev); ok {
			state = dst
		}
	}
	return seq, nil
}

// AutoCoverPlans computes the set of plans that together traverse every model edge the planner
// can reach — the coverage goal becomes a computed list, not a hand-written one. It drives one
// plan per *settling* edge (an edge landing on a terminal state); the path edges to reach it
// are covered along the way. An edge is driven under the hosting its HostedIn trait requires
// (Standalone for terminate), else Embedded. Two server-timer edges have no atomic action and
// are skipped (PlanEdge returns an error): the backing_off→scheduled retry reschedule and
// started→timed_out — they still need bespoke drives.
func AutoCoverPlans() [][]umpire.Action {
	lc, ok := planner.DefaultModels().Lifecycle(string(model.NexusOperationType))
	if !ok {
		return nil
	}
	var plans [][]umpire.Action
	for _, e := range lc.Edges() {
		if !lc.Terminal(e.To) {
			continue // drive only settling edges; their prefixes cover the rest
		}
		hosting := umpire.Embedded
		if lc.EdgeHosting(e.From, e.Event) == umpire.Standalone {
			hosting = umpire.Standalone
		}
		if seq, err := PlanEdge(e.From, e.Event, hosting); err == nil {
			plans = append(plans, seq)
		}
	}
	return plans
}

// mustPlan is PlanEdge for the known-good edges the named plan constructors expose; it panics
// on a planning error (a programming mistake in the model or registry, not a runtime failure).
func mustPlan(from, event string, hosting umpire.Hosting) []umpire.Action {
	seq, err := PlanEdge(from, event, hosting)
	if err != nil {
		panic(err)
	}
	return seq
}
