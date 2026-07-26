package umpire

import (
	"context"
	"fmt"
	"time"
)

// The actions model is a declarative layer over the entity lifecycles: an Action is a driver
// operator with preconditions over entity states and effects that fire lifecycle transitions
// (possibly on several entities). A planner assembles actions into sequences that cover the
// edge set; the generic Drive runtime here executes a sequence. This file is domain-agnostic —
// the concrete Temporal actions and their realizers live in tests/umpire/action. See
// UMPIRE_ACTIONS.md.

// Kind is how an action is physically realized against a live environment. It also fixes the
// action's temporal mode: proactive kinds are fired at a point (once preconditions hold);
// reactive/standing kinds are installed up front and fire when the server reaches them.
type Kind int

const (
	ClientRPC          Kind = iota // client → frontend RPC                         (proactive)
	WorkerCommand                  // a worker decision / command (e.g. kitchensink) (proactive)
	HandlerResponse                // the mock handler's reaction                    (reactive)
	CompletionCallback             // deliver an async completion                    (proactive)
	Timer                          // a server timer forced via a test hook          (reactive)
	Fault                          // perturb another action's footprint             (standing)
)

// Ref selects an entity within an action. Var binds it across the action's preconditions and
// effects and across a plan; a Fresh effect mints a new entity, other refs reuse a bound Var.
type Ref struct {
	Type  EntityType
	Var   string
	Fresh bool
}

// Pre is a precondition: the entity bound to Ref.Var must currently be in State.
type Pre struct {
	Ref   Ref
	State string
}

// Effect is what an action causes: firing Event (a lifecycle transition) on Ref.
type Effect struct {
	Ref   Ref
	Event string
}

// Action is a declarative driver operator. It is domain-agnostic; concrete actions (with
// Temporal realizers) are declared in tests/umpire/action.
type Action struct {
	Name      string
	Kind      Kind
	Hosting   Hosting
	Requires  []Pre
	Effects   []Effect
	Realize   Realizer
	Faultable []string // footprint points (RPC method / HTTP path) a Fault may attach to
}

// RealizeContext is the opaque handle a Realizer operates on. The generic runtime passes it
// through unexamined; a concrete Realizer type-asserts it to reach the live environment. It
// also carries the running Var→entity-identity bindings a plan accumulates.
type RealizeContext interface {
	Binding(varName string) (string, bool)
	Bind(varName, id string)
}

// Realizer drives one action. Reactive/standing kinds do their work in Install (before any
// firing); proactive kinds do it in Fire (once preconditions hold). Each kind implements the
// half it needs and leaves the other a no-op.
type Realizer interface {
	Install(rc RealizeContext, a Action) error
	Fire(ctx context.Context, rc RealizeContext, a Action) error
}

// StateOracle reports the current observed state of a bound entity — implemented Temporal-side
// over the Monitor's ModelState, so the runtime can wait on preconditions without knowing the
// Monitor.
type StateOracle interface {
	Current(t EntityType, id string) (string, bool)
}

// EffectResolver maps an effect's event to the state it leads to, so Drive can confirm the plan
// reached its endpoint. Implemented over the entity lifecycles (see Lifecycle.Destination).
type EffectResolver interface {
	Destination(t EntityType, event string) (string, bool)
}

// Drive is the generic runtime: it installs every action's standing config, fires the proactive
// actions in order (each blocking on its preconditions via the oracle), then confirms the final
// action's effects have been observed. All Temporal knowledge stays behind the interfaces.
func Drive(ctx context.Context, rc RealizeContext, oracle StateOracle, resolver EffectResolver, poll time.Duration, plan []Action) error {
	for _, a := range plan {
		if err := a.Realize.Install(rc, a); err != nil {
			return fmt.Errorf("install %s: %w", a.Name, err)
		}
	}
	for _, a := range plan {
		for _, p := range a.Requires {
			if err := awaitState(ctx, rc, oracle, poll, p.Ref, p.State); err != nil {
				return fmt.Errorf("%s precondition %s@%s: %w", a.Name, p.Ref.Var, p.State, err)
			}
		}
		if err := a.Realize.Fire(ctx, rc, a); err != nil {
			return fmt.Errorf("fire %s: %w", a.Name, err)
		}
	}
	// Confirm the plan's endpoint: the final action's effects must be observed. Earlier
	// effects are already confirmed by the preconditions of the actions that follow them.
	if len(plan) > 0 && resolver != nil {
		last := plan[len(plan)-1]
		for _, e := range last.Effects {
			dst, ok := resolver.Destination(e.Ref.Type, e.Event)
			if !ok {
				continue
			}
			if err := awaitState(ctx, rc, oracle, poll, e.Ref, dst); err != nil {
				return fmt.Errorf("%s effect %s:%s (→%s) not confirmed: %w", last.Name, e.Ref.Var, e.Event, dst, err)
			}
		}
	}
	return nil
}

// awaitState blocks until the entity bound to ref is observed in state, or ctx is done.
func awaitState(ctx context.Context, rc RealizeContext, oracle StateOracle, poll time.Duration, ref Ref, state string) error {
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		if id, ok := rc.Binding(ref.Var); ok {
			if cur, ok := oracle.Current(ref.Type, id); ok && cur == state {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// VisitedOracle reports the edges an entity has traversed — implemented Temporal-side over the
// entity's VisitedEdges, so Reconcile can ground declared effects against reality.
type VisitedOracle interface {
	Visited(t EntityType, id string) ([]Edge, bool)
}

// Drift is a declared effect that was not observed — an action claiming an effect it did not
// produce (or one whose entity never appeared).
type Drift struct {
	Action string
	Effect Effect
	Reason string
}

func (d Drift) String() string {
	return fmt.Sprintf("%s: effect %s:%s %s", d.Action, d.Effect.Ref.Var, d.Effect.Event, d.Reason)
}

// Reconcile grounds the actions model against reality: it returns every declared effect that
// the run did not actually produce (the entity never traversed an edge with that event). Same
// intent as the FSM conformance check, one layer up.
func Reconcile(vo VisitedOracle, rc RealizeContext, plan []Action) []Drift {
	var drift []Drift
	for _, a := range plan {
		for _, e := range a.Effects {
			id, ok := rc.Binding(e.Ref.Var)
			if !ok {
				drift = append(drift, Drift{a.Name, e, "entity never bound"})
				continue
			}
			edges, ok := vo.Visited(e.Ref.Type, id)
			if !ok {
				drift = append(drift, Drift{a.Name, e, "entity never observed"})
				continue
			}
			if !visitedEvent(edges, e.Event) {
				drift = append(drift, Drift{a.Name, e, "event not observed"})
			}
		}
	}
	return drift
}

func visitedEvent(edges []Edge, event string) bool {
	for _, e := range edges {
		if e.Event == event {
			return true
		}
	}
	return false
}
