package umpire

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// The actions model is a declarative layer over the entity lifecycles: an Action is a driver
// operator with preconditions over entity states and effects that fire lifecycle transitions
// (possibly on several entities). A planner assembles actions into sequences that cover the
// edge set; the generic Drive runtime here executes a sequence. This file is domain-agnostic —
// the concrete Temporal actions and their realizers live in tests/umpire1/action. See
// UMPIRE.md.

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
	// LinkedFrom, when set, names the Var of this entity's predecessor. The entity is bound by
	// *observation* — to whatever the LineageOracle reports as that predecessor's successor
	// (continue-as-new / reset / retry) — rather than to a driver-supplied id, because a
	// server-minted successor RunID cannot be known in advance. See UMPIRE.md.
	LinkedFrom string
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
// Temporal realizers) are declared in tests/umpire1/action.
type Action struct {
	Name     string
	Kind     Kind
	Hosting  Hosting
	Requires []Pre
	Effects  []Effect
	Realize  Realizer
	// Entry names the RPC(s) / HTTP path(s) this action issues directly. A Drop of an entry call
	// fails the drive rather than testing resilience, so entry calls are *excluded* from a plan's
	// learned fault targets — the internal/retryable calls a fault can meaningfully perturb are
	// discovered by observing a drive, not declared here (see tests/umpire1/action fault.go:
	// LearnFootprint / FaultTargets).
	Entry []string
	// Footprint names the internal RPC(s) / HTTP path(s) this action is *expected* to trigger
	// downstream (beyond the Entry call it issues directly). It is the wire-level analog of Effects:
	// where Effects declare the lifecycle transitions an action causes, Footprint declares the calls
	// it should make to cause them, reconciled against the observed footprint (see
	// tests/umpire1/action footprint.go: ReconcileFootprint) to catch drift a refactor introduces —
	// a new or removed internal call — that the effect-level check would miss. Opt-in: a nil
	// Footprint is not reconciled.
	Footprint []string
	// Reject, when non-nil, declares this action is expected to be rejected synchronously rather
	// than produce its Effects — an invalid input (malformed / unknown / stale; see UMPIRE.md).
	// Drive treats a Fire error on such an action as the expected outcome (recorded via RejectSink,
	// not a drive failure); the domain side judges the captured error against the rejection
	// contract, since that judgment needs transport knowledge this package deliberately lacks.
	Reject *Reject
}

// Reject declares an action's expected synchronous rejection. It is
// deliberately transport-agnostic: an empty Code means "any client-error class" — the generic
// rejection contract, with the specific grounded on first observation — while a set Code/Message
// pin a grounded or by-design specific.
type Reject struct {
	Code    string // grounded status-code name (e.g. "NotFound"); "" = any client-error class
	Message string // optional substring the rejection message must contain
}

// ValidityClass tags why a mutated value diverges from a field's valid domain.
// It is the negative-space analog of a lifecycle state: the class, not the specific value, is what
// the model reasons about.
type ValidityClass int

const (
	WellFormed ValidityClass = iota // the valid base
	Malformed                       // syntactically invalid (empty, over-long, bad charset, …)
	Unknown                         // well-formed but names something that does not exist
	Stale                           // well-formed, was valid, since superseded (e.g. an old RunID)
	OutOfRange                      // a numeric / enum value outside the allowed set
)

// Variant is one labeled perturbation of a field's valid value and the outcome it should produce.
// Mutate maps the request's current (valid) value to the perturbed one; Expect
// is the outcome the server should produce — for now the rejection contract (an empty Reject means
// "any client-error class", grounded).
type Variant struct {
	Label  string
	Class  ValidityClass
	Mutate func(valid any) any
	Expect *Reject
}

// Domain describes a request field's valid values and its standard invalid neighbors. Concrete
// domains are reflected from the proto descriptor on the domain side; this package holds only the
// abstract shape so the schema and planner can reason about params
// uniformly, without any proto/RPC dependency.
type Domain interface {
	Variants() []Variant
}

// Param binds a request field path to its Domain. The full set of an action's
// params is usually enumerated by reflecting the request descriptor, not hand-authored.
type Param struct {
	Path   string
	Domain Domain
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

// RejectSink is optionally implemented by a RealizeContext to capture an action's synchronous
// rejection (see Action.Reject). When an action declares a Reject, Drive records the Fire outcome
// here — the error on rejection, or nil if the request was (unexpectedly) accepted — and continues
// instead of aborting the drive, leaving the domain side to judge it against the contract.
type RejectSink interface {
	ObserveReject(action string, err error)
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
		if err := observeExecution(ctx, rc, ExecutionObservation{
			Kind:    ExecutionActionStart,
			Action:  a.Name,
			Phase:   "install",
			Outcome: ExecutionOutcomeStarted,
		}); err != nil {
			return fmt.Errorf("observe install %s: %w", a.Name, err)
		}
		if err := a.Realize.Install(rc, a); err != nil {
			if observeErr := observeExecution(ctx, rc, ExecutionObservation{
				Kind:       ExecutionActionFinish,
				Action:     a.Name,
				Phase:      "install",
				Outcome:    ExecutionOutcomeFailed,
				ErrorClass: ExecutionErrorClass(err),
			}); observeErr != nil {
				return errors.Join(fmt.Errorf("install %s: %w", a.Name, err), fmt.Errorf("observe install failure %s: %w", a.Name, observeErr))
			}
			return fmt.Errorf("install %s: %w", a.Name, err)
		}
	}
	for _, a := range plan {
		// Only proactive actions wait on their preconditions: they fire an RPC at a point, so
		// the entity must already be in the required (stable) state. A reactive action is
		// installed up front and fires server-side when the entity passes through its
		// precondition state — a transient the client can't reliably observe — so waiting on it
		// would race; its effect is confirmed by the next proactive action's precondition.
		if proactive(a.Kind) {
			for _, p := range a.Requires {
				if err := awaitState(ctx, rc, oracle, poll, p.Ref, p.State); err != nil {
					return fmt.Errorf("%s precondition %s@%s: %w", a.Name, p.Ref.Var, p.State, err)
				}
			}
		}
		err := a.Realize.Fire(ctx, rc, a)
		if a.Reject != nil {
			// A declared rejection is the expected outcome, not a drive failure: record the Fire
			// result (the error, or nil if the request was accepted) for the domain side to judge,
			// and move on. Such an action produces no effects, so there is nothing to confirm.
			if sink, ok := rc.(RejectSink); ok {
				sink.ObserveReject(a.Name, err)
			}
			if observeErr := observeExecution(ctx, rc, ExecutionObservation{
				Kind:       ExecutionActionFinish,
				Action:     a.Name,
				Phase:      "fire",
				Outcome:    ExecutionOutcomeRejected,
				ErrorClass: ExecutionErrorClass(err),
			}); observeErr != nil {
				return fmt.Errorf("observe rejection %s: %w", a.Name, observeErr)
			}
			continue
		}
		if err != nil {
			if observeErr := observeExecution(ctx, rc, ExecutionObservation{
				Kind:       ExecutionActionFinish,
				Action:     a.Name,
				Phase:      "fire",
				Outcome:    ExecutionOutcomeFailed,
				ErrorClass: ExecutionErrorClass(err),
			}); observeErr != nil {
				return errors.Join(fmt.Errorf("fire %s: %w", a.Name, err), fmt.Errorf("observe fire failure %s: %w", a.Name, observeErr))
			}
			return fmt.Errorf("fire %s: %w", a.Name, err)
		}
		if err := observeExecution(ctx, rc, ExecutionObservation{
			Kind:    ExecutionActionFinish,
			Action:  a.Name,
			Phase:   "fire",
			Outcome: ExecutionOutcomeSucceeded,
		}); err != nil {
			return fmt.Errorf("observe fire %s: %w", a.Name, err)
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
				if observeErr := observeExecution(ctx, rc, ExecutionObservation{
					Kind:       ExecutionVerdict,
					Action:     last.Name,
					Checkpoint: "endpoint",
					ErrorClass: ExecutionErrorClass(err),
					Pass:       false,
					Violations: 1,
				}); observeErr != nil {
					return errors.Join(fmt.Errorf("%s effect %s:%s (→%s) not confirmed: %w", last.Name, e.Ref.Var, e.Event, dst, err), fmt.Errorf("observe endpoint verdict %s: %w", last.Name, observeErr))
				}
				return fmt.Errorf("%s effect %s:%s (→%s) not confirmed: %w", last.Name, e.Ref.Var, e.Event, dst, err)
			}
		}
		if err := observeExecution(ctx, rc, ExecutionObservation{
			Kind:       ExecutionVerdict,
			Action:     last.Name,
			Checkpoint: "endpoint",
			Pass:       true,
		}); err != nil {
			return fmt.Errorf("observe endpoint verdict %s: %w", last.Name, err)
		}
	}
	return nil
}

func observeExecution(ctx context.Context, rc RealizeContext, observed ExecutionObservation) error {
	observer, ok := rc.(ExecutionObserver)
	if !ok || observer == nil {
		return nil
	}
	return observer.ObserveExecution(ctx, observed)
}

// proactive reports whether an action is fired at a point (and so must wait for its
// preconditions) rather than installed to fire reactively / stand by.
func proactive(k Kind) bool {
	switch k {
	case ClientRPC, WorkerCommand, CompletionCallback:
		return true
	default: // HandlerResponse, Timer, Fault
		return false
	}
}

// LineageOracle optionally reports the successor an entity produced (the run created from it via
// continue-as-new / reset / retry), so Drive can bind a LinkedFrom ref by observation rather than a
// server-minted id the driver could not know in advance. Implemented Temporal-side over the run
// graph (see UMPIRE.md).
type LineageOracle interface {
	Successor(t EntityType, predecessorID string) (string, bool)
}

// awaitState blocks until the entity bound to ref is observed in state, or ctx is done. A ref with
// LinkedFrom is bound lazily, on observation, to its predecessor's successor.
func awaitState(ctx context.Context, rc RealizeContext, oracle StateOracle, poll time.Duration, ref Ref, state string) error {
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		bindLineage(rc, oracle, ref)
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

// bindLineage binds a not-yet-bound LinkedFrom ref to the successor the LineageOracle reports for
// its (bound) predecessor — bind-on-observation. A no-op until the predecessor is bound and its
// successor observed, so it is safe to call every poll tick.
func bindLineage(rc RealizeContext, oracle StateOracle, ref Ref) {
	if ref.LinkedFrom == "" {
		return
	}
	if _, ok := rc.Binding(ref.Var); ok {
		return
	}
	lo, ok := oracle.(LineageOracle)
	if !ok {
		return
	}
	predID, ok := rc.Binding(ref.LinkedFrom)
	if !ok {
		return
	}
	if succID, ok := lo.Successor(ref.Type, predID); ok {
		rc.Bind(ref.Var, succID)
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
