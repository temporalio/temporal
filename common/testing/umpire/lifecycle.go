package umpire

import (
	"context"
	"fmt"
	"iter"
	"sort"
	"strings"
	"time"

	"github.com/looplab/fsm"
)

// Transition is one edge of a Lifecycle's state machine: firing Event moves the
// entity from any of the From states to To. Traits annotate the edge (see Trait);
// the built-in one is Needs, declaring the drive-capability the edge requires.
type Transition struct {
	Event  string
	From   []string
	To     string
	Traits Traits
}

// TransitionKind is the three-valued verdict of applying an event in a given
// state — the output of the Lifecycle's executable transition function. Modelling
// every (state, event) pair with one of these three outcomes is what turns a bare
// FSM into an oracle: there is no fourth "we never thought about this" case that
// silently passes.
type TransitionKind int

const (
	// Advance: the event is a legal edge to a new state, or a forward jump to a
	// reachable state over intermediate states that were not observed; the machine moves.
	Advance TransitionKind = iota
	// NoOp: the event is not a forward edge, but it is a benign re-observation — a
	// duplicate / late / out-of-order fact consistent with the progress already
	// made, or any event arriving once the entity is terminal. State is unchanged
	// and it is NOT a violation. This is the case that made the generic
	// transition-legality rule too noisy to enforce before it was modelled.
	NoOp
	// Illegal: the event is impossible given the observed history — neither a legal
	// edge, a benign re-observation, nor a reachable forward jump (its target is in
	// a branch unreachable from and unable to reach the current state). A real violation.
	Illegal
)

func (k TransitionKind) String() string {
	switch k {
	case Advance:
		return "Advance"
	case NoOp:
		return "NoOp"
	case Illegal:
		return "Illegal"
	default:
		return "TransitionKind(?)"
	}
}

// Outcome is the predicted result of applying an event from a state: the pure,
// inspectable output of Classify. To == From for NoOp and Illegal.
type Outcome struct {
	Kind  TransitionKind
	From  string
	Event string
	To    string
}

// Cell is one entry of a lifecycle's decision table: the predicted Outcome of
// applying Event from state From.
type Cell struct {
	From  string
	Event string
	Kind  TransitionKind
	To    string
}

// Disposition tags a terminal state as a modeled outcome: a Success terminal is a
// clean completion; a Failure terminal is a modeled, acceptable failure (a timeout,
// cancellation, …). Non-terminal or untagged states are Unset. It lets a fault
// campaign judge an outcome from the model — reaching a Success terminal is
// recovery, a Failure terminal is (acceptable) degradation, and failing to settle
// at all is the bug — instead of hand-writing that verdict per test.
type Disposition int

const (
	Unset Disposition = iota
	Success
	Failure
)

func (d Disposition) String() string {
	switch d {
	case Success:
		return "Success"
	case Failure:
		return "Failure"
	default:
		return "Unset"
	}
}

// Trait is an arbitrary annotation attached to a state. Disposition and MustProgress
// are built-in traits; callers may attach a value of any type and read it back with
// StateTrait. Co-locating a state's traits keeps everything the model knows about it
// in one place, instead of scattered across parallel MustProgress/Dispositions lists.
type Trait = any

// Traits is the set of traits on a state.
type Traits []Trait

// States maps each state to its traits — the trait-based alternative to the
// MustProgress and Dispositions fields. When set, it is the source of truth for
// per-state metadata, and every state a transition references must be declared here.
type States map[string]Traits

// mustProgressTrait is the built-in marker trait exposed as MustProgress.
type mustProgressTrait struct{}

// MustProgress is the built-in marker trait: an entity must eventually leave a state
// carrying it (the generic EntityProgress liveness rule enforces this).
var MustProgress Trait = mustProgressTrait{}

// Capability is a class of drive power an edge may require to be realized. It is
// environment-independent knowledge about the edge (e.g. a timeout edge inherently
// needs timing/fault control); the concrete realization stays in the Driver, and a
// run enables only the capabilities its environment grants (see UMPIRE_SPEC.md).
type Capability int

const (
	// RPCDrive: realizable through the public frontend API / SDK. Runnable anywhere.
	RPCDrive Capability = iota + 1
	// Faults: needs fault/timing injection at internal seams. local-* only.
	Faults
	// DirectDrive: calls a CHASM transition directly, no wire. local-chasm only.
	DirectDrive
)

func (c Capability) String() string {
	switch c {
	case RPCDrive:
		return "RPCDrive"
	case Faults:
		return "Faults"
	case DirectDrive:
		return "DirectDrive"
	default:
		return "Capability(?)"
	}
}

// Hosting is the execution context a lifecycle instance runs in. Unlike a Capability set,
// it is single-valued: a drive environment realizes either Standalone entities (their own
// root execution) or Embedded ones (a child component of another execution, e.g. a
// workflow), not both at once. An edge restricted to one hosting carries a HostedIn trait.
type Hosting int

const (
	AnyHosting Hosting = iota // unrestricted (default)
	Standalone                // the entity is its own root execution
	Embedded                  // the entity is a child component of another execution
)

func (h Hosting) String() string {
	switch h {
	case Standalone:
		return "Standalone"
	case Embedded:
		return "Embedded"
	default:
		return "AnyHosting"
	}
}

// HostedIn is the built-in transition trait restricting an edge to a single Hosting (e.g.
// terminate is standalone-only). Construct it with RequiresHosting; read it back with
// EdgeHosting or EdgeTrait[HostedIn].
type HostedIn struct{ Hosting Hosting }

// RequiresHosting restricts an edge to the given hosting.
func RequiresHosting(h Hosting) HostedIn { return HostedIn{Hosting: h} }

// Requires is the built-in transition trait: the drive-capabilities an edge needs.
// Construct it with Needs; read it back with EdgeRequires or EdgeTrait[Requires].
type Requires struct{ Capabilities []Capability }

// Needs builds a Requires transition trait: Needs(Faults) marks an edge that can
// only be crossed where fault injection is available.
func Needs(caps ...Capability) Requires { return Requires{Capabilities: caps} }

// LifecycleSpec declares an entity's state machine. Terminal states are derived
// automatically (a state that is never the source of a transition), unless
// overridden via Terminal.
type LifecycleSpec struct {
	Initial     string
	Transitions []Transition
	Terminal    map[string]bool // optional override; nil = derive
	// States is the per-state metadata: each state's traits (see States, Trait).
	// The built-in MustProgress marker and the Success/Failure Dispositions are
	// traits; a state may also carry arbitrary domain traits read back with
	// StateTrait. When set, every state a transition references must be declared.
	States States
}

// IllegalTransition records a fact that attempted a transition that was not legal
// in the entity's current state — normally silently dropped, captured here so
// rules can flag it (e.g. an out-of-order or impossible state change).
type IllegalTransition struct {
	From  string
	Event string
	At    time.Time
}

// Lifecycle wraps a looplab FSM with the observability rules need: per-state
// entry timestamps, terminal-state knowledge, and a record of illegal
// transitions. It is a drop-in superset of the FSM methods entities use
// (Current/Can/Event/SetState) plus Fire and the accessors below.
//
// Entities should advance state with Fire (which records illegal attempts);
// Event/Can/SetState remain for compatibility and direct manipulation in tests.
type Lifecycle struct {
	fsm          *fsm.FSM
	initial      string
	states       []string                     // all declared states, stable sorted
	eventNames   []string                     // all declared event names, stable sorted
	edges        map[string]map[string]string // from -> event -> to (legal edges)
	edgeTraits   map[string]map[string]Traits // from -> event -> edge traits
	eventDests   map[string][]string          // event -> declared destination states
	canReach     map[string]map[string]bool   // transitive closure over legal edges (≥1 hop)
	terminal     map[string]bool
	mustProgress map[string]bool
	dispositions map[string]Disposition
	traits       map[string]Traits
	declared     bool // true when States was used (enables the declared-state check)
	entered      map[string]time.Time
	illegal      []IllegalTransition
	visited      map[string]struct{} // direct edges actually traversed (coverage)
}

// NewLifecycle builds a Lifecycle from a spec.
func NewLifecycle(spec LifecycleSpec) *Lifecycle {
	events := make(fsm.Events, 0, len(spec.Transitions))
	srcSeen := map[string]bool{}
	dstSeen := map[string]bool{}
	edges := map[string]map[string]string{}
	edgeTraits := map[string]map[string]Traits{}
	eventDestSet := map[string]map[string]bool{}
	stateSet := map[string]bool{spec.Initial: true}
	eventSet := map[string]bool{}
	for _, t := range spec.Transitions {
		events = append(events, fsm.EventDesc{Name: t.Event, Src: t.From, Dst: t.To})
		eventSet[t.Event] = true
		stateSet[t.To] = true
		dstSeen[t.To] = true
		if eventDestSet[t.Event] == nil {
			eventDestSet[t.Event] = map[string]bool{}
		}
		eventDestSet[t.Event][t.To] = true
		for _, s := range t.From {
			srcSeen[s] = true
			stateSet[s] = true
			if edges[s] == nil {
				edges[s] = map[string]string{}
			}
			edges[s][t.Event] = t.To
			if len(t.Traits) > 0 {
				if edgeTraits[s] == nil {
					edgeTraits[s] = map[string]Traits{}
				}
				edgeTraits[s][t.Event] = t.Traits
			}
		}
	}

	terminal := spec.Terminal
	if terminal == nil {
		terminal = map[string]bool{}
		// A reachable state that is never a transition source is terminal.
		for s := range dstSeen {
			if !srcSeen[s] {
				terminal[s] = true
			}
		}
	}

	// Per-state metadata is derived from the state traits.
	mustProgress := map[string]bool{}
	dispositions := map[string]Disposition{}
	traits := map[string]Traits{}
	for st, trs := range spec.States {
		stateSet[st] = true
		traits[st] = trs
		for _, tr := range trs {
			switch v := tr.(type) {
			case Disposition:
				dispositions[st] = v
			case mustProgressTrait:
				mustProgress[st] = true
			}
		}
	}

	eventDests := map[string][]string{}
	for e, set := range eventDestSet {
		eventDests[e] = sortedKeys(set)
	}

	return &Lifecycle{
		fsm:          fsm.NewFSM(spec.Initial, events, fsm.Callbacks{}),
		initial:      spec.Initial,
		states:       sortedKeys(stateSet),
		eventNames:   sortedKeys(eventSet),
		edges:        edges,
		edgeTraits:   edgeTraits,
		eventDests:   eventDests,
		canReach:     reachClosure(edges),
		terminal:     terminal,
		mustProgress: mustProgress,
		dispositions: dispositions,
		traits:       traits,
		declared:     spec.States != nil,
		entered:      map[string]time.Time{spec.Initial: time.Now()},
	}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// reachClosure returns, for each state, the set of states reachable from it by
// following one or more legal edges (self-loops excluded).
func reachClosure(edges map[string]map[string]string) map[string]map[string]bool {
	adj := map[string][]string{}
	for from, evs := range edges {
		for _, to := range evs {
			if to != from {
				adj[from] = append(adj[from], to)
			}
		}
	}
	reach := map[string]map[string]bool{}
	for from := range edges {
		seen := map[string]bool{}
		queue := append([]string{}, adj[from]...)
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			if seen[cur] {
				continue
			}
			seen[cur] = true
			queue = append(queue, adj[cur]...)
		}
		reach[from] = seen
	}
	return reach
}

// Classify predicts the Outcome of applying event from the current state, purely
// from the spec and the entity's observed history. It is the Lifecycle's
// executable transition function; Fire is defined in terms of it, and rules or
// tools can consult it directly. It never panics: every (state, event) pair maps
// to exactly one of Advance / NoOp / Illegal.
func (l *Lifecycle) Classify(event string) Outcome {
	return l.classifyFrom(l.fsm.Current(), event)
}

// classifyFrom is the pure core of Classify: it predicts the Outcome of event from
// an arbitrary state, reading only the static spec. Classify calls it with the
// current state; Cells calls it across the reachable set to build the decision
// table without mutating the FSM.
func (l *Lifecycle) classifyFrom(from, event string) Outcome {
	if to, ok := l.edges[from][event]; ok {
		kind := Advance
		if to == from {
			kind = NoOp // self-loop: legal, but no movement
		}
		return Outcome{Kind: kind, From: from, Event: event, To: to}
	}
	// No direct edge. Decide whether this is a benign re-observation, a forward
	// jump over unobserved states, or a genuinely illegal transition.
	if l.terminal[from] {
		// A terminal entity absorbs every further event as a stale no-op: once
		// closed, late/duplicate facts about it carry no new legal transition.
		return Outcome{Kind: NoOp, From: from, Event: event, To: from}
	}
	for _, d := range l.eventDests[event] {
		switch {
		case d == from || l.canReach[d][from]:
			// The event announces reaching d, but we are already at d or past it:
			// a duplicate / late / out-of-order fact consistent with our progress.
			return Outcome{Kind: NoOp, From: from, Event: event, To: from}
		case l.canReach[from][d]:
			// d lies ahead of us and is reachable: a forward jump over intermediate
			// states we did not observe. Observe-only cannot tell a missed
			// observation from an illegal skip, so this is treated as legal —
			// advance to the observed target so the model reflects it.
			return Outcome{Kind: Advance, From: from, Event: event, To: d}
		}
	}
	return Outcome{Kind: Illegal, From: from, Event: event, To: from}
}

// Fire applies event according to Classify. On Advance the machine moves and the
// destination's entry time is stamped (first entry wins), returning true. On NoOp
// (a benign duplicate/stale/out-of-order re-observation) nothing changes and it
// returns false. On Illegal the attempt is recorded as an IllegalTransition and it
// returns false. This replaces the guarded `if Can(x) { Event(x) }` pattern, which
// silently dropped both impossible transitions and benign re-observations alike.
func (l *Lifecycle) Fire(ctx context.Context, event string) bool {
	return l.FireAt(ctx, event, time.Now())
}

// FireAt applies event using the supplied event time for destination entry and
// illegal-transition evidence. A zero event time falls back to observation time.
func (l *Lifecycle) FireAt(ctx context.Context, event string, eventTime time.Time) bool {
	if eventTime.IsZero() {
		eventTime = time.Now()
	}
	switch o := l.Classify(event); o.Kind {
	case Advance:
		if _, isEdge := l.edges[o.From][event]; isEdge {
			_ = l.fsm.Event(ctx, event) // direct legal edge
			l.recordVisit(o.From, event, o.To)
		} else {
			l.fsm.SetState(o.To) // forward jump over unobserved intermediate states
		}
		l.stampEntryAt(eventTime)
		return true
	case NoOp:
		return false
	default: // Illegal
		l.illegal = append(l.illegal, IllegalTransition{From: o.From, Event: event, At: eventTime})
		return false
	}
}

func (l *Lifecycle) stampEntry() {
	l.stampEntryAt(time.Now())
}

func (l *Lifecycle) stampEntryAt(eventTime time.Time) {
	st := l.fsm.Current()
	if _, ok := l.entered[st]; !ok {
		l.entered[st] = eventTime
	}
}

// Current returns the current state.
func (l *Lifecycle) Current() string { return l.fsm.Current() }

// Initial returns the state the entity starts in — the root of the model graph a
// planner routes from.
func (l *Lifecycle) Initial() string { return l.initial }

// Can reports whether event is a legal transition from the current state.
func (l *Lifecycle) Can(event string) bool { return l.fsm.Can(event) }

// Event advances the machine (looplab-compatible); prefer Fire in entities.
func (l *Lifecycle) Event(ctx context.Context, event string, args ...any) error {
	err := l.fsm.Event(ctx, event, args...)
	if err == nil {
		l.stampEntry()
	}
	return err
}

// SetState forces the current state without a transition (used by tests).
func (l *Lifecycle) SetState(state string) {
	l.fsm.SetState(state)
	l.stampEntry()
}

// Reached reports whether the entity has ever been in state.
func (l *Lifecycle) Reached(state string) bool { _, ok := l.entered[state]; return ok }

// EnteredAt returns when the entity first entered state, and whether it did.
func (l *Lifecycle) EnteredAt(state string) (time.Time, bool) {
	t, ok := l.entered[state]
	return t, ok
}

// IsTerminal reports whether the current state is terminal.
func (l *Lifecycle) IsTerminal() bool { return l.terminal[l.fsm.Current()] }

// MustProgress reports whether the current state is one the entity is required
// to eventually leave (declared via LifecycleSpec.MustProgress).
func (l *Lifecycle) MustProgress() bool { return l.mustProgress[l.fsm.Current()] }

// StateMustProgress reports whether the given state is one the entity is required
// to eventually leave (declared via LifecycleSpec.MustProgress).
func (l *Lifecycle) StateMustProgress(state string) bool { return l.mustProgress[state] }

// Terminal reports whether the given state is terminal.
func (l *Lifecycle) Terminal(state string) bool { return l.terminal[state] }

// Disposition returns the modeled outcome tag of state (Unset if untagged).
func (l *Lifecycle) Disposition(state string) Disposition { return l.dispositions[state] }

// CurrentDisposition returns the disposition of the current state.
func (l *Lifecycle) CurrentDisposition() Disposition { return l.dispositions[l.fsm.Current()] }

// StateTraits returns the traits attached to state (nil if none / not trait-defined).
func (l *Lifecycle) StateTraits(state string) Traits { return l.traits[state] }

// StateTrait returns the first trait of type T attached to state, and whether one
// exists — the typed way to read an arbitrary state annotation, e.g.
// StateTrait[Disposition](lc, "succeeded") or StateTrait[MyTrait](lc, s).
func StateTrait[T any](l *Lifecycle, state string) (T, bool) {
	for _, tr := range l.traits[state] {
		if v, ok := tr.(T); ok {
			return v, true
		}
	}
	var zero T
	return zero, false
}

// EdgeTraits returns the traits on the from→event edge (nil if none).
func (l *Lifecycle) EdgeTraits(from, event string) Traits { return l.edgeTraits[from][event] }

// EdgeTrait returns the first trait of type T on the from→event edge — the typed way
// to read an edge annotation, e.g. EdgeTrait[Requires](lc, from, event).
func EdgeTrait[T any](l *Lifecycle, from, event string) (T, bool) {
	for _, tr := range l.edgeTraits[from][event] {
		if v, ok := tr.(T); ok {
			return v, true
		}
	}
	var zero T
	return zero, false
}

// EdgeRequires returns the drive-capabilities the from→event edge needs (nil if the
// edge declares none), read from its Needs/Requires trait.
// EdgeHosting returns the hosting an edge is restricted to, or AnyHosting if unrestricted.
func (l *Lifecycle) EdgeHosting(from, event string) Hosting {
	if h, ok := EdgeTrait[HostedIn](l, from, event); ok {
		return h.Hosting
	}
	return AnyHosting
}

func (l *Lifecycle) EdgeRequires(from, event string) []Capability {
	if r, ok := EdgeTrait[Requires](l, from, event); ok {
		return r.Capabilities
	}
	return nil
}

// Illegal returns the illegal transitions observed so far.
func (l *Lifecycle) Illegal() []IllegalTransition { return l.illegal }

// RecordIllegalAt retains domain-level branch evidence that cannot be represented
// as a legal transition from the lifecycle's current terminal state.
func (l *Lifecycle) RecordIllegalAt(event string, eventTime time.Time) {
	if eventTime.IsZero() {
		eventTime = time.Now()
	}
	l.illegal = append(l.illegal, IllegalTransition{From: l.Current(), Event: event, At: eventTime})
}

func (l *Lifecycle) recordVisit(from, event, to string) {
	if l.visited == nil {
		l.visited = map[string]struct{}{}
	}
	l.visited[from+"\x00"+event+"\x00"+to] = struct{}{}
}

// VisitedEdges returns the direct transition edges this entity actually traversed —
// the coverage this one instance contributes. Forward jumps over unobserved states
// are not counted (their skipped edges were never really traversed). Stable order.
func (l *Lifecycle) VisitedEdges() []Edge {
	out := make([]Edge, 0, len(l.visited))
	for k := range l.visited {
		p := strings.SplitN(k, "\x00", 3)
		out = append(out, Edge{From: p[0], Event: p[1], To: p[2]})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].Event < out[j].Event
	})
	return out
}

// States returns all states declared by the spec (including the initial state), sorted.
func (l *Lifecycle) States() []string { return append([]string(nil), l.states...) }

// Events returns all event names declared by the spec, sorted.
func (l *Lifecycle) Events() []string { return append([]string(nil), l.eventNames...) }

// Reachable returns the set of states reachable from the initial state by
// following legal edges. It is structural — independent of observed history — and
// is the coverage target for exploring or validating the lifecycle.
func (l *Lifecycle) Reachable() map[string]bool {
	out := map[string]bool{l.initial: true}
	for s := range l.canReach[l.initial] {
		out[s] = true
	}
	return out
}

// Edge is one direct transition of the model.
type Edge struct {
	From  string
	Event string
	To    string
}

// Edges returns every direct transition edge (from -> event -> to) declared by the
// spec, in stable order. These are the actual transitions — distinct from
// Classify's forward-jump interpretation — and are what a planner routes over.
// Destination returns the state an event leads to. Each event has a single destination in
// these models (its To is fixed regardless of From), so the lookup is unambiguous.
func (l *Lifecycle) Destination(event string) (string, bool) {
	for _, dst := range l.edges {
		if to, ok := dst[event]; ok {
			return to, true
		}
	}
	return "", false
}

func (l *Lifecycle) Edges() []Edge {
	froms := make([]string, 0, len(l.edges))
	for f := range l.edges {
		froms = append(froms, f)
	}
	sort.Strings(froms)
	var out []Edge
	for _, f := range froms {
		evs := make([]string, 0, len(l.edges[f]))
		for e := range l.edges[f] {
			evs = append(evs, e)
		}
		sort.Strings(evs)
		for _, e := range evs {
			out = append(out, Edge{From: f, Event: e, To: l.edges[f][e]})
		}
	}
	return out
}

// Cells returns the model's decision table over its reachable states: for every
// reachable state × declared event, the predicted Outcome. It is the coverage
// target (the denominator) for exploring the model and a readable, server-free
// description of how the entity behaves — computed from the spec alone, without
// touching the FSM's current state. States and events are in stable sorted order.
func (l *Lifecycle) Cells() []Cell {
	reachable := l.Reachable()
	froms := sortedKeys(reachable)
	out := make([]Cell, 0, len(froms)*len(l.eventNames))
	for _, from := range froms {
		for _, e := range l.eventNames {
			o := l.classifyFrom(from, e)
			out = append(out, Cell{From: from, Event: e, Kind: o.Kind, To: o.To})
		}
	}
	return out
}

// Validate is the Tier-1 static check on the spec: it needs no server and catches
// spec drift up front. It verifies the initial state is set, every declared state
// is reachable from it (no dead states), terminal states have no outgoing edges,
// and every MustProgress annotation is coherent (a declared, non-terminal state
// from which a terminal state is actually reachable — otherwise the generic
// EntityProgress liveness rule would either never fire or fire unsatisfiably).
// Classify's totality (every state × event yields a defined Outcome) holds by
// construction, so it needs no runtime assertion here.
func (l *Lifecycle) Validate() error {
	if l.initial == "" {
		return fmt.Errorf("lifecycle: empty initial state")
	}
	reachable := l.Reachable()
	stateSet := map[string]bool{}
	for _, s := range l.states {
		stateSet[s] = true
		if !reachable[s] {
			return fmt.Errorf("lifecycle: state %q is unreachable from initial %q", s, l.initial)
		}
		// When States is used it must declare every state the transitions reference,
		// so a typo in a transition is caught up front rather than modelled silently.
		if l.declared {
			if _, ok := l.traits[s]; !ok {
				return fmt.Errorf("lifecycle: state %q is used in a transition but not declared in States", s)
			}
		}
	}
	for s := range l.terminal {
		if len(l.edges[s]) > 0 {
			return fmt.Errorf("lifecycle: terminal state %q has outgoing transitions", s)
		}
	}
	for s := range l.mustProgress {
		switch {
		case !stateSet[s]:
			return fmt.Errorf("lifecycle: must-progress state %q is not a declared state", s)
		case l.terminal[s]:
			return fmt.Errorf("lifecycle: must-progress state %q is terminal (can never be left)", s)
		case !l.reachesTerminal(s):
			return fmt.Errorf("lifecycle: no terminal state is reachable from must-progress state %q", s)
		}
	}
	for s := range l.dispositions {
		switch {
		case !stateSet[s]:
			return fmt.Errorf("lifecycle: disposition state %q is not a declared state", s)
		case !l.terminal[s]:
			return fmt.Errorf("lifecycle: disposition state %q is not terminal (only terminals carry an outcome)", s)
		}
	}
	return nil
}

// reachesTerminal reports whether some terminal state is reachable from s by
// following legal edges — i.e. progress out of s is actually possible.
func (l *Lifecycle) reachesTerminal(s string) bool {
	for t := range l.canReach[s] {
		if l.terminal[t] {
			return true
		}
	}
	return false
}

// Lifecycled is implemented by entities backed by a Lifecycle, letting generic
// rules operate over any such entity regardless of its concrete type.
type Lifecycled interface {
	Entity
	Lifecycle() *Lifecycle
}

// LifecycleResult pairs a registry key with a Lifecycled entity.
type LifecycleResult struct {
	Key    string
	Entity Lifecycled
}

// ChangedLifecycles yields every entity implementing Lifecycled that changed
// since the rule's last check (respecting the rule's dirty-generation watermark
// and namespace scope). It is the type-erased counterpart of Changed[T], letting
// one rule judge many entity types by their shared lifecycle. Like Changed, it is
// a method on the embedded ruleContext, promoted to both rule contexts.
func (c *ruleContext) ChangedLifecycles() iter.Seq[LifecycleResult] {
	return func(yield func(LifecycleResult) bool) {
		for _, e := range c.ModelState.QueryAll(c.sinceGeneration, c.scope) {
			if c.Err() != nil {
				return
			}
			if lc, ok := e.Entity.(Lifecycled); ok {
				if !yield(LifecycleResult{Key: e.Key, Entity: lc}) {
					return
				}
			}
		}
	}
}
