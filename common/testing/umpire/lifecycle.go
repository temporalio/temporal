package umpire

import (
	"context"
	"iter"
	"time"

	"github.com/looplab/fsm"
)

// Transition is one edge of a Lifecycle's state machine: firing Event moves the
// entity from any of the From states to To.
type Transition struct {
	Event string
	From  []string
	To    string
}

// LifecycleSpec declares an entity's state machine. Terminal states are derived
// automatically (a state that is never the source of a transition), unless
// overridden via Terminal.
type LifecycleSpec struct {
	Initial     string
	Transitions []Transition
	Terminal    map[string]bool // optional override; nil = derive
	// MustProgress lists non-terminal states the entity must eventually leave.
	// The generic EntityProgress liveness rule flags an entity left in one of
	// these at teardown. States not listed (e.g. an initial "not yet started"
	// state) are treated as acceptable resting points.
	MustProgress []string
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
	terminal     map[string]bool
	mustProgress map[string]bool
	entered      map[string]time.Time
	illegal      []IllegalTransition
}

// NewLifecycle builds a Lifecycle from a spec.
func NewLifecycle(spec LifecycleSpec) *Lifecycle {
	events := make(fsm.Events, 0, len(spec.Transitions))
	srcSeen := map[string]bool{}
	dstSeen := map[string]bool{}
	for _, t := range spec.Transitions {
		events = append(events, fsm.EventDesc{Name: t.Event, Src: t.From, Dst: t.To})
		for _, s := range t.From {
			srcSeen[s] = true
		}
		dstSeen[t.To] = true
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

	mustProgress := make(map[string]bool, len(spec.MustProgress))
	for _, s := range spec.MustProgress {
		mustProgress[s] = true
	}

	return &Lifecycle{
		fsm:          fsm.NewFSM(spec.Initial, events, fsm.Callbacks{}),
		terminal:     terminal,
		mustProgress: mustProgress,
		entered:      map[string]time.Time{spec.Initial: time.Now()},
	}
}

// Fire attempts the transition for event. If it is legal, the machine advances
// and the destination's entry time is stamped (first entry wins); it returns
// true. If it is not legal, the attempt is recorded as an IllegalTransition and
// it returns false. This replaces the guarded `if Can(x) { Event(x) }` pattern,
// which silently drops impossible transitions.
func (l *Lifecycle) Fire(ctx context.Context, event string) bool {
	if !l.fsm.Can(event) {
		l.illegal = append(l.illegal, IllegalTransition{From: l.fsm.Current(), Event: event, At: time.Now()})
		return false
	}
	_ = l.fsm.Event(ctx, event)
	l.stampEntry()
	return true
}

func (l *Lifecycle) stampEntry() {
	st := l.fsm.Current()
	if _, ok := l.entered[st]; !ok {
		l.entered[st] = time.Now()
	}
}

// Current returns the current state.
func (l *Lifecycle) Current() string { return l.fsm.Current() }

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

// Terminal reports whether the given state is terminal.
func (l *Lifecycle) Terminal(state string) bool { return l.terminal[state] }

// Illegal returns the illegal transitions observed so far.
func (l *Lifecycle) Illegal() []IllegalTransition { return l.illegal }

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
// and namespace scope). It is the type-erased counterpart of ChangedEntities[T],
// letting one rule judge many entity types by their shared lifecycle.
func ChangedLifecycles(c dirtyQuerier) iter.Seq[LifecycleResult] {
	ctx, reg, since, scope := c.dirtyQuery()
	return func(yield func(LifecycleResult) bool) {
		for _, e := range reg.QueryAll(since, scope) {
			if ctx.Err() != nil {
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
