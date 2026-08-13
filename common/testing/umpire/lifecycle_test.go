package umpire

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testSpec() LifecycleSpec {
	return LifecycleSpec{
		Initial: "unspecified",
		Transitions: []Transition{
			{Event: "admit", From: []string{"unspecified"}, To: "admitted"},
			{Event: "accept", From: []string{"admitted"}, To: "accepted"},
			{Event: "complete", From: []string{"admitted", "accepted"}, To: "completed"},
			{Event: "reject", From: []string{"unspecified", "admitted", "accepted"}, To: "rejected"},
			{Event: "abort", From: []string{"unspecified", "admitted", "accepted"}, To: "aborted"},
		},
	}
}

// branchSpec is a lifecycle with two independent branches, so it has a genuinely
// illegal transition: from "b" the event "toC" targets the sibling branch "c",
// which is neither reachable from nor able to reach "b". (testSpec, a converging
// DAG, has no such transition — every event's target is forward- or
// backward-reachable, so nothing in it is ever illegal.)
func branchSpec() LifecycleSpec {
	return LifecycleSpec{
		Initial: "a",
		Transitions: []Transition{
			{Event: "toB", From: []string{"a"}, To: "b"},
			{Event: "toC", From: []string{"a"}, To: "c"},
			{Event: "b2", From: []string{"b"}, To: "bEnd"},
			{Event: "c2", From: []string{"c"}, To: "cEnd"},
		},
	}
}

func TestLifecycle_DerivesTerminalStates(t *testing.T) {
	l := NewLifecycle(testSpec())
	require.True(t, l.Terminal("completed"))
	require.True(t, l.Terminal("rejected"))
	require.True(t, l.Terminal("aborted"))
	require.False(t, l.Terminal("unspecified"))
	require.False(t, l.Terminal("admitted"))
	require.False(t, l.Terminal("accepted"))
}

func TestLifecycle_FireLegalTransitionsStampsEntryAndTerminal(t *testing.T) {
	ctx := context.Background()
	l := NewLifecycle(testSpec())

	require.True(t, l.Fire(ctx, "admit"))
	require.True(t, l.Fire(ctx, "accept"))
	require.True(t, l.Fire(ctx, "complete"))

	require.Equal(t, "completed", l.Current())
	require.True(t, l.IsTerminal())
	require.Empty(t, l.Illegal())

	require.True(t, l.Reached("admitted"))
	require.True(t, l.Reached("accepted"))
	if _, ok := l.EnteredAt("accepted"); !ok {
		t.Fatal("expected an entry timestamp for accepted")
	}
	require.False(t, l.Reached("rejected"))
}

func TestLifecycle_FireIllegalTransitionIsRecordedNotApplied(t *testing.T) {
	ctx := context.Background()
	l := NewLifecycle(branchSpec())

	require.True(t, l.Fire(ctx, "toB")) // a -> b
	// "toC" from "b" targets the sibling branch "c": unreachable in either
	// direction, so it is a genuinely illegal transition, not a forward jump.
	require.False(t, l.Fire(ctx, "toC"))
	require.Equal(t, "b", l.Current(), "illegal transition must not change state")
	require.Len(t, l.Illegal(), 1)
	require.Equal(t, "b", l.Illegal()[0].From)
	require.Equal(t, "toC", l.Illegal()[0].Event)

	// A subsequent legal transition still works and is not recorded as illegal.
	require.True(t, l.Fire(ctx, "b2"))
	require.Len(t, l.Illegal(), 1)
}

func TestLifecycle_FireAtUsesEventTime(t *testing.T) {
	ctx := context.Background()
	l := NewLifecycle(branchSpec())
	enteredAt := time.Date(2026, time.August, 12, 10, 30, 0, 0, time.UTC)
	illegalAt := enteredAt.Add(time.Second)

	require.True(t, l.FireAt(ctx, "toB", enteredAt))
	require.False(t, l.FireAt(ctx, "toC", illegalAt))

	actualEnteredAt, ok := l.EnteredAt("b")
	require.True(t, ok)
	require.Equal(t, enteredAt, actualEnteredAt)
	require.Equal(t, illegalAt, l.Illegal()[0].At)
}

func TestLifecycle_FireAtPreservesFirstEntryAndDuplicateIsNoOp(t *testing.T) {
	ctx := context.Background()
	l := NewLifecycle(testSpec())
	first := time.Date(2026, time.August, 12, 10, 30, 0, 0, time.UTC)

	require.True(t, l.FireAt(ctx, "admit", first))
	require.False(t, l.FireAt(ctx, "admit", first.Add(time.Hour)))

	enteredAt, ok := l.EnteredAt("admitted")
	require.True(t, ok)
	require.Equal(t, first, enteredAt)
	require.Empty(t, l.Illegal())
}

func TestLifecycle_FireAtZeroTimeFallsBackToNow(t *testing.T) {
	ctx := context.Background()
	l := NewLifecycle(testSpec())
	before := time.Now()

	require.True(t, l.FireAt(ctx, "admit", time.Time{}))

	enteredAt, ok := l.EnteredAt("admitted")
	require.True(t, ok)
	require.False(t, enteredAt.Before(before))
	require.False(t, enteredAt.After(time.Now()))
}

func TestLifecycle_FireForwardJumpAdvancesToTarget(t *testing.T) {
	ctx := context.Background()
	l := NewLifecycle(testSpec())

	// "complete" observed while still "unspecified" (admit/accept were never
	// observed): a forward jump straight to the reachable target "completed".
	require.True(t, l.Fire(ctx, "complete"))
	require.Equal(t, "completed", l.Current())
	require.True(t, l.IsTerminal())
	require.Empty(t, l.Illegal(), "a forward jump is legal, not illegal")
	require.True(t, l.Reached("completed"))
	require.False(t, l.Reached("accepted"), "jumped-over states are not marked reached")
}

func TestLifecycle_ClassifyAdvanceForwardJumpIllegal(t *testing.T) {
	l := NewLifecycle(testSpec())

	// Advance: a legal forward edge.
	require.Equal(t, Outcome{Kind: Advance, From: "unspecified", Event: "admit", To: "admitted"}, l.Classify("admit"))
	// Forward jump: no direct edge, but "accepted" is reachable ahead (admit was
	// not observed) — legal, advancing to the observed target.
	require.Equal(t, Outcome{Kind: Advance, From: "unspecified", Event: "accept", To: "accepted"}, l.Classify("accept"))

	// Illegal: a transition into an unreachable sibling branch.
	b := NewLifecycle(branchSpec())
	b.SetState("b")
	require.Equal(t, Illegal, b.Classify("toC").Kind)
}

func TestLifecycle_ClassifyBenignReObservationsAreNoOp(t *testing.T) {
	ctx := context.Background()
	l := NewLifecycle(testSpec())
	require.True(t, l.Fire(ctx, "admit"))
	require.True(t, l.Fire(ctx, "accept")) // now in "accepted"

	// Duplicate: "accept" observed again while already accepted — its destination
	// equals the current state, so it is a benign no-op, not illegal.
	require.Equal(t, NoOp, l.Classify("accept").Kind)
	// Stale/out-of-order: "admit" observed again after we have progressed past
	// "admitted" — its destination lies behind us, so it too is a no-op.
	require.Equal(t, NoOp, l.Classify("admit").Kind)

	// Firing them records nothing and does not move the machine.
	require.False(t, l.Fire(ctx, "accept"))
	require.False(t, l.Fire(ctx, "admit"))
	require.Equal(t, "accepted", l.Current())
	require.Empty(t, l.Illegal())
}

func TestLifecycle_ClassifyTerminalAbsorbsEvents(t *testing.T) {
	ctx := context.Background()
	l := NewLifecycle(testSpec())
	require.True(t, l.Fire(ctx, "admit"))
	require.True(t, l.Fire(ctx, "complete")) // "admitted" -> "completed" (terminal)

	// Once terminal, any further event is a stale no-op, never illegal.
	for _, e := range l.Events() {
		require.Equalf(t, NoOp, l.Classify(e).Kind, "terminal state must absorb %q as NoOp", e)
	}
	require.False(t, l.Fire(ctx, "accept"))
	require.Empty(t, l.Illegal())
}

// Classify must be total: every (reachable state, event) yields exactly one of the
// three defined outcomes and never panics. This is the Tier-1 decision-coverage
// analog for the framework itself.
func TestLifecycle_ClassifyIsTotalOverReachableStates(t *testing.T) {
	spec := testSpec()
	for state := range NewLifecycle(spec).Reachable() {
		l := NewLifecycle(spec)
		l.SetState(state)
		for _, e := range l.Events() {
			o := l.Classify(e)
			require.Contains(t, []TransitionKind{Advance, NoOp, Illegal}, o.Kind)
		}
	}
}

func TestLifecycle_Cells(t *testing.T) {
	l := NewLifecycle(testSpec())
	cells := l.Cells()

	// One cell per (reachable state, event). 6 reachable states × 5 events.
	require.Len(t, cells, len(l.Reachable())*len(l.Events()))

	// Spot-check a few decisive entries.
	get := func(from, event string) Cell {
		for _, c := range cells {
			if c.From == from && c.Event == event {
				return c
			}
		}
		t.Fatalf("no cell for (%s, %s)", from, event)
		return Cell{}
	}
	require.Equal(t, Cell{From: "unspecified", Event: "admit", Kind: Advance, To: "admitted"}, get("unspecified", "admit"))
	require.Equal(t, Cell{From: "unspecified", Event: "accept", Kind: Advance, To: "accepted"}, get("unspecified", "accept")) // forward jump
	require.Equal(t, NoOp, get("completed", "accept").Kind)                                                                   // terminal absorbs
	require.Equal(t, NoOp, get("accepted", "admit").Kind)                                                                     // stale, behind current

	// Every declared event must be an Advance from at least one reachable state —
	// i.e. the model has no dead events.
	advanced := map[string]bool{}
	for _, c := range cells {
		if c.Kind == Advance {
			advanced[c.Event] = true
		}
	}
	for _, e := range l.Events() {
		require.Truef(t, advanced[e], "event %q is never a live transition (dead event)", e)
	}
}

func TestLifecycle_Reachable(t *testing.T) {
	l := NewLifecycle(testSpec())
	require.Equal(t, map[string]bool{
		"unspecified": true, "admitted": true, "accepted": true,
		"completed": true, "rejected": true, "aborted": true,
	}, l.Reachable())
}

func TestLifecycle_Validate(t *testing.T) {
	require.NoError(t, NewLifecycle(testSpec()).Validate())

	// A dead state (unreachable from initial) is rejected.
	require.Error(t, NewLifecycle(LifecycleSpec{
		Initial:     "a",
		Transitions: []Transition{{Event: "x", From: []string{"a"}, To: "b"}, {Event: "y", From: []string{"orphan"}, To: "c"}},
	}).Validate())

	// A terminal state with an outgoing edge is inconsistent.
	require.Error(t, NewLifecycle(LifecycleSpec{
		Initial:     "a",
		Transitions: []Transition{{Event: "x", From: []string{"a"}, To: "b"}},
		Terminal:    map[string]bool{"a": true},
	}).Validate())

	// A well-formed spec with a coherent MustProgress trait is valid.
	require.NoError(t, NewLifecycle(LifecycleSpec{
		Initial:     "a",
		Transitions: []Transition{{Event: "x", From: []string{"a"}, To: "b"}},
		States:      States{"a": {MustProgress}, "b": {}},
	}).Validate())

	// A transition referencing a state not declared in States is rejected.
	require.Error(t, NewLifecycle(LifecycleSpec{
		Initial:     "a",
		Transitions: []Transition{{Event: "x", From: []string{"a"}, To: "b"}},
		States:      States{"a": {MustProgress}}, // "b" is undeclared
	}).Validate())

	// MustProgress on a terminal state (can never be left) is rejected.
	require.Error(t, NewLifecycle(LifecycleSpec{
		Initial:     "a",
		Transitions: []Transition{{Event: "x", From: []string{"a"}, To: "b"}},
		States:      States{"a": {}, "b": {MustProgress}},
	}).Validate())

	// MustProgress on a state with no path to any terminal (a pure cycle) is rejected.
	require.Error(t, NewLifecycle(LifecycleSpec{
		Initial: "a",
		Transitions: []Transition{
			{Event: "x", From: []string{"a"}, To: "b"},
			{Event: "y", From: []string{"b"}, To: "a"},
		},
		States: States{"a": {MustProgress}, "b": {}},
	}).Validate())

	// A Disposition trait on a non-terminal state is rejected.
	require.Error(t, NewLifecycle(LifecycleSpec{
		Initial:     "a",
		Transitions: []Transition{{Event: "x", From: []string{"a"}, To: "b"}},
		States:      States{"a": {Success}, "b": {}},
	}).Validate())
}

func TestLifecycle_EdgeTraits(t *testing.T) {
	type risk struct{ level int } // an arbitrary domain edge trait
	lc := NewLifecycle(LifecycleSpec{
		Initial: "a",
		States:  States{"a": {}, "b": {}, "c": {}},
		Transitions: []Transition{
			{Event: "go", From: []string{"a"}, To: "b", Traits: Traits{Needs(RPCDrive)}},
			{Event: "boom", From: []string{"a", "b"}, To: "c", Traits: Traits{Needs(Faults), risk{level: 3}}},
			{Event: "plain", From: []string{"b"}, To: "c"}, // no traits
		},
	})
	require.NoError(t, lc.Validate())

	// Built-in capability trait, read via the convenience accessor.
	require.Equal(t, []Capability{RPCDrive}, lc.EdgeRequires("a", "go"))
	require.Equal(t, []Capability{Faults}, lc.EdgeRequires("a", "boom"))
	require.Equal(t, []Capability{Faults}, lc.EdgeRequires("b", "boom")) // applies to every From
	require.Nil(t, lc.EdgeRequires("b", "plain"))

	// Arbitrary edge trait, read generically by type.
	r, ok := EdgeTrait[risk](lc, "a", "boom")
	require.True(t, ok)
	require.Equal(t, 3, r.level)
	_, ok = EdgeTrait[risk](lc, "a", "go")
	require.False(t, ok)
}
