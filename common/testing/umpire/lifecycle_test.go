package umpire

import (
	"context"
	"testing"

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
	l := NewLifecycle(testSpec())

	// "accept" is illegal from the initial "unspecified" state.
	require.False(t, l.Fire(ctx, "accept"))
	require.Equal(t, "unspecified", l.Current(), "illegal transition must not change state")
	require.Len(t, l.Illegal(), 1)
	require.Equal(t, "unspecified", l.Illegal()[0].From)
	require.Equal(t, "accept", l.Illegal()[0].Event)

	// A subsequent legal transition still works and is not recorded as illegal.
	require.True(t, l.Fire(ctx, "admit"))
	require.Len(t, l.Illegal(), 1)
}

func TestLifecycle_ClassifyAdvanceNoOpIllegal(t *testing.T) {
	l := NewLifecycle(testSpec())

	// Advance: a legal forward edge.
	require.Equal(t, Outcome{Kind: Advance, From: "unspecified", Event: "admit", To: "admitted"}, l.Classify("admit"))
	// Illegal: an event whose destination lies neither ahead via an edge nor behind us.
	require.Equal(t, Illegal, l.Classify("accept").Kind)
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
}
