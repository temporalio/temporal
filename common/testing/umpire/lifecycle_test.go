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
