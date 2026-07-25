package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	umpire "go.temporal.io/server/common/testing/umpire"
)

// Tier-1 (server-free): the standalone-activity lifecycle is sound, and ActivityTransition
// is total over the whole (state × event) grid, agrees with Classify on the state part, and
// predicts a rejection exactly on illegal edges — SAA's static model validation.
func TestActivityLifecycle_ValidAndTotal(t *testing.T) {
	require.NoError(t, NewActivityLifecycle().Validate())
}

func TestActivityTransition_TotalAndConsistentWithLifecycle(t *testing.T) {
	lc := NewActivityLifecycle()
	cfg := ActivityConfig{} // unlimited attempts → no config-dependent branch
	for _, st := range lc.States() {
		for _, ev := range lc.Events() {
			out := ActivityTransition(cfg, ActivityAbstract{State: st, Attempt: 1}, ev)

			lc.SetState(st)
			base := lc.Classify(ev)
			require.Equal(t, base.Kind, out.Kind, "kind at %s/%s", st, ev)
			if out.Kind == umpire.Advance {
				require.Equal(t, base.To, out.Next.State, "advance target at %s/%s", st, ev)
			}
			if out.Kind == umpire.Illegal {
				require.NotEmpty(t, out.Reject, "illegal edge must reject at %s/%s", st, ev)
			} else {
				require.Empty(t, out.Reject, "legal edge must not reject at %s/%s", st, ev)
			}
		}
	}
}

func TestActivityTransition_RetryBudgetExhaustedFailsTerminally(t *testing.T) {
	out := ActivityTransition(ActivityConfig{MaxAttempts: 1},
		ActivityAbstract{State: ActivityStarted, Attempt: 1}, ActivityAttemptFailed)
	require.Equal(t, umpire.Advance, out.Kind)
	require.Equal(t, ActivityFailed, out.Next.State)
	require.True(t, out.Terminal)
	require.False(t, out.BackoffArmed)
}

func TestActivityTransition_RetryBudgetRemainingBacksOff(t *testing.T) {
	out := ActivityTransition(ActivityConfig{MaxAttempts: 3},
		ActivityAbstract{State: ActivityStarted, Attempt: 1}, ActivityAttemptFailed)
	require.Equal(t, ActivityBackingOff, out.Next.State)
	require.True(t, out.BackoffArmed)
	require.False(t, out.Terminal)
}

func TestActivityTransition_RescheduleIncrementsAttempt(t *testing.T) {
	out := ActivityTransition(ActivityConfig{},
		ActivityAbstract{State: ActivityBackingOff, Attempt: 1}, ActivitySchedule)
	require.Equal(t, ActivityScheduled, out.Next.State)
	require.Equal(t, 2, out.Next.Attempt)
	require.Equal(t, 1, out.AttemptDelta)
}

// A mini conformance walk through a retry-then-complete route, asserting the abstract state
// (including attempt count) evolves as predicted at every edge.
func TestActivityTransition_RetryThenCompleteWalk(t *testing.T) {
	cfg := ActivityConfig{MaxAttempts: 3}
	cur := ActivityAbstract{State: ActivityUnspecified}
	for _, ev := range []string{
		ActivitySchedule, ActivityStart, ActivityAttemptFailed, ActivitySchedule, ActivityStart, ActivityComplete,
	} {
		out := ActivityTransition(cfg, cur, ev)
		require.NotEqual(t, umpire.Illegal, out.Kind, "event %q unexpectedly illegal at %s", ev, cur.State)
		cur = out.Next
	}
	require.Equal(t, ActivityCompleted, cur.State)
	require.Equal(t, 2, cur.Attempt, "two schedules → two attempts")
}
