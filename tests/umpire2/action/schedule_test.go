package action_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/action"
)

// TestScheduleFaults pins the scheduler's contract: it schedules each distinct fault target once
// (breadth) before any repeat, caps the drive count at the budget, and reports the overflow as
// dropped rather than silently truncating. Entry and ambient calls contribute no targets.
func TestScheduleFaults(t *testing.T) {
	p1 := action.PlanFootprint{
		Plan:    []umpire.Action{{Name: "p1", Entry: []string{"Entry"}}},
		Label:   "p1",
		Learned: []string{"/svc/A", "/svc/B", "/svc/Entry", "/svc/PollWorkflowTaskQueue"},
	}
	p2 := action.PlanFootprint{
		Plan:    []umpire.Action{{Name: "p2", Entry: []string{"Entry"}}},
		Label:   "p2",
		Learned: []string{"/svc/B", "/svc/C"},
	}
	// Distinct targets (entry + ambient excluded): A, B, C. Candidate (plan,target) pairs:
	// p1:A, p1:B, p2:B, p2:C — four, of which p2:B is the repeat of B.

	drives, dropped := action.ScheduleFaults([]action.PlanFootprint{p1, p2}, 3)
	require.Len(t, drives, 3, "budget caps the drive count")
	require.Len(t, dropped, 1, "the fourth candidate overflows the budget")

	targets := []string{drives[0].Target, drives[1].Target, drives[2].Target}
	require.ElementsMatch(t, []string{"/svc/A", "/svc/B", "/svc/C"}, targets,
		"each distinct target is scheduled once before any repeat")
	require.Contains(t, dropped[0].Target, "B", "the dropped candidate is the repeat of B")

	for _, d := range drives {
		require.Equal(t, umpire.Fault, d.Plan[0].Kind, "each drive prepends the target's Drop")
	}

	_, none := action.ScheduleFaults([]action.PlanFootprint{p1, p2}, 10)
	require.Empty(t, none, "a budget at or above the candidate count drops nothing")
}
