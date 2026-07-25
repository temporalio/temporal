package planner_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/planner"
)

type recordingDriver struct{ events []string }

func (d *recordingDriver) Do(_ context.Context, event string) error {
	d.events = append(d.events, event)
	return nil
}

func plannerLifecycle() *umpire.Lifecycle {
	return umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: "created",
		Transitions: []umpire.Transition{
			{Event: "initialize", From: []string{"created"}, To: "ready"},
			{Event: "advance", From: []string{"ready"}, To: "running"},
			{Event: "finish", From: []string{"ready", "running"}, To: "completed"},
			{Event: "reject", From: []string{"ready", "running"}, To: "rejected"},
			{Event: "abort", From: []string{"ready", "running"}, To: "aborted"},
		},
	})
}

func TestPlanTo_ShortestRouteIsDeterministic(t *testing.T) {
	plan, err := planner.PlanTo(plannerLifecycle(), "completed", planner.Shortest, planner.Constraints{})
	require.NoError(t, err)
	require.True(t, plan.Reaches("completed"))
	require.Equal(t, [][]string{{"initialize", "finish"}}, plan.Routes)
}

func TestPlanTo_AllRoutes(t *testing.T) {
	plan, err := planner.PlanTo(plannerLifecycle(), "completed", planner.AllRoutes, planner.Constraints{})
	require.NoError(t, err)
	require.ElementsMatch(t, [][]string{
		{"initialize", "finish"},
		{"initialize", "advance", "finish"},
	}, plan.Routes)
}

func TestPlanTo_RandomIsSeeded(t *testing.T) {
	a, err := planner.PlanTo(plannerLifecycle(), "completed", planner.Random, planner.Constraints{}, planner.WithSeed(42))
	require.NoError(t, err)
	b, err := planner.PlanTo(plannerLifecycle(), "completed", planner.Random, planner.Constraints{}, planner.WithSeed(42))
	require.NoError(t, err)
	require.Equal(t, a.Routes, b.Routes, "same seed must yield the same route")
}

func TestPlanTo_UnreachableFailsFast(t *testing.T) {
	_, err := planner.PlanTo(plannerLifecycle(), "running", planner.Shortest,
		planner.Constraints{DenyEvents: []string{"advance"}})
	require.Error(t, err)

	_, err = planner.PlanTo(plannerLifecycle(), "no_such_state", planner.Shortest, planner.Constraints{})
	require.Error(t, err)
}

func TestPlanTo_ConstraintsShapeTheRoute(t *testing.T) {
	plan, err := planner.PlanTo(plannerLifecycle(), "completed", planner.Shortest,
		planner.Constraints{DenyStates: []string{"rejected", "aborted"}})
	require.NoError(t, err)
	require.NotContains(t, flatten(plan.Routes), "reject")
	require.NotContains(t, flatten(plan.Routes), "abort")
}

func TestPlan_RunDrivesTheRoute(t *testing.T) {
	plan, err := planner.PlanTo(plannerLifecycle(), "completed", planner.Shortest, planner.Constraints{})
	require.NoError(t, err)

	d := &recordingDriver{}
	require.NoError(t, plan.Run(context.Background(), d))
	require.Equal(t, []string{"initialize", "finish"}, d.events)
}

func TestExplore_StaysWithinConstraints(t *testing.T) {
	plan := planner.Explore(plannerLifecycle(),
		planner.Constraints{DenyEvents: []string{"reject", "abort"}, MaxDepth: 5},
		planner.WithSeed(1))

	require.NotEmpty(t, plan.Routes[0])
	for _, ev := range plan.Routes[0] {
		require.NotContains(t, []string{"reject", "abort"}, ev)
	}
}

func TestModels_PlanByQualifiedState(t *testing.T) {
	plan, err := planner.DefaultModels().PlanTo("Workflow", "completed", planner.Shortest, planner.Constraints{})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"start", "complete"}}, plan.Routes)
}

func TestModels_UnknownEntity(t *testing.T) {
	_, err := planner.DefaultModels().PlanTo("Nope", "completed", planner.Shortest, planner.Constraints{})
	require.Error(t, err)
}

func flatten(routes [][]string) []string {
	var out []string
	for _, r := range routes {
		out = append(out, r...)
	}
	return out
}
