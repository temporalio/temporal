package planner_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire1/planner"
)

type recordingDriver struct{ events []string }

func (d *recordingDriver) Do(_ context.Context, a planner.Step) error {
	d.events = append(d.events, a.Event)
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

func capLifecycle() *umpire.Lifecycle {
	return umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: "start",
		States:  umpire.States{"start": {}, "done": {}, "expired": {}},
		Transitions: []umpire.Transition{
			{Event: "run", From: []string{"start"}, To: "done", Traits: umpire.Traits{umpire.Needs(umpire.RPCDrive)}},
			{Event: "expire", From: []string{"start"}, To: "expired", Traits: umpire.Traits{umpire.Needs(umpire.Faults)}},
		},
	})
}

func TestPlanTo_CapabilityFiltering(t *testing.T) {
	lc := capLifecycle()

	// No grant set: capability filtering is off — every edge is available.
	_, err := planner.PlanTo(lc, "expired", planner.Shortest, planner.Constraints{})
	require.NoError(t, err)

	// Granting only RPCDrive: "done" is reachable.
	plan, err := planner.PlanTo(lc, "done", planner.Shortest,
		planner.Constraints{Grants: []umpire.Capability{umpire.RPCDrive}})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"run"}}, plan.Routes)

	// "expired" needs Faults, so it fails fast with a named-capability error.
	_, err = planner.PlanTo(lc, "expired", planner.Shortest,
		planner.Constraints{Grants: []umpire.Capability{umpire.RPCDrive}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Faults")

	// Granting Faults too makes it reachable.
	plan, err = planner.PlanTo(lc, "expired", planner.Shortest,
		planner.Constraints{Grants: []umpire.Capability{umpire.RPCDrive, umpire.Faults}})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"expire"}}, plan.Routes)
}

// TestPlanTo_HostingFiltering shows the Hosting dimension on the real NexusOperation model:
// the terminated terminal is reachable only when the run drives Standalone operations —
// an Embedded (workflow-child) run cannot reach it, with a named hosting shortfall.
func TestPlanTo_HostingFiltering(t *testing.T) {
	models := planner.DefaultModels()
	powers := []umpire.Capability{umpire.RPCDrive, umpire.Faults}

	// Embedded run: terminate is Standalone-only, so terminated is unreachable — and the
	// shortfall is named, not silent.
	_, err := models.PlanTo("NexusOperation", "terminated", planner.Shortest,
		planner.Constraints{Grants: powers, Hosting: umpire.Embedded})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Standalone hosting")

	// Standalone run: terminated is reachable.
	plan, err := models.PlanTo("NexusOperation", "terminated", planner.Shortest,
		planner.Constraints{Grants: powers, Hosting: umpire.Standalone})
	require.NoError(t, err)
	require.NotEmpty(t, plan.Routes)

	// A non-hosting-restricted terminal (succeeded) is reachable under either hosting.
	for _, h := range []umpire.Hosting{umpire.Embedded, umpire.Standalone} {
		_, err := models.PlanTo("NexusOperation", "succeeded", planner.Shortest,
			planner.Constraints{Grants: powers, Hosting: h})
		require.NoErrorf(t, err, "succeeded should be reachable under %s", h)
	}
}
