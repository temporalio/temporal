package planner

// This file preserves the disabled Workflow Update support for future work.
// All archived source is commented out intentionally.

// tests/umpire1/planner/models.go
// // Models is the authoring surface: a catalog of every entity model the planner can
// // plan over, keyed by entity type. A developer names targets fully-qualified by
// // entity ("WorkflowUpdate", "completed") instead of first fetching a Lifecycle by
// // hand — which also disambiguates states that several entities share (both
// // Workflow and WorkflowUpdate have a "completed" state).
//
// tests/umpire1/planner/planner_test.go
// package planner_test
//
// import (
// 	"context"
// 	"testing"
//
// 	"github.com/stretchr/testify/require"
// 	"go.temporal.io/server/tests/umpire1/model"
// 	"go.temporal.io/server/tests/umpire1/planner"
// )
//
// // recordingDriver is a fake Driver: instead of sending RPCs it records the events
// // it was asked to realize, so tests can assert what a Plan drives. A real driver
// // would map each event onto Temporal traffic.
// type recordingDriver struct{ events []string }
//
// func (d *recordingDriver) Do(_ context.Context, event string) error {
// 	d.events = append(d.events, event)
// 	return nil
// }
//
// // A fixed regression test: describe the target state; the planner finds the route.
// func TestPlanTo_ShortestRouteIsDeterministic(t *testing.T) {
// 	lc := model.NewWorkflowUpdate().Lifecycle()
//
// 	plan, err := planner.PlanTo(lc, "completed", planner.Shortest, planner.Constraints{})
// 	require.NoError(t, err)
// 	require.True(t, plan.Reaches("completed"))
// 	require.Equal(t, [][]string{{"admit", "complete"}}, plan.Routes)
// }
//
// // AllRoutes exercises every distinct way to reach the target — the "exercise all
// // routes" mode for route-dependent regressions.
// func TestPlanTo_AllRoutes(t *testing.T) {
// 	lc := model.NewWorkflowUpdate().Lifecycle()
//
// 	plan, err := planner.PlanTo(lc, "completed", planner.AllRoutes, planner.Constraints{})
// 	require.NoError(t, err)
// 	require.ElementsMatch(t, [][]string{
// 		{"admit", "complete"},           // accept+complete in one WFT
// 		{"admit", "accept", "complete"}, // separate accept then complete
// 	}, plan.Routes)
// }
//
// // Random picks one route, reproducibly via the seed.
// func TestPlanTo_RandomIsSeeded(t *testing.T) {
// 	lc := model.NewWorkflowUpdate().Lifecycle()
//
// 	a, err := planner.PlanTo(lc, "completed", planner.Random, planner.Constraints{}, planner.WithSeed(42))
// 	require.NoError(t, err)
// 	b, err := planner.PlanTo(lc, "completed", planner.Random, planner.Constraints{}, planner.WithSeed(42))
// 	require.NoError(t, err)
// 	require.Equal(t, a.Routes, b.Routes, "same seed must yield the same route")
// }
//
// // Planning happens before running: an unreachable target fails fast, with no
// // driving. This doubles as a negative/reachability assertion.
// func TestPlanTo_UnreachableFailsFast(t *testing.T) {
// 	lc := model.NewWorkflowUpdate().Lifecycle()
//
// 	// "accepted" is reachable in general, but not if "accept" is forbidden.
// 	_, err := planner.PlanTo(lc, "accepted", planner.Shortest,
// 		planner.Constraints{DenyEvents: []string{"accept"}})
// 	require.Error(t, err)
//
// 	// A state the model simply does not have is rejected outright.
// 	_, err = planner.PlanTo(lc, "no_such_state", planner.Shortest, planner.Constraints{})
// 	require.Error(t, err)
// }
//
// // Constraints shape the route: forbid the direct accept+complete edge and the plan
// // must go the long way through "accepted".
// func TestPlanTo_ConstraintsShapeTheRoute(t *testing.T) {
// 	lc := model.NewWorkflowUpdate().Lifecycle()
//
// 	plan, err := planner.PlanTo(lc, "completed", planner.Shortest,
// 		planner.Constraints{DenyStates: []string{"rejected", "aborted"}})
// 	require.NoError(t, err)
// 	require.NotContains(t, flatten(plan.Routes), "reject")
// 	require.NotContains(t, flatten(plan.Routes), "abort")
// }
//
// // Run drives the planned route through an Driver, in order.
// func TestPlan_RunDrivesTheRoute(t *testing.T) {
// 	lc := model.NewWorkflowUpdate().Lifecycle()
// 	plan, err := planner.PlanTo(lc, "completed", planner.Shortest, planner.Constraints{})
// 	require.NoError(t, err)
//
// 	d := &recordingDriver{}
// 	require.NoError(t, plan.Run(context.Background(), d))
// 	require.Equal(t, []string{"admit", "complete"}, d.events)
// }
//
// // Exploration walks a constrained sub-graph, reproducibly, staying inside the
// // constraints by construction.
// func TestExplore_StaysWithinConstraints(t *testing.T) {
// 	lc := model.NewWorkflowUpdate().Lifecycle()
//
// 	plan := planner.Explore(lc,
// 		planner.Constraints{DenyEvents: []string{"reject", "abort"}, MaxDepth: 5},
// 		planner.WithSeed(1))
//
// 	require.NotEmpty(t, plan.Routes[0])
// 	for _, ev := range plan.Routes[0] {
// 		require.NotContains(t, []string{"reject", "abort"}, ev)
// 	}
// }
//
// // The catalog is the ergonomic surface: no hand-fetched Lifecycle, and states are
// // named fully-qualified by entity.
// func TestModels_PlanByQualifiedState(t *testing.T) {
// 	models := planner.DefaultModels()
//
// 	plan, err := models.PlanTo("WorkflowUpdate", "completed", planner.Shortest, planner.Constraints{})
// 	require.NoError(t, err)
// 	require.Equal(t, [][]string{{"admit", "complete"}}, plan.Routes)
// }
//
// // "completed" is ambiguous across entities — qualifying by entity disambiguates it.
// // Workflow reaches completed via start->complete; WorkflowUpdate via admit->complete.
// func TestModels_QualifiedStateDisambiguates(t *testing.T) {
// 	models := planner.DefaultModels()
//
// 	wf, err := models.PlanTo("Workflow", "completed", planner.Shortest, planner.Constraints{})
// 	require.NoError(t, err)
// 	require.Equal(t, [][]string{{"start", "complete"}}, wf.Routes)
//
// 	upd, err := models.PlanTo("WorkflowUpdate", "completed", planner.Shortest, planner.Constraints{})
// 	require.NoError(t, err)
// 	require.Equal(t, [][]string{{"admit", "complete"}}, upd.Routes)
// }
//
// func TestModels_UnknownEntity(t *testing.T) {
// 	_, err := planner.DefaultModels().PlanTo("Nope", "completed", planner.Shortest, planner.Constraints{})
// 	require.Error(t, err)
// }
//
// func flatten(routes [][]string) []string {
// 	var out []string
// 	for _, r := range routes {
// 		out = append(out, r...)
// 	}
// 	return out
// }
