package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpire/fact"
	"go.temporal.io/server/tests/umpire/model"
	"go.temporal.io/server/tests/umpire/planner"
)

// UmpireTestSuite is an end-to-end test of both halves of the umpire together:
// the Driver plans a route over an entity model, an Actuator realizes that route
// as real traffic against a live cluster, and the Monitor (wired into every
// functional cluster) judges the result. It is the first test that exercises
// plan -> drive -> judge against a real server.
type UmpireTestSuite struct {
	parallelsuite.Suite[*UmpireTestSuite]
}

func TestUmpireTestSuite(t *testing.T) {
	parallelsuite.Run(t, &UmpireTestSuite{})
}

// signalThenComplete blocks on a "finish" signal, then returns — so the workflow's
// start and its completion are two separately drivable steps, matching the
// Workflow model's "start" and "complete" events.
func signalThenComplete(ctx workflow.Context) error {
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	return nil
}

// workflowActuator realizes the Workflow model's abstract events as real SDK calls.
// It holds the run handle between steps, since a route drives its events in order.
type workflowActuator struct {
	env        *testcore.TestEnv
	workflowID string
	run        sdkclient.WorkflowRun
}

func (a *workflowActuator) Do(ctx context.Context, event string) error {
	switch event {
	case "start":
		run, err := a.env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        a.workflowID,
			TaskQueue: a.env.WorkerTaskQueue(),
		}, signalThenComplete)
		if err != nil {
			return err
		}
		a.run = run
		return nil
	case "complete":
		if err := a.env.SdkClient().SignalWorkflow(ctx, a.workflowID, "", "finish", nil); err != nil {
			return err
		}
		return a.run.Get(ctx, nil) // block until the workflow actually completes
	default:
		return fmt.Errorf("workflowActuator: unhandled event %q", event)
	}
}

func (s *UmpireTestSuite) TestPlanAndDriveWorkflowToCompletion() {
	t := s.T()
	env := testcore.NewEnv(t)
	env.SdkWorker().RegisterWorkflow(signalThenComplete)

	// 1) PLAN: describe the target state; the Driver computes the route. No traffic yet.
	plan, err := planner.DefaultModels().PlanTo("Workflow", "completed", planner.Shortest, planner.Constraints{})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"start", "complete"}}, plan.Routes)

	// 2) DRIVE: realize the planned route as real RPCs against the live cluster.
	act := &workflowActuator{env: env, workflowID: "umpire-e2e-wf"}
	require.NoError(t, plan.Run(env.Context(), act))

	// 3) JUDGE: wait until the Monitor's model reflects the completed workflow, then
	// assert it finds no violations for this namespace. (The teardown check does this
	// again authoritatively; asserting here makes the plan->drive->judge loop explicit.)
	nsRoot := umpirefw.NewEntityID(model.NamespaceType, env.NamespaceID().String())
	require.Eventually(t, func() bool {
		for _, e := range env.GetMonitor().ModelState().QueryEntities(model.WorkflowType, 0, &nsRoot) {
			if wf, ok := e.Entity.(*model.Workflow); ok && wf.FSM.Current() == "completed" {
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "the Monitor should observe the workflow completing")

	violations := env.GetMonitor().CheckNamespace(env.Context(), env.NamespaceID().String())
	require.Empty(t, violations, "the Monitor should find no violations for a cleanly driven workflow")
}

// nexusEntityPath builds the namespace-rooted path for a Nexus operation, keyed
// "<workflowID>:<scheduledEventID>" under its caller workflow — the identity the
// HSM span instrumentation will target.
func nexusEntityPath(nsID, workflowID, scheduledEventID string) *umpirefw.EntityPath {
	return &umpirefw.EntityPath{
		EntityID: umpirefw.NewEntityID(model.NexusOperationType, workflowID+":"+scheduledEventID),
		Ancestors: []umpirefw.EntityID{
			umpirefw.NewEntityID(model.NamespaceType, nsID),
			umpirefw.NewEntityID(model.WorkflowType, workflowID),
		},
	}
}

// TestNexusOperationModel verifies the Nexus operation model end-to-end through the
// umpire's own layers — the planner (active side) and the live cluster Monitor
// (passive side). Driving a real Nexus operation would require HSM span
// instrumentation that is not built yet (see UMPIRE_NEXUS.md), so the Monitor is
// fed the lifecycle facts that instrumentation will emit; everything else — the
// registered entity, its FSM, and the rules judging it — is the real production wiring.
func (s *UmpireTestSuite) TestNexusOperationModel() {
	t := s.T()
	env := testcore.NewEnv(t)
	ctx := env.Context()
	nsID := env.NamespaceID().String()

	// PLAN: the Nexus model is in the shared catalog and the planner can route over
	// it. Shortest route to "succeeded" is the sync path (start is skipped).
	plan, err := planner.DefaultModels().PlanTo("NexusOperation", "succeeded", planner.Shortest, planner.Constraints{})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"schedule", "succeed"}}, plan.Routes)

	// JUDGE: feed the live Monitor an async operation lifecycle and confirm the
	// registered entity builds and the rules find no violation.
	const wfID, schedID = "nexus-e2e-wf", "5"
	path := nexusEntityPath(nsID, wfID, schedID)

	scheduled := &fact.NexusOperationScheduled{}
	scheduled.ScheduledEventID, scheduled.WorkflowID, scheduled.EntityPath = schedID, wfID, path
	started := &fact.NexusOperationStarted{}
	started.ScheduledEventID, started.WorkflowID, started.EntityPath = schedID, wfID, path
	succeeded := &fact.NexusOperationSucceeded{}
	succeeded.ScheduledEventID, succeeded.WorkflowID, succeeded.EntityPath, succeeded.Outcome = schedID, wfID, path, "success"

	require.NoError(t, env.GetMonitor().ModelState().RouteFacts(ctx,
		[]umpirefw.Fact{scheduled, started, succeeded}))

	// The registered entity reached its terminal state in the live model.
	nsRoot := umpirefw.NewEntityID(model.NamespaceType, nsID)
	found := false
	for _, e := range env.GetMonitor().ModelState().QueryEntities(model.NexusOperationType, 0, &nsRoot) {
		if op, ok := e.Entity.(*model.NexusOperation); ok && op.FSM.Current() == "succeeded" {
			found = true
			require.Equal(t, "success", op.Outcome)
		}
	}
	require.True(t, found, "the Monitor should have built a succeeded NexusOperation")

	violations := env.GetMonitor().CheckNamespace(ctx, nsID)
	require.Empty(t, violations, "a cleanly-settled Nexus operation must yield no violations")
}
