package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/parallelsuite"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpire2"
	ksworker "go.temporal.io/server/tests/umpire2/kitchensink/worker"
)

// UmpireTestSuite is an end-to-end test of both halves of the umpire together:
// the Planner plans a route over an entity model, a Driver realizes that route
// as real traffic against a live cluster, and the Monitor (wired into every
// functional cluster) judges the result. It is the first test that exercises
// plan -> drive -> judge against a real server.
type UmpireTestSuite struct {
	parallelsuite.Suite[*UmpireTestSuite]
}

func TestUmpireTestSuite(t *testing.T) {
	parallelsuite.Run(t, &UmpireTestSuite{})
}

func defaultUmpireProtocol(t *testing.T) *umpire2.Protocol {
	t.Helper()
	compiled, err := umpire2.DefaultProtocol()
	require.NoError(t, err)
	return compiled
}

// signalThenComplete blocks on a "finish" signal, then returns — so the workflow's
// start and its completion are two separately drivable steps, matching the
// Workflow model's "start" and "complete" events.
func signalThenComplete(ctx workflow.Context) error {
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	return nil
}

// workflowDriver realizes the Workflow model's abstract events as real SDK calls.
// It holds the run handle between steps, since a route drives its events in order.
type workflowDriver struct {
	env        *testcore.TestEnv
	workflowID string
	run        sdkclient.WorkflowRun
}

func (d *workflowDriver) Do(ctx context.Context, a umpirefw.Step) error {
	switch a.Event {
	case "start":
		run, err := d.env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        d.workflowID,
			TaskQueue: d.env.WorkerTaskQueue(),
		}, signalThenComplete)
		if err != nil {
			return err
		}
		d.run = run
		return nil
	case "complete":
		if err := d.env.SdkClient().SignalWorkflow(ctx, d.workflowID, "", "finish", nil); err != nil {
			return err
		}
		return d.run.Get(ctx, nil) // block until the workflow actually completes
	default:
		return fmt.Errorf("workflowDriver: unhandled event %q", a.Event)
	}
}

func (s *UmpireTestSuite) TestPlanAndDriveWorkflowToCompletion() {
	t := s.T()
	env := testcore.NewEnv(t, testcore.WithUmpireMonitorFactory(umpire2.NewMonitor))
	env.SdkWorker().RegisterWorkflow(signalThenComplete)

	// 1) PLAN: describe the target state; the Driver computes the route. No traffic yet.
	plan, err := defaultUmpireProtocol(t).PlanTo(umpire2.WorkflowType, umpire2.WorkflowCompleted, umpirefw.Shortest, umpirefw.Constraints{})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"start", "complete"}}, plan.Routes)

	// 2) DRIVE: realize the planned route as real RPCs against the live cluster.
	driver := &workflowDriver{env: env, workflowID: "umpire-e2e-wf"}
	require.NoError(t, plan.Run(env.Context(), driver))

	// 3) JUDGE: wait until the Monitor's model reflects the completed workflow, then
	// assert it finds no violations for this namespace. (The teardown check does this
	// again authoritatively; asserting here makes the plan->drive->judge loop explicit.)
	require.Eventually(t, func() bool {
		for _, workflow := range env.GetMonitor().Snapshot(env.NamespaceID().String()).EntitiesOfType(umpire2.WorkflowType) {
			if workflow.Current == "completed" {
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "the Monitor should observe the workflow completing")

	violations := env.GetMonitor().CheckNamespace(env.Context(), env.NamespaceID().String())
	require.Empty(t, violations, "the Monitor should find no violations for a cleanly driven workflow")
}

// TestPlanAndDriveKitchenSinkWorkflow is the same plan -> drive -> judge loop, but the
// workload is the copied Omes kitchensink interpreter rather than a bespoke workflow:
// the planned route compiles (ksdriver.Compile) to a kitchensink TestInput, and
// ksdriver.RunPlan starts + drives it. This proves the umpire can drive arbitrary
// kitchensink-described behaviour and still judge it with zero hand-written assertions.
func (s *UmpireTestSuite) TestPlanAndDriveKitchenSinkWorkflow() {
	t := s.T()
	env := testcore.NewEnv(t, testcore.WithUmpireMonitorFactory(umpire2.NewMonitor))
	env.SdkWorker().RegisterWorkflow(ksworker.KitchenSinkWorkflow)

	// 1) PLAN: describe the target state; the Planner computes the route.
	plan, err := defaultUmpireProtocol(t).PlanTo(umpire2.WorkflowType, umpire2.WorkflowCompleted, umpirefw.Shortest, umpirefw.Constraints{})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"start", "complete"}}, plan.Routes)

	// 2) DRIVE: the route compiles to a kitchensink TestInput (a WorkflowInput that
	// returns a result); RunPlan starts the kitchensink workflow and drives it.
	require.NoError(t, umpire2.RunKitchenSinkPlan(env.Context(), env.SdkClient(), umpire2.KitchenSinkRunOptions{
		Namespace:      env.NamespaceID().String(),
		TaskQueue:      env.WorkerTaskQueue(),
		WorkflowType:   "KitchenSinkWorkflow",
		WorkflowIDBase: "umpire-ks-wf",
	}, "Workflow", plan))

	// 3) JUDGE: the Monitor observes the completion, then finds no violations.
	require.Eventually(t, func() bool {
		for _, workflow := range env.GetMonitor().Snapshot(env.NamespaceID().String()).EntitiesOfType(umpire2.WorkflowType) {
			if workflow.Current == umpire2.WorkflowCompleted {
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "the Monitor should observe the kitchensink workflow completing")

	violations := env.GetMonitor().CheckNamespace(env.Context(), env.NamespaceID().String())
	require.Empty(t, violations, "a cleanly driven kitchensink workflow must yield no violations")
}

// TestPlanAndDriveNexusOperationCHASM drives a REAL CHASM Nexus operation end-to-end
// and confirms the Monitor observes it via the enriched, generic chasm.transition
// telemetry (no per-operation instrumentation). This is the CHASM path — HSM has no
// such telemetry. The operation's transitions are delivered on the recording ambient
// span of the enclosing request/task, which reaches the Monitor through the cluster's
// TracerProvider.
func (s *UmpireTestSuite) TestPlanAndDriveNexusOperationCHASM() {
	t := s.T()

	env := newNexusTestEnv(t, true,
		testcore.WithUmpireMonitorFactory(umpire2.NewMonitor),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, true),
	)
	ctx := env.Context()
	nsID := env.NamespaceID().String()

	// A chasm.transition is emitted either on the recording ambient span of the
	// enclosing request/task (delivered to the Monitor via the cluster's own
	// TracerProvider, into which the functional harness wires it as a span
	// processor) or, when a transition runs with no recording ambient span, on a
	// fresh span from the global tracer. Wire the global tracer to this cluster's
	// Monitor too, so both delivery paths reach it.
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(env.GetMonitor())))
	t.Cleanup(func() { otel.SetTracerProvider(prevTP) })

	// A mock Nexus handler that completes the operation synchronously.
	endpointName := env.createRandomExternalNexusServer(ctx, t, nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
		},
	})

	// A caller workflow that invokes the Nexus operation and waits for it.
	callerWorkflow := func(wctx workflow.Context) error {
		c := workflow.NewNexusClient(endpointName, "service")
		return c.ExecuteOperation(wctx, "operation", "input", workflow.NexusOperationOptions{}).Get(wctx, nil)
	}
	env.SdkWorker().RegisterWorkflow(callerWorkflow)

	// PLAN: shortest route to a settled operation (sync path skips "started").
	plan, err := defaultUmpireProtocol(t).PlanTo(umpire2.NexusOperationType, umpire2.NexusSucceeded, umpirefw.Shortest, umpirefw.Constraints{})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"schedule", "succeed"}}, plan.Routes)

	// DRIVE: run the caller workflow — it schedules and completes the real CHASM operation.
	run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        "umpire-nexus-caller",
		TaskQueue: env.WorkerTaskQueue(),
	}, callerWorkflow)
	require.NoError(t, err)
	require.NoError(t, run.Get(ctx, nil))

	// JUDGE: the Monitor built a settled NexusOperation purely from chasm.transition
	// telemetry, and finds no violations.
	var observedAttempt int
	require.Eventually(t, func() bool {
		for _, operation := range env.GetMonitor().Snapshot(nsID).EntitiesOfType(umpire2.NexusOperationType) {
			if operation.Terminal {
				observedAttempt = operation.Attempt
				t.Logf("observed CHASM Nexus operation in state %q (attempt %d)", operation.Current, operation.Attempt)
				return true
			}
		}
		return false
	}, 20*time.Second, 200*time.Millisecond, "the Monitor should observe the CHASM Nexus operation via chasm.transition telemetry")

	// The enriched chasm.transition telemetry carries the attempt count, which the Monitor
	// records — signal the workflow history does not expose.
	require.GreaterOrEqual(t, observedAttempt, 1, "attempt count must flow via the enriched chasm.transition telemetry")

	violations := env.GetMonitor().CheckNamespace(ctx, nsID)
	require.Empty(t, violations, "a cleanly settled CHASM Nexus operation must yield no violations")
}

// TestPlanAndDriveKitchenSinkNexusOperation drives a Nexus operation the same way, but the
// caller is the copied Omes kitchensink interpreter rather than a bespoke workflow: the
// NexusOperation route compiles (ksdriver.Compile + WithNexus) to a kitchensink workflow
// whose ExecuteNexusOperation action schedules and awaits the op. Same plan -> drive ->
// judge loop, arbitrary kitchensink-described workload.
func (s *UmpireTestSuite) TestPlanAndDriveKitchenSinkNexusOperation() {
	t := s.T()

	env := newNexusTestEnv(t, true,
		testcore.WithUmpireMonitorFactory(umpire2.NewMonitor),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, true),
	)
	ctx := env.Context()
	nsID := env.NamespaceID().String()

	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(env.GetMonitor())))
	t.Cleanup(func() { otel.SetTracerProvider(prevTP) })

	// A mock Nexus handler that completes synchronously.
	endpointName := env.createRandomExternalNexusServer(ctx, t, nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
		},
	})

	// The workload is the copied kitchensink interpreter; its ExecuteNexusOperation action
	// schedules the op (against the kitchensink service on the endpoint above).
	env.SdkWorker().RegisterWorkflow(ksworker.KitchenSinkWorkflow)

	// PLAN: shortest route to a settled operation (sync path skips "started").
	plan, err := defaultUmpireProtocol(t).PlanTo(umpire2.NexusOperationType, umpire2.NexusSucceeded, umpirefw.Shortest, umpirefw.Constraints{})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"schedule", "succeed"}}, plan.Routes)

	// DRIVE: the route compiles to a kitchensink workflow that schedules + awaits the op.
	require.NoError(t, umpire2.RunKitchenSinkPlan(ctx, env.SdkClient(), umpire2.KitchenSinkRunOptions{
		Namespace:      nsID,
		TaskQueue:      env.WorkerTaskQueue(),
		WorkflowType:   "KitchenSinkWorkflow",
		WorkflowIDBase: "umpire-ks-nexus",
		NexusEndpoint:  endpointName,
		NexusOperation: "operation",
	}, "NexusOperation", plan))

	// JUDGE: the Monitor built a settled NexusOperation from chasm.transition telemetry.
	require.Eventually(t, func() bool {
		for _, operation := range env.GetMonitor().Snapshot(nsID).EntitiesOfType(umpire2.NexusOperationType) {
			if operation.Terminal {
				t.Logf("observed kitchensink-driven Nexus operation in state %q", operation.Current)
				return true
			}
		}
		return false
	}, 20*time.Second, 200*time.Millisecond, "the Monitor should observe the kitchensink-driven Nexus operation settle")

	violations := env.GetMonitor().CheckNamespace(ctx, nsID)
	require.Empty(t, violations, "a cleanly settled kitchensink Nexus operation must yield no violations")
}
