package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/probe"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpire2/action"
	"go.temporal.io/server/tests/umpire2/model"
	"go.temporal.io/server/tests/umpire2/planner"
	umpire2protocol "go.temporal.io/server/tests/umpire2/protocol"
)

// TEMPORAL_OTEL_DEBUG enables the generic chasm.transition telemetry the umpire Monitor
// consumes. Set once for the whole test binary — never per-test — so concurrent umpire
// tests can't unset it out from under one another mid-flight.
func init() { os.Setenv("TEMPORAL_OTEL_DEBUG", "true") }

// newNexusProbeEnv builds a fresh CHASM-Nexus-enabled cluster env — its own namespace.
// The Monitor is already a span processor on the cluster's own TracerProvider, so
// chasm.transition events reach it on the enclosing request/task's recording span; no
// process-global tracer wiring is needed (and per-execution global wiring would race
// across concurrent tests).
func (s *UmpireTestSuite) newNexusProbeEnv(t *testing.T) *NexusTestEnv {
	return newNexusTestEnv(t, true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, true),
	)
}

// callerFor builds the caller workflow from a resolved mock-endpoint name; different
// builders drive the operation to different outcomes (wait, cancel, time out).
type callerFor func(endpoint string) any

// syncCaller executes the operation and waits for it — the default drive. The outcome
// (sync success, op failure, retry, async start) is determined by the mock handler.
func syncCaller(endpoint string) any {
	return func(wctx workflow.Context) error {
		c := workflow.NewNexusClient(endpoint, "service")
		return c.ExecuteOperation(wctx, "operation", "input", workflow.NexusOperationOptions{}).Get(wctx, nil)
	}
}

// timeoutCaller starts an async operation the handler never completes, bounded by a
// short schedule-to-close timeout, driving the operation to the timed_out terminal.
func timeoutCaller(endpoint string) any {
	return func(wctx workflow.Context) error {
		c := workflow.NewNexusClient(endpoint, "service")
		return c.ExecuteOperation(wctx, "operation", "input", workflow.NexusOperationOptions{
			ScheduleToCloseTimeout: 2 * time.Second,
		}).Get(wctx, nil) // resolves with a timed-out error
	}
}

// nexusProbeDrive registers the given caller workflow (built from a fresh mock endpoint
// using onStart) and returns a probe DriveFunc that runs it.
func (s *UmpireTestSuite) nexusProbeDrive(t *testing.T, env *NexusTestEnv, onStart nexustest.Handler, build callerFor) probe.DriveFunc {
	endpointName := env.createRandomExternalNexusServer(env.Context(), t, onStart)
	callerWorkflow := build(endpointName)
	env.SdkWorker().RegisterWorkflow(callerWorkflow)
	return func(dctx context.Context, iter int) error {
		run, err := env.SdkClient().ExecuteWorkflow(dctx, sdkclient.StartWorkflowOptions{
			ID:        fmt.Sprintf("umpire-probe-nexus-%d", iter),
			TaskQueue: env.WorkerTaskQueue(),
		}, callerWorkflow)
		if err != nil {
			return err
		}
		return run.Get(dctx, nil)
	}
}

// nexusExec is the probe EnvFunc: for each execution it builds a fresh, isolated env
// (its own namespace) and the drive that runs the caller workflow with onStart.
func (s *UmpireTestSuite) nexusExec(onStart nexustest.Handler, build callerFor) probe.EnvFunc {
	return func(t *testing.T, _ int) (*testcore.TestEnv, probe.DriveFunc) {
		env := s.newNexusProbeEnv(t)
		return env.TestEnv, s.nexusProbeDrive(t, env, onStart, build)
	}
}

// newNexusStandaloneEnv is a probe env with standalone Nexus operations enabled (in
// addition to workflow-based ones), for the Standalone-hosting driver.
func (s *UmpireTestSuite) newNexusStandaloneEnv(t *testing.T) *NexusTestEnv {
	return newNexusTestEnv(t, true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, true),
		testcore.WithDynamicConfig(chasmnexus.Enabled, true),
	)
}

// nexusGenExecPlan is the single generated Standalone-hosting driver (actions-model): it runs
// any declared plan through the generic umpire.Drive runtime — no bespoke drive logic — then
// reconciles the declared effects against what was observed. The plan is the only thing that
// varies across drives. Drive confirms the plan's endpoint state before returning.
func (s *UmpireTestSuite) nexusGenExecPlan(plan []umpire.Action) probe.EnvFunc {
	return func(t *testing.T, _ int) (*testcore.TestEnv, probe.DriveFunc) {
		env := s.newNexusStandaloneEnv(t)
		policy := action.NewResponsePolicy()
		endpoint := env.createRandomExternalNexusServer(env.Context(), t, policy.Handler())
		return env.TestEnv, func(dctx context.Context, iter int) error {
			rc := action.NewCtx(env.TestEnv, endpoint, policy, iter)
			defer rc.Cleanup() // unregister any fault actions the plan armed
			oracle := action.Oracle{Env: env.TestEnv}
			if err := umpire.Drive(dctx, rc, oracle, action.Resolver{}, 50*time.Millisecond, plan); err != nil {
				return err
			}
			if drift := umpire.Reconcile(oracle, rc, plan); len(drift) > 0 {
				return fmt.Errorf("actions model drift: %v", drift)
			}
			return nil
		}
	}
}

// TestProbeNexusGeneratedCompletion is the Phase-1 round-trip proof: a declared action
// sequence, driven by the generic runtime, reproduces a path we already trust — the standalone
// operation settles at succeeded.
func (s *UmpireTestSuite) TestProbeNexusGeneratedCompletion() {
	t := s.T()
	report := probe.Umpire(t).
		Reach("NexusOperation", "succeeded").
		Execution(s.nexusGenExecPlan(action.StandaloneCompletion())).
		Timeout(15 * time.Second).
		Judge()

	require.Equal(t, probe.Recovered, report.Baseline.Verdict, "generated standalone completion should reach succeeded")
	require.Equal(t, "succeeded", report.Baseline.Terminal)
}

// TestProbeNexusRejectedStart drives a well-formed StartNexusOperationExecution naming a
// non-existent endpoint, and its
// synchronous rejection is modeled as an entity reaching the `rejected` Failure terminal. The
// Monitor decodes the RPC error into a fact, so the same umpire.Reconcile that judges any other
// transition confirms the action's reject Effect, and exactly one (rejected) operation exists.
func (s *UmpireTestSuite) TestProbeNexusRejectedStart() {
	t := s.T()
	env := s.newNexusStandaloneEnv(t)

	plan := []umpire.Action{action.StartUnknownEndpoint}
	dctx, cancel := context.WithTimeout(env.Context(), 15*time.Second)
	defer cancel()
	oracle := action.Oracle{Env: env.TestEnv}
	rc := action.NewCtx(env.TestEnv, "", action.NewResponsePolicy(), 0)
	defer rc.Cleanup()

	err := umpire.Drive(dctx, rc, oracle, action.Resolver{}, 50*time.Millisecond, plan)
	require.NoError(t, err, "a declared rejection is an expected outcome, not a drive failure")
	require.Empty(t, umpire.Reconcile(oracle, rc, plan), "the modeled rejection (reject edge) must be observed")
	require.Equal(t, 1, action.CountEntities(env.TestEnv, model.NexusOperationType), "the rejection is modeled as exactly one rejected operation")
}

// TestProbeNexusReflectedVariant enumerates invalid actions by reflecting the
// StartNexusOperationExecution descriptor — a variant per string field
// — rather than hand-authored. Driving one reflected variant (operation_id mutated to empty on an
// otherwise-valid base) observes the op reaching the
// `rejected` terminal, confirmed by Reconcile.
func (s *UmpireTestSuite) TestProbeNexusReflectedVariant() {
	t := s.T()

	variants := action.StartFieldVariants()
	require.NotEmpty(t, variants, "reflection should derive per-field variants from the request descriptor")
	var mutated umpire.Action
	for _, a := range variants {
		if strings.Contains(a.Name, "operation_id=empty") {
			mutated = a
			break
		}
	}
	require.NotEmpty(t, mutated.Name, "the descriptor should yield an operation_id string field")

	env := s.newNexusStandaloneEnv(t)
	policy := action.NewResponsePolicy()
	endpoint := env.createRandomExternalNexusServer(env.Context(), t, policy.Handler())

	plan := []umpire.Action{mutated}
	dctx, cancel := context.WithTimeout(env.Context(), 15*time.Second)
	defer cancel()
	oracle := action.Oracle{Env: env.TestEnv}
	rc := action.NewCtx(env.TestEnv, endpoint, policy, 0)
	defer rc.Cleanup()

	err := umpire.Drive(dctx, rc, oracle, action.Resolver{}, 50*time.Millisecond, plan)
	require.NoError(t, err, "a reflected invalid variant is an expected rejection, not a drive failure")
	require.Empty(t, umpire.Reconcile(oracle, rc, plan), "the reflected variant's rejection must be observed as the reject edge")
	require.Equal(t, 1, action.CountEntities(env.TestEnv, model.NexusOperationType), "the rejection is modeled as exactly one rejected operation")
}

// TestProbeNexusReflectedDurationVariant proves descriptor reflection generalizes past strings to
// message-typed fields. It drives a reflected
// Duration variant (schedule_to_start_timeout mutated negative on an otherwise-valid base), which
// the server rejects (InvalidArgument) before the operation exists, and the model observes it as
// the op reaching the rejected terminal.
func (s *UmpireTestSuite) TestProbeNexusReflectedDurationVariant() {
	t := s.T()

	var mutated umpire.Action
	for _, a := range action.StartFieldVariants() {
		if strings.Contains(a.Name, "schedule_to_start_timeout=negative") {
			mutated = a
			break
		}
	}
	require.NotEmpty(t, mutated.Name, "the descriptor should yield a schedule_to_start_timeout Duration field")

	env := s.newNexusStandaloneEnv(t)
	policy := action.NewResponsePolicy()
	endpoint := env.createRandomExternalNexusServer(env.Context(), t, policy.Handler())

	plan := []umpire.Action{mutated}
	dctx, cancel := context.WithTimeout(env.Context(), 15*time.Second)
	defer cancel()
	oracle := action.Oracle{Env: env.TestEnv}
	rc := action.NewCtx(env.TestEnv, endpoint, policy, 0)
	defer rc.Cleanup()

	err := umpire.Drive(dctx, rc, oracle, action.Resolver{}, 50*time.Millisecond, plan)
	require.NoError(t, err, "a reflected Duration variant is an expected rejection, not a drive failure")
	require.Empty(t, umpire.Reconcile(oracle, rc, plan), "the Duration variant's rejection must be observed as the reject edge")
	require.Equal(t, 1, action.CountEntities(env.TestEnv, model.NexusOperationType), "the rejection is modeled as exactly one rejected operation")
}

// TestProbeNexusFaultAction shows a fault as a first-class plan action (Phase 5): a Hold on the
// outbound Nexus invocation is injected declaratively into a completion plan; the operation is
// delayed but still settles at succeeded — resilience, driven by the actions model.
func (s *UmpireTestSuite) TestProbeNexusFaultAction() {
	t := s.T()
	// Hold the outbound Nexus HTTP invocation (matched by its "…/service/operation" path) for
	// 500ms, then run the standalone completion path.
	plan := append([]umpire.Action{action.Hold("service/operation", 500*time.Millisecond)}, action.StandaloneCompletion()...)
	report := probe.Umpire(t).
		Reach("NexusOperation", "succeeded").
		Execution(s.nexusGenExecPlan(plan)).
		Timeout(20 * time.Second).
		Judge()

	require.Equal(t, probe.Recovered, report.Baseline.Verdict, "the operation should tolerate a held invocation")
	require.Equal(t, "succeeded", report.Baseline.Terminal)
}

// TestProbeNexusResilience is the trace-derived resilience demo, now over a generated plan
// (actions model): drive the sync-success completion via the runtime, learn the underlying
// gRPC/HTTP calls from the happy-path trace, break each one, and let the Monitor judge — no
// hand-written faults and no hand-written outcome assertions (only the fault-free baseline is
// asserted). This is learned-footprint fault exploration over an action plan.
func (s *UmpireTestSuite) TestProbeNexusResilience() {
	t := s.T()
	report := probe.Umpire(t).
		Reach("NexusOperation", "succeeded").
		Execution(s.nexusGenExecPlan(action.EmbeddedSyncSuccess())).
		Timeout(10 * time.Second).
		MaxFaults(6).
		FaultEachObservedCall().
		Judge()

	require.NoError(t, report.Baseline.DriveErr, "baseline (no fault) should reach the target")
	require.Empty(t, report.Baseline.Violations, "baseline (no fault) must be clean")
	require.NotEmpty(t, report.Observed, "the probe should observe the happy-path call footprint")
	require.NotEmpty(t, report.Scenarios, "the probe should derive at least one fault scenario")
}

// TestProbeNexusDegraded shows the model-derived Degraded verdict: the operation
// handler fails the operation, so it settles at the `failed` terminal — a modeled
// Failure disposition, judged as acceptable degradation, not a bug.
func (s *UmpireTestSuite) TestProbeNexusDegraded() {
	t := s.T()
	report := probe.Umpire(t).
		Reach("NexusOperation", "succeeded").
		Execution(s.nexusGenExecPlan(action.EmbeddedOpFailure())). // generated from the actions model
		Timeout(15 * time.Second).
		Judge() // baseline only; the op-failed handler action produces the outcome

	require.Equal(t, probe.Degraded, report.Baseline.Verdict, "a failed operation is a modeled Failure terminal")
	require.Equal(t, "failed", report.Baseline.Terminal)
}

// TestProbeNexusFlagged shows the model-derived Flagged verdict: the handler fails
// retryably forever, so the operation never settles; at teardown the generic
// EntityProgress liveness rule flags it — the real bug tier.
func (s *UmpireTestSuite) TestProbeNexusFlagged() {
	t := s.T()
	report := probe.Umpire(t).
		Reach("NexusOperation", "succeeded").
		Execution(s.nexusExec(nexustest.Handler{
			OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "umpire probe: injected retryable failure")
			},
		}, syncCaller)).
		Timeout(8 * time.Second). // the op never settles; bound the stranded drive
		Judge()

	require.Equal(t, probe.Flagged, report.Baseline.Verdict, "an operation that never settles must be flagged")
	require.NotEmpty(t, report.Baseline.Violations, "EntityProgress should flag the stuck operation")
}

// TestProbeNexusHTTPFaultSeam proves the Nexus operation's outbound HTTP call to its
// handler now flows through the same fault seam as gRPC: it appears in the observed
// footprint (method "HTTP <METHOD> <path>"), so drop/hold faults can target it. It also
// exercises the hold fault against that HTTP call and confirms the hold fired.
func (s *UmpireTestSuite) TestProbeNexusHTTPFaultSeam() {
	t := s.T()
	syncOK := nexustest.Handler{OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
		return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
	}}

	// MaxFaults(1) keeps it fast (one fault scenario) while still capturing the full
	// observed footprint, which now includes the outbound Nexus HTTP invocation.
	report := probe.Umpire(t).
		Reach("NexusOperation", "succeeded").
		Execution(s.nexusExec(syncOK, syncCaller)).
		Timeout(10 * time.Second).
		MaxFaults(1).
		FaultEachObservedCall().
		Judge()

	var httpCalls []string
	for _, m := range report.Observed {
		if strings.HasPrefix(m, "HTTP ") {
			httpCalls = append(httpCalls, m)
		}
	}
	require.NotEmpty(t, httpCalls, "the Nexus HTTP invocation should be observed via the shared fault seam; observed=%v", report.Observed)

	// Hold that exact HTTP call for 500ms and confirm the hold fired — the same seam and
	// matching drives both gRPC and HTTP faults.
	held := probe.Umpire(t).
		Reach("NexusOperation", "succeeded").
		Execution(s.nexusExec(syncOK, syncCaller)).
		Timeout(10*time.Second).
		InjectHoldOn(httpCalls[0], 500*time.Millisecond).
		Judge()

	require.Len(t, held.Scenarios, 1)
	require.True(t, held.Scenarios[0].Fired, "the hold fault should have matched and held the Nexus HTTP call")
}

// TestProbeNexusExploration drives the NexusOperation through several distinct
// outcomes — each in its own isolated namespace — accumulating transition coverage
// into one shared tracker, and reports which of the model's valid edges the real
// implementation actually exercised (and which remain unexercised). It is the
// "explore as fully as possible + summary" mode.
func (s *UmpireTestSuite) TestProbeNexusExploration() {
	t := s.T()
	// Gate: every WorkerCommand action must have a kitchensink mapping before any drive runs,
	// so the exploration can't silently skip a worker-driven edge (see action.kitchensink).
	require.NoError(t, action.ValidateKitchensinkMappings(), "kitchensink mappings must be exhaustive")
	// Gate: every request field under invalid-input testing must be enumerated or consciously
	// deferred, so the negative-space drive can't silently skip a field (see action.mutation_gate).
	require.NoError(t, action.ValidateMutationCoverage(), "mutation coverage must be exhaustive")
	cov := probe.NewCoverage()
	compiled, err := umpire2protocol.Default()
	require.NoError(t, err)
	semanticCoverage, err := compiled.NewCoverage(true, umpire2protocol.CoverageCatalogOptions{
		EntityTypes: []umpire.EntityType{model.NexusOperationType},
		Kinds:       []umpire.CoverageKind{umpire.CoverageTransition},
	})
	require.NoError(t, err)

	exploreEnv := func(exec probe.EnvFunc, timeout time.Duration) {
		probe.Umpire(t).
			WithCoverage(cov).
			WithSemanticCoverage(semanticCoverage).
			Reach("NexusOperation", "succeeded"). // plan validation only; the handler+caller drive the actual outcome
			Execution(exec).
			Timeout(timeout).
			Judge()
	}
	explore := func(handler nexustest.Handler, build callerFor, timeout time.Duration) {
		exploreEnv(s.nexusExec(handler, build), timeout)
	}

	// asyncStart is an async acknowledgement the handler never resolves — the operation
	// reaches STARTED and stays there until the caller cancels it or it times out.
	asyncStart := nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "umpire-probe-async-token"}, nil
		},
		OnCancelOperation: func(_ context.Context, _, _, _ string, _ nexus.CancelOperationOptions) error { return nil },
	}

	// The list of drives is computed from the model — one plan
	// per settling edge, assembled by PlanEdge and driven in its own env. This covers every
	// edge across both hostings (embedded handler/completion outcomes, standalone terminates)
	// except the two server-timer edges below, which have no atomic action.
	for _, plan := range action.AutoCoverPlans() {
		exploreEnv(s.nexusGenExecPlan(plan), 15*time.Second)
	}
	// The two server-driven edges auto-cover skips, driven bespoke:
	//   backing_off --schedule--> scheduled: the retry reschedule — a cycling retryable handler
	//   observes it (and scheduled --attempt_failed--> backing_off) but never settles.
	explore(nexustest.Handler{OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "umpire probe: injected retryable failure")
	}}, syncCaller, 8*time.Second)
	//   started --timeout--> timed_out: a real schedule-to-close timer on a never-completing
	//   async operation (the force-timeout hook fires on the attempt, not once started).
	explore(asyncStart, timeoutCaller, 10*time.Second)
	//   unspecified --reject--> rejected: a synchronous rejection (invalid input) — the Monitor
	//   models the RPC error as the operation reaching the rejected terminal.
	exploreEnv(s.nexusGenExecPlan([]umpire.Action{action.StartUnknownEndpoint}), 15*time.Second)

	lc, ok := planner.DefaultModels().Lifecycle("NexusOperation")
	require.True(t, ok)
	rep := cov.Report("NexusOperation", lc.Edges())
	t.Logf("[exploration] NexusOperation transition coverage: %d/%d edges exercised", rep.Covered, rep.Total)
	for _, e := range rep.Missing() {
		t.Logf("[exploration]   MISSING: %s --%s--> %s", e.From, e.Event, e.To)
	}
	// Every modelled edge is now exercised, across both hostings: the workflow (Embedded)
	// driver covers the shared lifecycle, and the standalone driver covers the three
	// {scheduled,backing_off,started} --terminate--> terminated edges that are Standalone-only
	// (terminated is reached via the operation's own Terminate RPC — a workflow-child op has
	// none, and workflow termination does not cascade to it). The unreachable-by-construction
	// backing_off --> {start,succeed,fail,cancel} edges were pruned from the model, so the
	// count reflects the machine's real shape.
	require.Equal(t, rep.Total, rep.Covered,
		"every modelled NexusOperation edge should be exercised across the workflow and standalone drivers")
	for _, point := range semanticCoverage.Unmet() {
		t.Logf("[exploration]   MISSING semantic %s: %s", point.Kind, point.ID)
	}
	require.Empty(t, semanticCoverage.Unmet(), "every protocol-declared NexusOperation transition should be exercised")
}

// TestProbeWorkflowGenerated is the second-entity proof: a WorkflowRun — not a NexusOperation — is
// driven by the same generic runtime (Drive/Reconcile), with the same generalized Oracle reading
// its lifecycle. Nothing in the generic layer changed to add it; only the Workflow-family registry,
// realizers, and the run-precise WorkflowRun entity (keyed by RunID, so multiple runs of one
// WorkflowID are distinct). The run is driven to completed and the Monitor's model must observe it
// there with no reconciliation drift — the run's observable lifecycle is exactly created→completed
// (the completion span carries the RunID), so unlike the Workflow-by-id aggregate this reconciles
// cleanly.
func (s *UmpireTestSuite) TestProbeWorkflowGenerated() {
	t := s.T()
	env := testcore.NewEnv(t) // a plain env: the Monitor observes classic workflow facts
	plan := action.WorkflowRunPlan()
	dctx, cancel := context.WithTimeout(env.Context(), 15*time.Second)
	defer cancel()
	rc := action.NewCtx(env, "", action.NewResponsePolicy(), 0)
	defer rc.Cleanup()
	oracle := action.Oracle{Env: env}

	require.NoError(t, umpire.Drive(dctx, rc, oracle, action.Resolver{}, 50*time.Millisecond, plan),
		"the generic runtime should drive the WorkflowRun entity created→started→completed")
	require.Empty(t, umpire.Reconcile(oracle, rc, plan), "the run lifecycle should ground clean")

	runID, ok := rc.Binding("run")
	require.True(t, ok, "the run should be bound")
	state, ok := oracle.Current(model.WorkflowRunType, runID)
	require.True(t, ok, "the run should be modelled")
	require.Equal(t, model.WorkflowRunCompleted, state, "the run should reach completed")

	// The run was observed at start with its lineage: a first run is its own chain root with no
	// predecessor (the foundation the continue-as-new / reset graph builds on).
	run := workflowRun(env, runID)
	require.NotNil(t, run, "the run entity should be modelled")
	require.Equal(t, runID, run.FirstRunID, "a first run is its own chain root")
	require.Empty(t, run.PreviousRunID, "a first run has no predecessor")
	t.Logf("[workflow] drove run %s to %s (first=%s prev=%q)", runID, state, run.FirstRunID, run.PreviousRunID)
}

// continueAsNewOnceWorkflow continues-as-new once (first run), then completes (successor run) — so
// one WorkflowID has two runs linked by lineage.
func continueAsNewOnceWorkflow(ctx workflow.Context, done bool) error {
	if done {
		return nil
	}
	return workflow.NewContinueAsNewError(ctx, continueAsNewOnceWorkflow, true)
}

// TestProbeWorkflowContinueAsNew is the run-graph proof: a continue-as-new produces two runs of one
// WorkflowID, and the Monitor models both — the first (its own chain root, no predecessor) and the
// successor (predecessor = the first run, same root) — from the start telemetry's lineage
// attributes. This is the foundation the CAN/reset action model (relationship refs) builds on.
func (s *UmpireTestSuite) TestProbeWorkflowContinueAsNew() {
	t := s.T()
	env := testcore.NewEnv(t)
	env.SdkWorker().RegisterWorkflow(continueAsNewOnceWorkflow)

	run, err := env.SdkClient().ExecuteWorkflow(env.Context(), sdkclient.StartWorkflowOptions{
		ID:        "umpire-can-wf",
		TaskQueue: env.WorkerTaskQueue(),
	}, continueAsNewOnceWorkflow, false)
	require.NoError(t, err)
	require.NoError(t, run.Get(env.Context(), nil), "the continue-as-new chain should complete")

	nsRoot := umpire.NewEntityID(model.NamespaceType, env.NamespaceID().String())
	var first, succ *model.WorkflowRun
	require.Eventually(t, func() bool {
		first, succ = nil, nil
		for _, e := range env.GetMonitor().ModelState().QueryEntities(model.WorkflowRunType, 0, &nsRoot) {
			r, ok := e.Entity.(*model.WorkflowRun)
			if !ok {
				continue
			}
			if r.PreviousRunID == "" {
				first = r
			} else {
				succ = r
			}
		}
		// Wait until both runs are modelled and the predecessor has reached its continue-as-new
		// terminal (its close fact may arrive just after the successor's start).
		return first != nil && succ != nil && first.FSM.Current() == model.WorkflowRunContinuedAsNew
	}, 15*time.Second, 200*time.Millisecond, "both runs modelled, predecessor continued-as-new")

	require.Equal(t, first.RunID, succ.PreviousRunID, "the successor's predecessor is the first run")
	require.Equal(t, first.RunID, succ.FirstRunID, "both runs share the chain root")
	require.Equal(t, first.RunID, first.FirstRunID, "the first run is its own chain root")
	require.Equal(t, "continued_as_new", succ.Initiator, "the edge is typed continued_as_new")
	require.Equal(t, model.WorkflowRunContinuedAsNew, first.FSM.Current(), "the predecessor reached the continued_as_new terminal")
	require.Equal(t, model.WorkflowRunCompleted, succ.FSM.Current(), "the successor completed")
	t.Logf("[can] first=%s (%s) --%s--> succ=%s (%s)", first.RunID, first.FSM.Current(), succ.Initiator, succ.RunID, succ.FSM.Current())
}

// TestProbeWorkflowContinueAsNewGenerated is the multi-run action-model proof: the generic runtime
// drives a continue-as-new chain and reconciles *both* runs — the predecessor
// (created→started→continued_as_new) and its successor (created→started→completed). The successor's
// ref is bound by observation (the run whose predecessor is the first run), so the driver never
// supplies the server-minted successor RunID — the race-free identity design end-to-end.
func (s *UmpireTestSuite) TestProbeWorkflowContinueAsNewGenerated() {
	t := s.T()
	env := testcore.NewEnv(t)
	plan := action.WorkflowContinueAsNewPlan()
	dctx, cancel := context.WithTimeout(env.Context(), 20*time.Second)
	defer cancel()
	rc := action.NewCtx(env, "", action.NewResponsePolicy(), 0)
	defer rc.Cleanup()
	oracle := action.Oracle{Env: env}

	require.NoError(t, umpire.Drive(dctx, rc, oracle, action.Resolver{}, 50*time.Millisecond, plan),
		"the generic runtime should drive the continue-as-new run graph")
	require.Empty(t, umpire.Reconcile(oracle, rc, plan), "both runs should ground clean")

	runA, okA := rc.Binding("run")
	runB, okB := rc.Binding("run2")
	require.True(t, okA, "the predecessor should be bound (by the realizer)")
	require.True(t, okB, "the successor should be bound (by observation)")
	require.NotEqual(t, runA, runB, "predecessor and successor are distinct runs")

	a, b := workflowRun(env, runA), workflowRun(env, runB)
	require.Equal(t, model.WorkflowRunContinuedAsNew, a.FSM.Current(), "predecessor continued-as-new")
	require.Equal(t, model.WorkflowRunCompleted, b.FSM.Current(), "successor completed")
	require.Equal(t, runA, b.PreviousRunID, "the successor is linked to the predecessor")
	t.Logf("[can-gen] drove %s (%s) --%s--> %s (%s)", runA, a.FSM.Current(), b.Initiator, runB, b.FSM.Current())
}

// immediateWorkflow completes as soon as it runs — a base run to reset.
func immediateWorkflow(workflow.Context) error { return nil }

// TestProbeWorkflowReset proves the reset (tree-fork) lineage edge: resetting a run forks a new
// run, and the Monitor models it with the base run as its predecessor — from the reset run's start
// telemetry. Unlike continue-as-new (a chain), reset forks, but the edge is captured the same way.
func (s *UmpireTestSuite) TestProbeWorkflowReset() {
	t := s.T()
	env := testcore.NewEnv(t)
	env.SdkWorker().RegisterWorkflow(immediateWorkflow)

	run, err := env.SdkClient().ExecuteWorkflow(env.Context(), sdkclient.StartWorkflowOptions{
		ID:        "umpire-reset-wf",
		TaskQueue: env.WorkerTaskQueue(),
	}, immediateWorkflow)
	require.NoError(t, err)
	baseRunID := run.GetRunID()
	require.NoError(t, run.Get(env.Context(), nil), "the base run should complete")

	// Reset to the first workflow task completed (event 4), forking a new run from the base.
	resetResp, err := env.SdkClient().ResetWorkflowExecution(env.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: "umpire-reset-wf", RunId: baseRunID},
		Reason:                    "umpire reset test",
		WorkflowTaskFinishEventId: 4,
		RequestId:                 "umpire-reset-req",
	})
	require.NoError(t, err)
	resetRunID := resetResp.GetRunId()
	require.NotEqual(t, baseRunID, resetRunID, "reset forks a new run")

	require.Eventually(t, func() bool {
		r := workflowRun(env, resetRunID)
		return r != nil && r.PreviousRunID == baseRunID
	}, 15*time.Second, 200*time.Millisecond, "the reset run should be modelled with the base run as predecessor")
	t.Logf("[reset] base=%s reset=%s", baseRunID, resetRunID)
}

// workflowRun returns the modelled WorkflowRun with the given RunID, or nil.
func workflowRun(env *testcore.TestEnv, runID string) *model.WorkflowRun {
	nsRoot := umpire.NewEntityID(model.NamespaceType, env.NamespaceID().String())
	for _, e := range env.GetMonitor().ModelState().QueryEntities(model.WorkflowRunType, 0, &nsRoot) {
		if r, ok := e.Entity.(*model.WorkflowRun); ok && r.RunID == runID {
			return r
		}
	}
	return nil
}

// TestProbeNexusLearnedFootprint proves fault targeting comes from the *learned* footprint, not the
// statically declared Entry points: it drives the standalone completion path once under observation
// and reduces the observed calls to fault targets. The result is non-empty (the drive makes internal
// calls) and excludes the plan's own client-entry RPC (StartNexusOperationExecution) — a drop of
// which would just fail the drive rather than test resilience. It then reconciles the plan's
// *declared* footprint (Action.Footprint) against the observed one — a wire-level regression gate.
func (s *UmpireTestSuite) TestProbeNexusLearnedFootprint() {
	t := s.T()
	env := s.newNexusStandaloneEnv(t)
	policy := action.NewResponsePolicy()
	endpoint := env.createRandomExternalNexusServer(env.Context(), t, policy.Handler())

	plan := action.StandaloneCompletion()
	dctx, cancel := context.WithTimeout(env.Context(), 15*time.Second)
	defer cancel()
	rc := action.NewCtx(env.TestEnv, endpoint, policy, 0)
	defer rc.Cleanup()
	oracle := action.Oracle{Env: env.TestEnv}

	learned, err := action.LearnFootprint(dctx, rc, oracle, action.Resolver{}, 50*time.Millisecond, plan)
	require.NoError(t, err, "the observed drive should reach its terminal")
	require.NotEmpty(t, learned, "the drive should make observable calls")

	targets := action.FaultTargets(plan, learned)
	require.NotEmpty(t, targets, "the learned footprint should yield internal fault targets")
	for _, m := range targets {
		require.NotContains(t, m, "StartNexusOperationExecution", "the client-entry RPC must not be a fault target")
	}
	t.Logf("[footprint] learned %d call(s); %d fault target(s): %v", len(learned), len(targets), targets)

	// The plan's declared footprint (Action.Footprint) must match what was observed — no expected
	// internal call missing, no undeclared call observed. This is the wire-level analog of the
	// effect-level Reconcile: a refactor that changes which internal calls the transition makes
	// trips a drift here that the effect check would miss.
	drift := action.ReconcileFootprint(plan, learned)
	require.Empty(t, drift, "declared footprint should ground against the observed calls: %v", drift)
}

// TestProbeNexusCoverageGuidedFaults is the coverage-guided upgrade of the uniform-random fuzz
// loop: it learns each plan's footprint once, then ScheduleFaults hands back a novelty-prioritized,
// budget-bounded drive list — each distinct fault target once before any repeat. It drives the
// scheduled faults and asserts the rulebook invariant (no fault may flag a violation), logging what
// the budget forced it to drop so the coverage cost is explicit, never a silent truncation.
func (s *UmpireTestSuite) TestProbeNexusCoverageGuidedFaults() {
	t := s.T()

	// Learn phase: drive each plan's baseline once under observation to learn its footprint. Two
	// standalone plans with differing footprints, so the scheduler has breadth to prioritize.
	type spec struct {
		label string
		plan  []umpire.Action
	}
	specs := []spec{
		{"standalone-completion", action.StandaloneCompletion()},
		{"standalone-terminate", action.StandaloneTerminate(model.NexusScheduled)},
	}
	learn := func(plan []umpire.Action) []string {
		env := s.newNexusStandaloneEnv(t)
		policy := action.NewResponsePolicy()
		endpoint := env.createRandomExternalNexusServer(env.Context(), t, policy.Handler())
		dctx, cancel := context.WithTimeout(env.Context(), 15*time.Second)
		defer cancel()
		rc := action.NewCtx(env.TestEnv, endpoint, policy, 0)
		defer rc.Cleanup()
		learned, err := action.LearnFootprint(dctx, rc, action.Oracle{Env: env.TestEnv}, action.Resolver{}, 50*time.Millisecond, plan)
		require.NoError(t, err, "the observed drive should reach its terminal")
		return learned
	}

	var pfs []action.PlanFootprint
	for _, sp := range specs {
		learned := learn(sp.plan)
		pfs = append(pfs, action.PlanFootprint{Plan: sp.plan, Label: sp.label, Learned: learned})
		t.Logf("[cov-faults] learned %s: %d call(s), %d target(s)", sp.label, len(learned), len(action.FaultTargets(sp.plan, learned)))
	}

	const budget = 3
	drives, dropped := action.ScheduleFaults(pfs, budget)
	require.NotEmpty(t, drives, "the learned footprints should yield fault drives")
	t.Logf("[cov-faults] scheduled %d fault drive(s), dropped %d by budget %d", len(drives), len(dropped), budget)
	for _, d := range dropped {
		t.Logf("[cov-faults]   dropped by budget: %s", d.Label)
	}

	cov := probe.NewCoverage()
	for _, d := range drives {
		t.Logf("[cov-faults] drive: %s", d.Label)
		report := probe.Umpire(t).
			WithCoverage(cov).
			Reach("NexusOperation", "succeeded"). // plan validation only; the plan drives the real outcome
			Execution(s.nexusGenExecPlan(d.Plan)).
			Timeout(20 * time.Second).
			Judge()
		require.Empty(t, report.Baseline.Violations, "%s: an injected fault must not flag a conformance violation", d.Label)
	}

	lc, ok := planner.DefaultModels().Lifecycle("NexusOperation")
	require.True(t, ok)
	rep := cov.Report("NexusOperation", lc.Edges())
	t.Logf("[cov-faults] transition coverage after %d guided fault drives: %d/%d edges", len(drives), rep.Covered, rep.Total)
}

// TestProbeNexusRandomized is the seeded fuzz loop over the actions model: each iteration draws a
// random settling edge from a base seed, drives it in its own env, and — on that path's *learned*
// footprint — faults each observed call in turn. It extends resilience checking from the single
// sync-success path (TestProbeNexusResilience) to randomly-sampled outcomes, staying fully
// reproducible: a failing iteration replays from its logged seed alone.
//
// Two oracles, of different strength. The fault-free baseline is fully determined, so it must
// reconcile to its declared terminal (DriveErr nil) and be violation-free. Under an injected
// fault the terminal may legitimately shift (recover, degrade, or never settle), so the outcome
// is not asserted — but the rulebook is an invariant that must hold regardless of perturbation:
// no fault may make the Monitor flag a violation. A flagged scenario is a real conformance bug.
func (s *UmpireTestSuite) TestProbeNexusRandomized() {
	t := s.T()
	const baseSeed = 0x5eed // logged below so a red run reproduces from the exact seed
	const iterations = 6
	t.Logf("[randomized] base seed 0x%x, %d iterations", baseSeed, iterations)

	cov := probe.NewCoverage()
	for i := 0; i < iterations; i++ {
		seed := int64(baseSeed) + int64(i)
		plan, label := action.RandomPlan(seed)
		require.NotEmpty(t, plan, "seed %d: generator produced an empty plan", seed)
		t.Logf("[randomized] seed %d: %s", seed, label)

		report := probe.Umpire(t).
			WithCoverage(cov).
			Reach("NexusOperation", "succeeded"). // plan validation only; the plan drives the real outcome
			Execution(s.nexusGenExecPlan(plan)).
			Timeout(20 * time.Second).
			MaxFaults(4).
			FaultEachObservedCall().
			Judge()

		require.NoError(t, report.Baseline.DriveErr, "seed %d (%s): fault-free baseline must reconcile", seed, label)
		require.Empty(t, report.Baseline.Violations, "seed %d (%s): fault-free baseline conformance", seed, label)
		for _, sc := range report.Scenarios {
			require.Empty(t, sc.Violations, "seed %d (%s): dropping %q flagged a conformance violation", seed, label, sc.Method)
		}
		t.Logf("[randomized]   seed %d: %d observed call(s), %d fault scenario(s)", seed, len(report.Observed), len(report.Scenarios))
	}

	lc, ok := planner.DefaultModels().Lifecycle("NexusOperation")
	require.True(t, ok)
	rep := cov.Report("NexusOperation", lc.Edges())
	t.Logf("[randomized] NexusOperation transition coverage after %d random drives: %d/%d edges", iterations, rep.Covered, rep.Total)
}
