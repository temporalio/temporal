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
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/probe"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpire/action"
	"go.temporal.io/server/tests/umpire/model"
	"go.temporal.io/server/tests/umpire/planner"
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

// TestProbeNexusRejectedStart is the E3 rejection round-trip (UMPIRE_ERR.md §3): an invalid action
// — a well-formed StartNexusOperationExecution naming a non-existent endpoint — is driven, and its
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

// TestProbeNexusReflectedVariant is the E2 round-trip (UMPIRE_ERR.md §1): the invalid actions are
// enumerated by reflecting the StartNexusOperationExecution descriptor — a variant per string field
// — rather than hand-authored. Driving one reflected variant (operation_id mutated to empty on an
// otherwise-valid base) reaches the E3 model: the rejection is observed as the op reaching the
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
// asserted). This is the learned-footprint fault exploration on an actions plan (PLAN.md Phase 5).
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
	cov := probe.NewCoverage()

	exploreEnv := func(exec probe.EnvFunc, timeout time.Duration) {
		probe.Umpire(t).
			WithCoverage(cov).
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

	// Auto-cover (PLAN.md Phase 4): the LIST of drives is computed from the model — one plan
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

	lc, ok := planner.DefaultModels().Lifecycle("NexusOperation")
	require.True(t, ok)
	rep := cov.Report("NexusOperation", lc.Edges())
	t.Logf("[exploration] NexusOperation transition coverage: %d/%d edges exercised", rep.Covered, rep.Total)
	for _, e := range rep.Missing() {
		t.Logf("[exploration]   MISSING: %s --%s--> %s", e.From, e.Event, e.To)
	}
	// All 16 modelled edges are now exercised, across both hostings: the workflow (Embedded)
	// driver covers the shared lifecycle, and the standalone driver covers the three
	// {scheduled,backing_off,started} --terminate--> terminated edges that are Standalone-only
	// (terminated is reached via the operation's own Terminate RPC — a workflow-child op has
	// none, and workflow termination does not cascade to it). The unreachable-by-construction
	// backing_off --> {start,succeed,fail,cancel} edges were pruned from the model, so the
	// count reflects the machine's real shape.
	require.Equal(t, rep.Total, rep.Covered,
		"every modelled NexusOperation edge should be exercised across the workflow and standalone drivers")
}
