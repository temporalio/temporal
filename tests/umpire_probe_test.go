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
	"go.temporal.io/server/common/testing/testhooks"
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

// nexusExecForceTimeout builds an env whose Nexus invocation attempts resolve as a
// schedule-to-close timeout (via the NexusOperationForceTimeout server test hook), so the
// operation reaches the timed_out terminal deterministically — from scheduled, on the first
// attempt, with no real timer wait. The handler is never called (the hook short-circuits it).
func (s *UmpireTestSuite) nexusExecForceTimeout() probe.EnvFunc {
	return func(t *testing.T, _ int) (*testcore.TestEnv, probe.DriveFunc) {
		env := s.newNexusProbeEnv(t)
		env.InjectHook(testhooks.NewHook(testhooks.NexusOperationForceTimeout, testhooks.NexusForceTimeoutFromScheduled))
		unused := nexustest.Handler{OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
		}}
		return env.TestEnv, s.nexusProbeDrive(t, env, unused, syncCaller)
	}
}

// nexusExecForceTimeoutBackingOff reaches timed_out from backing_off. A retryable handler
// sends the first attempt into backoff (scheduled --> backing_off); the same
// NexusOperationForceTimeout hook — with the backing_off value — makes the backoff retry
// task time the operation out instead of rescheduling (backing_off --> timed_out).
func (s *UmpireTestSuite) nexusExecForceTimeoutBackingOff() probe.EnvFunc {
	return func(t *testing.T, _ int) (*testcore.TestEnv, probe.DriveFunc) {
		env := s.newNexusProbeEnv(t)
		env.InjectHook(testhooks.NewHook(testhooks.NexusOperationForceTimeout, testhooks.NexusForceTimeoutFromBackingOff))
		retry := nexustest.Handler{OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "umpire probe: injected retryable failure")
		}}
		return env.TestEnv, s.nexusProbeDrive(t, env, retry, syncCaller)
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
		Execution(s.nexusGenExecPlan(action.StandaloneCompletion)).
		Timeout(15 * time.Second).
		Judge()

	require.Equal(t, probe.Recovered, report.Baseline.Verdict, "generated standalone completion should reach succeeded")
	require.Equal(t, "succeeded", report.Baseline.Terminal)
}

// TestProbeNexusResilience is the trace-derived resilience demo: plan a route to a
// state, drive a REAL CHASM Nexus operation to it (each execution in its own
// namespace), learn the underlying gRPC calls from the happy-path trace, break each
// one, and let the Monitor judge — no hand-written faults and no hand-written outcome
// assertions (only the fault-free baseline is asserted).
func (s *UmpireTestSuite) TestProbeNexusResilience() {
	t := s.T()
	report := probe.Umpire(t).
		Reach("NexusOperation", "succeeded").
		Execution(s.nexusExec(nexustest.Handler{
			OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
			},
		}, syncCaller)).
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
		Execution(s.nexusExec(nexustest.Handler{
			OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
				return nil, nexus.NewOperationFailedError("umpire probe: injected operation failure")
			},
		}, syncCaller)).
		Timeout(15 * time.Second).
		Judge() // baseline only; no faults needed — the handler produces the outcome

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

	// Sync success and op-failure each settle in a single observed transition (a
	// forward jump), contributing no direct edges — but they exercise the drive path.
	explore(nexustest.Handler{OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
		return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
	}}, syncCaller, 10*time.Second)
	explore(nexustest.Handler{OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
		return nil, nexus.NewOperationFailedError("umpire probe: injected operation failure")
	}}, syncCaller, 10*time.Second)
	// The retryable path cycles scheduled <-> backing_off, exercising those direct edges.
	explore(nexustest.Handler{OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "umpire probe: injected retryable failure")
	}}, syncCaller, 8*time.Second)
	// An async start reaches STARTED (and then stays there), exercising the start edge.
	explore(asyncStart, syncCaller, 8*time.Second)
	// A handler reporting the operation canceled reaches the canceled terminal
	// (scheduled --cancel--> canceled) — a user-driven decision modeled as an untagged
	// terminal. (A workflow-side cancel request only advances the cancellation
	// sub-machine, which the umpire model does not track, leaving the op STARTED.)
	explore(nexustest.Handler{OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
		return nil, nexus.NewOperationCanceledError("umpire probe: injected cancellation")
	}}, syncCaller, 10*time.Second)
	// A short schedule-to-close timeout on a never-completing async operation reaches
	// the timed_out terminal (started --timeout--> timed_out).
	explore(asyncStart, timeoutCaller, 10*time.Second)
	// A forced schedule-to-close timeout on the first attempt reaches timed_out from
	// scheduled (scheduled --timeout--> timed_out), via the NexusOperationForceTimeout hook.
	exploreEnv(s.nexusExecForceTimeout(), 10*time.Second)
	// The same hook, valued for backing_off, times the op out during the backoff gap
	// (backing_off --timeout--> timed_out).
	exploreEnv(s.nexusExecForceTimeoutBackingOff(), 15*time.Second)
	// Embedded async completion, now generated from the actions model (see PLAN.md Phase 3):
	// schedule the op inside a caller workflow, async-ack the start, then deliver a completion
	// callback once it is STARTED — reaching started --> {succeeded, failed, canceled}.
	exploreEnv(s.nexusGenExecPlan(action.EmbeddedSucceed()), 15*time.Second)
	exploreEnv(s.nexusGenExecPlan(action.EmbeddedFail()), 15*time.Second)
	exploreEnv(s.nexusGenExecPlan(action.EmbeddedCancel()), 15*time.Second)

	// Standalone-hosting drives, now generated from the actions model (see PLAN.md Phase 3):
	// each plan starts the operation as its own execution, drives it into a state, then
	// terminates it — reaching {scheduled,backing_off,started} --terminate--> terminated. These
	// edges are Standalone-only (a workflow-child op has no terminate RPC and workflow
	// termination does not cascade to it).
	exploreEnv(s.nexusGenExecPlan(action.StandaloneTerminate(model.NexusScheduled)), 15*time.Second)
	exploreEnv(s.nexusGenExecPlan(action.StandaloneTerminate(model.NexusBackingOff)), 15*time.Second)
	exploreEnv(s.nexusGenExecPlan(action.StandaloneTerminate(model.NexusStarted)), 15*time.Second)

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
