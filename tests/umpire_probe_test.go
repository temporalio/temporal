package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/tests/probe"
	"go.temporal.io/server/tests/testcore"
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

// TestProbeNexusExploration drives the NexusOperation through several distinct
// outcomes — each in its own isolated namespace — accumulating transition coverage
// into one shared tracker, and reports which of the model's valid edges the real
// implementation actually exercised (and which remain unexercised). It is the
// "explore as fully as possible + summary" mode.
func (s *UmpireTestSuite) TestProbeNexusExploration() {
	t := s.T()
	cov := probe.NewCoverage()

	explore := func(handler nexustest.Handler, build callerFor, timeout time.Duration) {
		probe.Umpire(t).
			WithCoverage(cov).
			Reach("NexusOperation", "succeeded"). // plan validation only; the handler+caller drive the actual outcome
			Execution(s.nexusExec(handler, build)).
			Timeout(timeout).
			Judge()
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

	lc, ok := planner.DefaultModels().Lifecycle("NexusOperation")
	require.True(t, ok)
	rep := cov.Report("NexusOperation", lc.Edges())
	t.Logf("[exploration] NexusOperation transition coverage: %d/%d edges exercised", rep.Covered, rep.Total)
	for _, e := range rep.Missing() {
		t.Logf("[exploration]   MISSING: %s --%s--> %s", e.From, e.Event, e.To)
	}
	require.GreaterOrEqual(t, rep.Covered, 6,
		"the sync/retry/async/cancel/timeout drives should exercise the schedule, backoff, start, cancel and timeout edges")
}
