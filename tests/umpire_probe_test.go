package tests

import (
	"context"
	"fmt"
	"os"
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
	"go.temporal.io/server/tests/probe"
	"go.temporal.io/server/tests/testcore"
)

// newNexusProbeEnv builds a CHASM-Nexus-enabled cluster env with the generic
// chasm.transition telemetry routed to its Monitor (see TestPlanAndDriveNexusOperationCHASM).
func (s *UmpireTestSuite) newNexusProbeEnv() *NexusTestEnv {
	t := s.T()
	os.Setenv("TEMPORAL_OTEL_DEBUG", "true")
	t.Cleanup(func() { os.Unsetenv("TEMPORAL_OTEL_DEBUG") })

	env := newNexusTestEnv(t, true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, true),
	)
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(env.GetMonitor())))
	t.Cleanup(func() { otel.SetTracerProvider(prevTP) })
	return env
}

// nexusProbeDrive registers a caller workflow that invokes a Nexus operation against
// a mock endpoint using the given start handler, and returns a probe DriveFunc that
// starts a fresh caller workflow per scenario.
func (s *UmpireTestSuite) nexusProbeDrive(env *NexusTestEnv, onStart nexustest.Handler) probe.DriveFunc {
	t := s.T()
	endpointName := env.createRandomExternalNexusServer(env.Context(), t, onStart)
	callerWorkflow := func(wctx workflow.Context) error {
		c := workflow.NewNexusClient(endpointName, "service")
		return c.ExecuteOperation(wctx, "operation", "input", workflow.NexusOperationOptions{}).Get(wctx, nil)
	}
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

// TestProbeNexusResilience is the trace-derived resilience demo: plan a route to a
// state, drive a REAL CHASM Nexus operation to it, learn the underlying gRPC calls
// from the happy-path trace, break each one, and let the Monitor judge — no
// hand-written faults and no hand-written outcome assertions (only the fault-free
// baseline is asserted).
func (s *UmpireTestSuite) TestProbeNexusResilience() {
	t := s.T()
	env := s.newNexusProbeEnv()
	drive := s.nexusProbeDrive(env, nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
		},
	})

	report := probe.Umpire(t, env.TestEnv).
		Reach("NexusOperation", "succeeded").
		Drive(drive).
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
	env := s.newNexusProbeEnv()
	drive := s.nexusProbeDrive(env, nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return nil, nexus.NewOperationFailedError("umpire probe: injected operation failure")
		},
	})

	report := probe.Umpire(t, env.TestEnv).
		Reach("NexusOperation", "succeeded").
		Drive(drive).
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
	env := s.newNexusProbeEnv()
	drive := s.nexusProbeDrive(env, nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "umpire probe: injected retryable failure")
		},
	})

	report := probe.Umpire(t, env.TestEnv).
		Reach("NexusOperation", "succeeded").
		Drive(drive).
		Timeout(8 * time.Second). // the op never settles; bound the stranded drive
		Judge()

	require.Equal(t, probe.Flagged, report.Baseline.Verdict, "an operation that never settles must be flagged")
	require.NotEmpty(t, report.Baseline.Violations, "EntityProgress should flag the stuck operation")
}
