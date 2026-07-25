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

// TestProbeNexusResilience is an end-to-end demo of the single "Umpire" probe
// API: plan a route to a state, drive a REAL CHASM Nexus operation to it, learn
// the underlying gRPC calls from the happy-path trace, break each one, and let
// the Monitor judge — with no hand-written faults and no hand-written assertions
// on the outcome (only the fault-free baseline is asserted).
func (s *UmpireTestSuite) TestProbeNexusResilience() {
	t := s.T()

	os.Setenv("TEMPORAL_OTEL_DEBUG", "true")
	t.Cleanup(func() { os.Unsetenv("TEMPORAL_OTEL_DEBUG") })

	env := newNexusTestEnv(t, true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, true),
	)
	ctx := env.Context()

	// Route the CHASM transition telemetry to this cluster's Monitor (see
	// TestPlanAndDriveNexusOperationCHASM).
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(env.GetMonitor())))
	t.Cleanup(func() { otel.SetTracerProvider(prevTP) })

	endpointName := env.createRandomExternalNexusServer(ctx, t, nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, _ nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			return &nexus.HandlerStartOperationResultSync[any]{Value: "ok"}, nil
		},
	})
	callerWorkflow := func(wctx workflow.Context) error {
		c := workflow.NewNexusClient(endpointName, "service")
		return c.ExecuteOperation(wctx, "operation", "input", workflow.NexusOperationOptions{}).Get(wctx, nil)
	}
	env.SdkWorker().RegisterWorkflow(callerWorkflow)

	report := probe.Umpire(t, env.TestEnv).
		Reach("NexusOperation", "succeeded").
		Drive(func(dctx context.Context, iter int) error {
			run, err := env.SdkClient().ExecuteWorkflow(dctx, sdkclient.StartWorkflowOptions{
				ID:        fmt.Sprintf("umpire-probe-nexus-%d", iter),
				TaskQueue: env.WorkerTaskQueue(),
			}, callerWorkflow)
			if err != nil {
				return err
			}
			return run.Get(dctx, nil)
		}).
		Timeout(10 * time.Second).
		MaxFaults(6).
		FaultEachObservedCall().
		Judge()

	// The only hand-written assertion is the sanity of the fault-free baseline;
	// every fault verdict is reported, not asserted.
	require.NoError(t, report.Baseline.DriveErr, "baseline (no fault) should reach the target")
	require.Empty(t, report.Baseline.Violations, "baseline (no fault) must be clean")
	require.NotEmpty(t, report.Observed, "the probe should observe the happy-path call footprint")
	require.NotEmpty(t, report.Scenarios, "the probe should derive at least one fault scenario")
}
