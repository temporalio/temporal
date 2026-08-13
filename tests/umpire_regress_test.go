package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	chasmactivity "go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/callback"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tests/umpire2"
	"go.temporal.io/server/tests/umpire2/action"
	"go.temporal.io/server/tests/umpire2/protocol"
	regressactivity "go.temporal.io/server/tests/umpire2/regress/activity"
	"go.temporal.io/server/tests/umpire2/regress/capability"
	"go.temporal.io/server/tests/umpire2/regress/nexus"
	"go.temporal.io/server/tests/umpire2/regress/rpc"
	"go.temporal.io/server/tests/umpire2/regress/workflow"
)

func TestSparseRegressionOrdinaryNexusCompletion(t *testing.T) {
	resultDigest, err := umpirefw.CanonicalProtoDigest("result", payloads.MustEncodeSingle("ok"))
	require.NoError(t, err)
	plan := coreregress.OnePath(
		nexus.ScheduleEmbedded("op", "caller"),
		nexus.RespondStart("op", nexus.Sync),
		nexus.State("op", nexus.Completed),
		nexus.ResultDigest("op", resultDigest),
		nexus.LinkEndpoint("op", "workflow-event:umpire-regression/handler/handler-run/handler-start"),
		workflow.NexusStorageAbsent("caller", "op"),
	)
	runSparseRegression(t, plan, coreregress.Profile{Name: "local"})
}

func TestSparseRegressionCompletionBeforeStartResponse(t *testing.T) {
	plan := coreregress.AllPaths(
		nexus.Complete("op", nexus.Succeeded),
		nexus.RespondStart("op", nexus.Async),
		nexus.State("op", nexus.Completed),
	)
	runSparseRegression(t, plan, coreregress.Profile{Name: "local"})
}

func TestSparseRegressionCancellationRetry(t *testing.T) {
	plan := coreregress.OnePath(
		nexus.State("op", nexus.Started),
		coreregress.During(
			nexus.FailNext(rpc.CancelNexusOperation),
			nexus.CancelWithRetry("op"),
		),
		nexus.CancelRequestFailed("op"),
		nexus.State("op", nexus.Canceled),
	)
	runSparseRegression(t, plan, coreregress.Profile{
		Name:         "local",
		Capabilities: []string{capability.Faults.Name},
	})
}

func runSparseRegressionStartToCloseTimeout(t *testing.T, chasmEnabled bool) {
	t.Helper()
	plan := coreregress.OnePath(
		nexus.Schedule("op", nexus.StartToClose(2*time.Second)),
		nexus.RespondStart("op", nexus.Async),
		nexus.State("op", nexus.TimedOut),
	)
	runSparseRegressionWithCHASM(t, plan, coreregress.Profile{Name: "local"}, chasmEnabled)
}

func TestSparseRegressionSharedHandlerWorkflow(t *testing.T) {
	plan := coreregress.AllPaths(
		coreregress.AnyOrder(
			nexus.Start("left", nexus.HandlerWorkflow("handler")),
			nexus.Start("right", nexus.HandlerWorkflow("handler")),
		),
		workflow.State("handler", workflow.Completed),
		nexus.State("left", nexus.Completed),
		nexus.State("right", nexus.Completed),
	)
	runSparseRegressionWithCHASM(t, plan, coreregress.Profile{Name: "local"}, false)
}

func runSparseRegressionCallbackAfterCallerCompletion(t *testing.T, chasmEnabled bool) {
	t.Helper()
	plan := coreregress.OnePath(
		nexus.State("op", nexus.Started),
		workflow.State("caller", workflow.Completed),
		nexus.Complete("op", nexus.Succeeded),
		nexus.State("op", nexus.CallbackFailed),
	)
	runSparseRegressionWithCHASM(t, plan, coreregress.Profile{Name: "local"}, chasmEnabled)
}

func runSparseRegressionBidirectionalNexusActivityLinks(t *testing.T, chasmEnabled bool) {
	t.Helper()
	plan := coreregress.OnePath(
		coreregress.Require(capability.ActivityCallbacks),
		nexus.StartActivity("op", "activity"),
		regressactivity.State("activity", regressactivity.Completed),
		nexus.LinkedToActivity("op", "activity"),
		regressactivity.LinkedToNexusOperation("activity", "op"),
	)
	runSparseRegressionWithCHASM(t, plan, coreregress.Profile{
		Name:         "local",
		Capabilities: []string{capability.ActivityCallbacks.Name},
	}, chasmEnabled)
}

func runSparseRegression(t *testing.T, plan coreregress.Plan, profile coreregress.Profile) {
	runSparseRegressionWithCHASM(t, plan, profile, true)
}

func runSparseRegressionWithCHASM(t *testing.T, plan coreregress.Plan, profile coreregress.Profile, chasmEnabled bool) {
	t.Helper()
	domain, err := protocol.DefaultRegressionDomain()
	require.NoError(t, err)
	suite, err := coreregress.Compile(plan, domain, profile)
	require.NoError(t, err)

	harness := action.NewRegressionHarness(func(context.Context, int) (action.RegressionEnvironment, coreregress.Cleanup, error) {
		env := newNexusTestEnv(t, true,
			testcore.WithUmpireMonitorFactory(umpire2.NewMonitor),
			testcore.WithDynamicConfig(dynamicconfig.EnableChasm, chasmEnabled),
			testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, chasmEnabled),
			testcore.WithDynamicConfig(chasmnexus.Enabled, chasmEnabled),
			testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, chasmEnabled),
			testcore.WithDynamicConfig(chasmactivity.Enabled, chasmEnabled),
			testcore.WithDynamicConfig(chasmactivity.EnableCallbacks, chasmEnabled),
			testcore.WithDynamicConfig(callback.AllowedAddresses, []any{map[string]any{"Pattern": "*", "AllowInsecure": true}}),
		)
		return env, nil, nil
	}, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, coreregress.Run(ctx, suite, harness))
}
