package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/payloads"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	regressactivity "go.temporal.io/server/tests/umpire2/regress/activity"
	"go.temporal.io/server/tests/umpire2/regress/capability"
	"go.temporal.io/server/tests/umpire2/regress/nexus"
	"go.temporal.io/server/tests/umpire2/regress/workflow"
	"go.temporal.io/server/tests/umpire2/umpiretest"
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
	runSparseRegression(t, plan)
}

func TestSparseRegressionCompletionBeforeStartResponse(t *testing.T) {
	plan := coreregress.AllPaths(
		nexus.Complete("op", nexus.Succeeded),
		nexus.RespondStart("op", nexus.Async),
		nexus.State("op", nexus.Completed),
		nexus.LateStartResponseAccepted("op"),
	)
	runSparseRegression(t, plan)
}

func TestSparseRegressionCancellationRetry(t *testing.T) {
	plan := coreregress.OnePath(
		nexus.State("op", nexus.Started),
		coreregress.During(
			nexus.FailNext(nexus.CancelNexusOperation),
			nexus.CancelWithRetry("op"),
		),
		nexus.CancelRequestFailed("op"),
		nexus.State("op", nexus.Canceled),
	)
	runSparseRegression(t, plan)
}

func runSparseRegressionStartToCloseTimeout(t *testing.T, chasmEnabled bool) {
	t.Helper()
	plan := coreregress.OnePath(
		nexus.Schedule("op", nexus.StartToClose(2*time.Second)),
		nexus.RespondStart("op", nexus.Async),
		nexus.State("op", nexus.TimedOut),
	)
	runSparseRegressionWithCHASM(t, plan, chasmEnabled)
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
		nexus.CallbackReferenceConsistent("left", "handler"),
		nexus.CallbackReferenceConsistent("right", "handler"),
	)
	runSparseRegressionWithCHASM(t, plan, false)
}

func runSparseRegressionCallbackAfterCallerCompletion(t *testing.T, chasmEnabled bool) {
	t.Helper()
	plan := coreregress.OnePath(
		nexus.State("op", nexus.Started),
		workflow.State("caller", workflow.Completed),
		nexus.Complete("op", nexus.Succeeded),
		nexus.State("op", nexus.CallbackFailed),
	)
	runSparseRegressionWithCHASM(t, plan, chasmEnabled)
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
	runSparseRegressionWithCHASM(t, plan, chasmEnabled)
}

func runSparseRegression(t *testing.T, plan coreregress.Plan) {
	runSparseRegressionWithCHASM(t, plan, true)
}

func runSparseRegressionWithCHASM(t *testing.T, plan coreregress.Plan, chasmEnabled bool) {
	t.Helper()
	umpiretest.RequireRegression(t, plan, umpiretest.WithCHASM(chasmEnabled))
}
