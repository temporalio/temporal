package regress_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/tests/umpire2/protocol"
	"go.temporal.io/server/tests/umpire2/regress/activity"
	"go.temporal.io/server/tests/umpire2/regress/capability"
	"go.temporal.io/server/tests/umpire2/regress/nexus"
	"go.temporal.io/server/tests/umpire2/regress/rpc"
	"go.temporal.io/server/tests/umpire2/regress/workflow"
)

func TestRepresentativePlansCompileAgainstDefaultDomain(t *testing.T) {
	domain, err := protocol.DefaultRegressionDomain()
	require.NoError(t, err)
	profile := coreregress.Profile{
		Name: "local-chasm",
		Capabilities: []string{
			capability.CHASM.Name,
			capability.ActivityCallbacks.Name,
			capability.Faults.Name,
		},
	}
	tests := map[string]coreregress.Plan{
		"async completion before start response": coreregress.AllPaths(
			nexus.Complete("op", nexus.Succeeded),
			nexus.RespondStart("op", nexus.Async),
			nexus.State("op", nexus.Completed),
			nexus.LateStartResponseAccepted("op"),
		),
		"two callers share handler": coreregress.AllPaths(
			coreregress.AnyOrder(
				nexus.Start("left", nexus.HandlerWorkflow("handler")),
				nexus.Start("right", nexus.HandlerWorkflow("handler")),
			),
			workflow.State("handler", workflow.Completed),
			nexus.State("left", nexus.Completed),
			nexus.State("right", nexus.Completed),
			nexus.CallbackReferenceConsistent("left", "handler"),
			nexus.CallbackReferenceConsistent("right", "handler"),
		),
		"cancellation retried": coreregress.OnePath(
			nexus.State("op", nexus.Started),
			coreregress.During(
				nexus.FailNext(rpc.CancelNexusOperation),
				nexus.CancelWithRetry("op"),
			),
			nexus.CancelRequestFailed("op"),
			nexus.State("op", nexus.Canceled),
		),
		"start to close timeout": coreregress.OnePath(
			nexus.Schedule("op", nexus.StartToClose(2*time.Second)),
			nexus.RespondStart("op", nexus.Async),
			nexus.State("op", nexus.TimedOut),
		),
		"bidirectional activity links": coreregress.OnePath(
			coreregress.Require(capability.ActivityCallbacks),
			nexus.StartActivity("op", "activity"),
			activity.State("activity", activity.Completed),
			nexus.LinkedToActivity("op", "activity"),
			activity.LinkedToNexusOperation("activity", "op"),
		),
		"callback after caller completion": coreregress.OnePath(
			nexus.State("op", nexus.Started),
			workflow.State("caller", workflow.Completed),
			nexus.Complete("op", nexus.Succeeded),
			nexus.State("op", nexus.CallbackFailed),
		),
		"sync completion observations": coreregress.OnePath(
			nexus.ScheduleEmbedded("op", "caller"),
			nexus.RespondStart("op", nexus.Sync),
			nexus.State("op", nexus.Completed),
			nexus.ResultDigest("op", "result:sha256:canonical"),
			nexus.LinkEndpoint("op", "workflow-event:namespace/workflow/run/request"),
			workflow.NexusStorageAbsent("caller", "op"),
		),
	}

	for name, plan := range tests {
		t.Run(name, func(t *testing.T) {
			suite, err := coreregress.Compile(plan, domain, profile)
			require.NoError(t, err)
			require.NotEmpty(t, suite.Paths)
		})
	}
}

func TestTypedConstructorsRejectCrossDomainSymbolReuse(t *testing.T) {
	_, err := coreregress.Normalize(coreregress.OnePath(
		nexus.State("shared", nexus.Started),
		workflow.State("shared", workflow.Started),
	))

	require.ErrorIs(t, err, coreregress.ErrSymbolTypeConflict)
}

func TestObservedProjectionCompilesAsObservationAction(t *testing.T) {
	domain, err := protocol.DefaultRegressionDomain()
	require.NoError(t, err)

	suite, err := coreregress.Compile(coreregress.OnePath(
		nexus.Start("operation", nexus.HandlerWorkflow("handler")),
		coreregress.Bind("run", workflow.RunID("handler")),
	), domain, coreregress.Profile{Name: "local"})
	require.NoError(t, err)
	require.Equal(t, coreregress.ObservationAction, suite.Paths[0].Steps[len(suite.Paths[0].Steps)-1].Mode)
}
