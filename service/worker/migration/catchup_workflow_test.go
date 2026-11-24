package migration

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/wideevents"
)

func TestCatchupWorkflow(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	var a *activities
	var lifecycleEvents []wideevents.NamespaceMigrationWorkflowLifecycleInput
	env.OnGetVersion(migrationWorkflowLifecycleVersion, workflow.DefaultVersion, 1).Return(workflow.Version(1))
	env.OnActivity(a.EmitNamespaceMigrationWorkflowLifecycle, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			lifecycleEvents = append(lifecycleEvents, args.Get(1).(wideevents.NamespaceMigrationWorkflowLifecycleInput))
		}).
		Return(nil).
		Twice()

	env.OnActivity(a.WaitCatchup, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(CatchupWorkflow, CatchUpParams{
		Namespace:      "test-ns",
		CatchupCluster: "test-remote",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, []string{
		wideevents.PhaseNamespaceCatchupStarted,
		wideevents.PhaseNamespaceCatchupFinished,
	}, []string{lifecycleEvents[0].Phase, lifecycleEvents[1].Phase})
	env.AssertExpectations(t)
}
