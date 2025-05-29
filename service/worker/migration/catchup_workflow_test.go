package migration

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestCatchupWorkflow(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	var a *activities

	env.OnActivity(a.WaitCatchup, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(CatchupWorkflow, CatchUpParams{
		Namespace:      "test-ns",
		CatchupCluster: "test-remote",
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
