package restorenamespace

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func newWorkflowTestEnv(
	refresh func(context.Context, RestoreJobInput) (RefreshStats, error),
	complete func(context.Context, RestoreJobInput) error,
) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(RestoreNamespaceWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	env.RegisterActivityWithOptions(refresh, activity.RegisterOptions{Name: RefreshExecutionsActivityName})
	env.RegisterActivityWithOptions(complete, activity.RegisterOptions{Name: CompleteRestoreActivityName})
	return env
}

func TestRestoreNamespaceWorkflow(t *testing.T) {
	t.Parallel()
	stats := RefreshStats{TotalEstimate: 3, ProcessedCount: 3, RefreshStartTime: time.Unix(100, 0).UTC()}
	completed := false
	env := newWorkflowTestEnv(
		func(_ context.Context, input RestoreJobInput) (RefreshStats, error) {
			require.Equal(t, testInput, input)
			return stats, nil
		},
		func(_ context.Context, input RestoreJobInput) error {
			require.Equal(t, testInput, input)
			completed = true
			return nil
		},
	)
	env.OnUpsertMemo(map[string]any{StatsMemoKey: stats}).Return(nil).Once()

	env.ExecuteWorkflow(WorkflowName, testInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result RefreshStats
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, stats, result)
	require.True(t, completed)
	env.AssertExpectations(t)
}

func TestRestoreNamespaceWorkflow_RefreshStopped(t *testing.T) {
	t.Parallel()
	env := newWorkflowTestEnv(
		func(context.Context, RestoreJobInput) (RefreshStats, error) {
			return RefreshStats{}, temporal.NewNonRetryableApplicationError("superseded", errTypeRestoreStopped, nil)
		},
		func(context.Context, RestoreJobInput) error {
			t.Error("a stopped restore must not be completed")
			return nil
		},
	)

	env.ExecuteWorkflow(WorkflowName, testInput)
	require.True(t, env.IsWorkflowCompleted())
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, env.GetWorkflowError(), &appErr)
	require.Equal(t, errTypeRestoreStopped, appErr.Type())
}
