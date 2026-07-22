package scheduler_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
)

func TestSchedulerMigrationFailureReloadAndRetryProperty(t *testing.T) {
	env := newSchedulerPropertyEnv(t, false)
	env.drain(t, schedulerConformanceDrainLimit)
	schedulerRPCProfiles{}.migrationRetryable().Expect(&env.services.Migrate, migrateWorkflowMethod, "first migration", nil)
	_, err := env.handler.MigrateToWorkflow(env.engineCtx, &schedulerpb.MigrateToWorkflowRequest{NamespaceId: namespaceID, ScheduleId: scheduleID})
	require.NoError(t, err)
	runnable, err := env.engine.RunnableTasks(env.ref)
	require.NoError(t, err)
	require.Len(t, runnable, 1)
	migrationTask := runnable[0]

	_, err = env.engine.ExecuteTask(t.Context(), env.ref, migrationTask)
	require.Error(t, err)
	require.Len(t, env.services.Migrate.Calls(), 1)
	require.True(t, env.describe(t).GetSchedule().GetState().GetPaused())
	env.reload(t)

	_, err = env.engine.ExecuteTask(t.Context(), env.ref, migrationTask)
	require.NoError(t, err)
	require.Len(t, env.services.Migrate.Calls(), 1)
	env.assertScripts(t)

	result, err := env.engine.ExecuteTask(t.Context(), env.ref, migrationTask)
	require.NoError(t, err)
	require.True(t, result.Dropped)
}
